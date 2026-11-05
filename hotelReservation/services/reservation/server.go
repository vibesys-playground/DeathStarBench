package reservation

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/lease"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/registry"
	pb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tls"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const name = "srv-reservation"

// Server implements the user service
type Server struct {
	pb.UnimplementedReservationServer

	uuid string

	Tracer      opentracing.Tracer
	Port        int
	IpAddr      string
	MongoClient *mongo.Client
	Registry    *registry.Client
	MemcClient  *memcache.Client
}

// Run starts the server
func (s *Server) Run() error {
	opentracing.SetGlobalTracer(s.Tracer)

	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	if err := EnsureIndexes(context.Background(), s.MongoClient.Database("reservation-db")); err != nil {
		return fmt.Errorf("create the night counts' index: %v", err)
	}

	s.uuid = uuid.New().String()

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Timeout: 120 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(
			otgrpc.OpenTracingServerInterceptor(s.Tracer),
		),
	}

	if tlsopt := tls.GetServerOpt(); tlsopt != nil {
		opts = append(opts, tlsopt)
	}

	srv := grpc.NewServer(opts...)

	pb.RegisterReservationServer(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	log.Trace().Msgf("In reservation s.IpAddr = %s, port = %d", s.IpAddr, s.Port)

	err = s.Registry.Register(name, s.uuid, s.IpAddr, s.Port)
	if err != nil {
		return fmt.Errorf("failed register: %v", err)
	}
	log.Info().Msg("Successfully registered in consul")

	return srv.Serve(lis)
}

// Shutdown cleans up any processes
func (s *Server) Shutdown() {
	s.Registry.Deregister(s.uuid)
}

// MakeReservation reserves req.RoomNumber rooms of the hotel on every night
// of the stay, or none. The rooms taken per hotel and night are counted in
// MongoDB (collection "nights", one document per hotel and night), and a
// night is taken by a conditional increment that matches only while the
// night has room: replicas and concurrent requests can never oversell, since
// MongoDB applies each increment atomically. A stay that does not fit on some
// night gives back the nights it took. The cached counts of the nights are
// invalidated after the increments, before the answer.
func (s *Server) MakeReservation(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	res := new(pb.Result)
	res.HotelId = make([]string, 0)

	database := s.MongoClient.Database("reservation-db")
	resCollection := database.Collection("reservation")
	nightCollection := database.Collection("nights")
	numCollection := database.Collection("number")

	inDate, _ := time.Parse(
		time.RFC3339,
		req.InDate+"T12:00:00+00:00")

	outDate, _ := time.Parse(
		time.RFC3339,
		req.OutDate+"T12:00:00+00:00")
	hotelId := req.HotelId[0]
	rooms := int(req.RoomNumber)

	// The nights of the stay: their first days, and their count keys in
	// memcached (keyed by the day after, as CheckAvailability keys them).
	var nights, countKeys []string
	for day := inDate; day.Before(outDate); {
		nights = append(nights, day.String()[0:10])
		day = day.AddDate(0, 0, 1)
		countKeys = append(countKeys, hotelId+"_"+day.String()[0:10]+"_"+day.String()[0:10])
	}
	if len(nights) == 0 {
		res.HotelId = append(res.HotelId, hotelId)
		return res, nil
	}

	// check memc capacity
	memc_cap_key := hotelId + "_cap"
	item, err := s.MemcClient.Get(memc_cap_key)
	if err == nil && lease.IsLease(item) {
		err = memcache.ErrCacheMiss // another request is filling it
	}
	hotel_cap := 0
	if err == nil {
		// memcached hit
		hotel_cap, _ = strconv.Atoi(string(item.Value))
		log.Trace().Msgf("memcached hit %s = %d", memc_cap_key, hotel_cap)
	} else if err == memcache.ErrCacheMiss {
		// memcached miss; take the fill lease before reading MongoDB, so
		// a capacity change between the read and the fill cannot be undone
		fill := lease.Acquire(s.MemcClient, memc_cap_key)
		var num number
		err = numCollection.FindOne(context.TODO(), &bson.D{{"hotelId", hotelId}}).Decode(&num)
		if err != nil {
			log.Panic().Msgf("Tried to find hotelId [%v], but got error %v", hotelId, err.Error())
		}
		hotel_cap = int(num.Number)

		// write to memcache
		fill.Fill([]byte(strconv.Itoa(hotel_cap)))
	} else {
		log.Panic().Msgf("Tried to get memc_cap_key [%v], but got memmcached error = %s", memc_cap_key, err)
	}
	if rooms > hotel_cap {
		return res, nil
	}

	for index, night := range nights {
		taken, err := takeRooms(ctx, nightCollection, hotelId, night, rooms, hotel_cap)
		if err != nil {
			log.Panic().Msgf("Tried to reserve hotel [%v] on [%v], but got error %v", hotelId, night, err)
		}
		if !taken {
			for _, earlier := range nights[:index] {
				if err := giveBackRooms(ctx, nightCollection, hotelId, earlier, rooms); err != nil {
					log.Panic().Msgf("Tried to give back hotel [%v] on [%v], but got error %v", hotelId, earlier, err)
				}
			}
			s.invalidateCounts(countKeys[:index])
			return res, nil
		}
	}
	s.invalidateCounts(countKeys)

	for index, night := range nights {
		next := outDate.Format("2006-01-02")
		if index+1 < len(nights) {
			next = nights[index+1]
		}
		_, err := resCollection.InsertOne(
			context.TODO(),
			reservation{
				HotelId:      hotelId,
				CustomerName: req.CustomerName,
				InDate:       night,
				OutDate:      next,
				Number:       rooms,
			},
		)
		if err != nil {
			log.Panic().Msgf("Tried to insert hotel [hotelId %v], but got error %v", hotelId, err.Error())
		}
	}

	res.HotelId = append(res.HotelId, hotelId)

	return res, nil
}

// night counts the rooms reserved on one night of a hotel. Night is the
// night's first day (YYYY-MM-DD), as the reservations' inDate.
type night struct {
	HotelId string `bson:"hotelId"`
	Night   string `bson:"night"`
	Rooms   int    `bson:"rooms"`
}

// EnsureIndexes creates the unique index that makes a night's count one
// document. The service calls it at startup; it is idempotent.
func EnsureIndexes(ctx context.Context, database *mongo.Database) error {
	_, err := database.Collection("nights").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "hotelId", Value: 1}, {Key: "night", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}

// takeRooms adds rooms to the night's count if the count stays within
// capacity, and reports whether it did. The increment's filter matches only
// a count with room, so concurrent takes are serialized by MongoDB. A night
// without a document is created with the rooms; when two requests create it
// at once, the unique index lets one insert win and the other retries the
// increment.
func takeRooms(ctx context.Context, nights *mongo.Collection, hotelId, day string, rooms, capacity int) (bool, error) {
	if rooms > capacity {
		return false, nil
	}
	increment := func() (bool, error) {
		update, err := nights.UpdateOne(ctx,
			bson.D{{Key: "hotelId", Value: hotelId}, {Key: "night", Value: day}, {Key: "rooms", Value: bson.D{{Key: "$lte", Value: capacity - rooms}}}},
			bson.D{{Key: "$inc", Value: bson.D{{Key: "rooms", Value: rooms}}}},
		)
		if err != nil {
			return false, err
		}
		return update.MatchedCount == 1, nil
	}
	if taken, err := increment(); taken || err != nil {
		return taken, err
	}
	// Either the night has no document yet or it is full.
	_, err := nights.InsertOne(ctx, night{HotelId: hotelId, Night: day, Rooms: rooms})
	if err == nil {
		return true, nil
	}
	if !mongo.IsDuplicateKeyError(err) {
		return false, err
	}
	// The document exists (documents are never deleted): the increment
	// decides.
	return increment()
}

// giveBackRooms undoes takeRooms for a stay that did not fit.
func giveBackRooms(ctx context.Context, nights *mongo.Collection, hotelId, day string, rooms int) error {
	_, err := nights.UpdateOne(ctx,
		bson.D{{Key: "hotelId", Value: hotelId}, {Key: "night", Value: day}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "rooms", Value: -rooms}}}},
	)
	return err
}

// invalidateCounts deletes cached night counts after their documents changed.
func (s *Server) invalidateCounts(keys []string) {
	for _, key := range keys {
		if err := lease.Invalidate(s.MemcClient, key); err != nil {
			log.Panic().Msgf("Tried to invalidate memc_key [%v], but got memcached error = %s", key, err)
		}
	}
}

// CheckAvailability returns the requested hotels that have req.RoomNumber
// rooms free on every night of the stay. Capacities and reservation counts
// come from memcached, and from MongoDB for the keys memcached misses.
func (s *Server) CheckAvailability(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	res := new(pb.Result)
	res.HotelId = make([]string, 0)

	hotelIds := make([]string, 0, len(req.HotelId))
	hotelMemKeys := make([]string, 0, len(req.HotelId))
	seen := make(map[string]struct{}, len(req.HotelId))
	for _, hotelId := range req.HotelId {
		if _, duplicate := seen[hotelId]; duplicate {
			continue
		}
		seen[hotelId] = struct{}{}
		hotelIds = append(hotelIds, hotelId)
		hotelMemKeys = append(hotelMemKeys, hotelId+"_cap")
	}

	capMemSpan, _ := opentracing.StartSpanFromContext(ctx, "memcached_capacity_get_multi_number")
	capMemSpan.SetTag("span.kind", "client")
	cacheMemRes, err := s.MemcClient.GetMulti(hotelMemKeys)
	capMemSpan.Finish()
	if err != nil && err != memcache.ErrCacheMiss {
		log.Panic().Msgf("Tried to get memc_cap_key [%v], but got memmcached error = %s", hotelMemKeys, err)
	}

	// Capacities by hotel ID: memcached hits, then MongoDB for the misses
	// (GetMulti reports a miss by leaving the key out, not by an error).
	cacheCap := make(map[string]int, len(hotelIds))
	queryMissKeys := []string{}
	for _, hotelId := range hotelIds {
		if item, ok := cacheMemRes[hotelId+"_cap"]; ok && !lease.IsLease(item) {
			hotelCap, _ := strconv.Atoi(string(item.Value))
			cacheCap[hotelId] = hotelCap
		} else {
			queryMissKeys = append(queryMissKeys, hotelId)
		}
	}
	if len(queryMissKeys) > 0 {
		// Take the fill leases before reading MongoDB, so a capacity change
		// between the read and the fill cannot be undone.
		fills := make(map[string]lease.Lease, len(queryMissKeys))
		for _, hotelId := range queryMissKeys {
			fills[hotelId] = lease.Acquire(s.MemcClient, hotelId+"_cap")
		}
		numCollection := s.MongoClient.Database("reservation-db").Collection("number")
		var nums []number
		capMongoSpan, _ := opentracing.StartSpanFromContext(ctx, "mongodb_capacity_get_multi_number")
		capMongoSpan.SetTag("span.kind", "client")
		curr, err := numCollection.Find(context.TODO(), bson.D{{"hotelId", bson.D{{"$in", queryMissKeys}}}})
		if err == nil {
			err = curr.All(context.TODO(), &nums)
		}
		capMongoSpan.Finish()
		if err != nil {
			log.Panic().Msgf("Tried to find hotelId [%v], but got error %v", queryMissKeys, err)
		}
		for _, num := range nums {
			cacheCap[num.HotelId] = num.Number
			go fills[num.HotelId].Fill([]byte(strconv.Itoa(num.Number)))
		}
	}

	// Reservation counts by hotel and night, keyed as MakeReservation keys them.
	reqCommand := []string{}
	queryMap := make(map[string]map[string]string)
	for _, hotelId := range hotelIds {
		inDate, _ := time.Parse(
			time.RFC3339,
			req.InDate+"T12:00:00+00:00")
		outDate, _ := time.Parse(
			time.RFC3339,
			req.OutDate+"T12:00:00+00:00")
		for inDate.Before(outDate) {
			indate := inDate.String()[:10]
			inDate = inDate.AddDate(0, 0, 1)
			outDate := inDate.String()[:10]
			memcKey := hotelId + "_" + outDate + "_" + outDate
			reqCommand = append(reqCommand, memcKey)
			queryMap[memcKey] = map[string]string{
				"hotelId":   hotelId,
				"startDate": indate,
				"endDate":   outDate,
			}
		}
	}

	available := make(map[string]bool, len(hotelIds))
	for _, hotelId := range hotelIds {
		available[hotelId] = true
	}
	var mutex sync.Mutex
	check := func(hotelId string, count int) {
		if count+int(req.RoomNumber) > cacheCap[hotelId] {
			mutex.Lock()
			available[hotelId] = false
			mutex.Unlock()
		}
	}

	reserveMemSpan, _ := opentracing.StartSpanFromContext(ctx, "memcached_reserve_get_multi_number")
	reserveMemSpan.SetTag("span.kind", "client")
	itemsMap, err := s.MemcClient.GetMulti(reqCommand)
	reserveMemSpan.Finish()
	if err != nil && err != memcache.ErrCacheMiss {
		log.Panic().Msgf("Tried to get memc_key [%v], but got memmcached error = %s", reqCommand, err)
	}
	var wg sync.WaitGroup
	for comm, queryItem := range queryMap {
		if item, ok := itemsMap[comm]; ok && !lease.IsLease(item) {
			count, _ := strconv.Atoi(string(item.Value))
			check(queryItem["hotelId"], count)
			continue
		}
		// memcached miss: read the night's count in MongoDB
		wg.Add(1)
		go func(comm string, queryItem map[string]string) {
			defer wg.Done()

			// MakeReservation invalidates the counts it changes, which
			// also ends this lease, so an older count never replaces a
			// newer one.
			fill := lease.Acquire(s.MemcClient, comm)
			nightCollection := s.MongoClient.Database("reservation-db").Collection("nights")
			filter := bson.D{{Key: "hotelId", Value: queryItem["hotelId"]}, {Key: "night", Value: queryItem["startDate"]}}

			reserveMongoSpan, _ := opentracing.StartSpanFromContext(ctx, "mongodb_capacity_get_multi_number"+comm)
			reserveMongoSpan.SetTag("span.kind", "client")
			var counted night
			err := nightCollection.FindOne(context.TODO(), filter).Decode(&counted)
			if err == mongo.ErrNoDocuments {
				err = nil // nothing reserved on the night
			}
			reserveMongoSpan.Finish()
			if err != nil {
				log.Panic().Msgf("Tried to find hotelId [%v] from date [%v] to date [%v], but got error %v",
					queryItem["hotelId"], queryItem["startDate"], queryItem["endDate"], err)
			}
			go fill.Fill([]byte(strconv.Itoa(counted.Rooms)))
			check(queryItem["hotelId"], counted.Rooms)
		}(comm, queryItem)
	}
	wg.Wait()

	for _, hotelId := range hotelIds {
		if available[hotelId] {
			res.HotelId = append(res.HotelId, hotelId)
		}
	}

	return res, nil
}

type reservation struct {
	HotelId      string `bson:"hotelId"`
	CustomerName string `bson:"customerName"`
	InDate       string `bson:"inDate"`
	OutDate      string `bson:"outDate"`
	Number       int    `bson:"number"`
}

type number struct {
	HotelId string `bson:"hotelId"`
	Number  int    `bson:"numberOfRoom"`
}

// SetCapacity sets a hotel's number of rooms in MongoDB, then invalidates
// the cached capacity, so the next reservation uses it. A capacity below the
// rooms already reserved on any night is rejected: a capacity change never
// oversells. (A change is not ordered with reservations of the same hotel
// that run concurrently with it: one that read the old capacity may still
// take rooms up to it.)
func (s *Server) SetCapacity(ctx context.Context, req *pb.CapacityRequest) (*pb.CapacityResult, error) {
	if req.HotelId == "" || req.Rooms < 0 {
		return nil, status.Error(codes.InvalidArgument, "hotelId and a non-negative number of rooms are required")
	}
	database := s.MongoClient.Database("reservation-db")
	maxReserved, err := maxReservedRooms(ctx, database.Collection("nights"), req.HotelId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "count reservations: %v", err)
	}
	result := &pb.CapacityResult{MaxReserved: int32(maxReserved)}
	if maxReserved > int(req.Rooms) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"hotel %s has %d rooms reserved on a night, more than %d", req.HotelId, maxReserved, req.Rooms)
	}
	update, err := database.Collection("number").UpdateOne(ctx,
		bson.D{{Key: "hotelId", Value: req.HotelId}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "numberOfRoom", Value: req.Rooms}}}},
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update capacity: %v", err)
	}
	if update.MatchedCount == 0 {
		return nil, status.Errorf(codes.NotFound, "no hotel %s", req.HotelId)
	}
	if err := lease.Invalidate(s.MemcClient, req.HotelId+"_cap"); err != nil {
		return nil, status.Errorf(codes.Unavailable, "invalidate cached capacity: %v", err)
	}
	return result, nil
}

// AddCapacity stores the number of rooms of a new hotel. The collection's
// unique index on "hotelId" rejects a hotel that has one.
func (s *Server) AddCapacity(ctx context.Context, req *pb.CapacityRequest) (*pb.CapacityResult, error) {
	if req.HotelId == "" || req.Rooms < 0 {
		return nil, status.Error(codes.InvalidArgument, "hotelId and a non-negative number of rooms are required")
	}
	_, err := s.MongoClient.Database("reservation-db").Collection("number").InsertOne(ctx,
		bson.D{{Key: "hotelId", Value: req.HotelId}, {Key: "numberOfRoom", Value: req.Rooms}})
	if mongo.IsDuplicateKeyError(err) {
		return nil, status.Errorf(codes.AlreadyExists, "hotel %s has a capacity", req.HotelId)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert capacity: %v", err)
	}
	if err := lease.Invalidate(s.MemcClient, req.HotelId+"_cap"); err != nil {
		return nil, status.Errorf(codes.Unavailable, "invalidate cached capacity: %v", err)
	}
	return &pb.CapacityResult{}, nil
}

// maxReservedRooms is the largest number of rooms reserved on one night of
// the hotel (the counts MakeReservation keeps).
func maxReservedRooms(ctx context.Context, collection *mongo.Collection, hotelId string) (int, error) {
	var most night
	err := collection.FindOne(ctx, bson.D{{Key: "hotelId", Value: hotelId}},
		options.FindOne().SetSort(bson.D{{Key: "rooms", Value: -1}})).Decode(&most)
	if err == mongo.ErrNoDocuments {
		return 0, nil
	}
	return most.Rooms, err
}
