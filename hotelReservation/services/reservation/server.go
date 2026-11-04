package reservation

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/registry"
	pb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tls"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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

// MakeReservation makes a reservation based on given information
func (s *Server) MakeReservation(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	res := new(pb.Result)
	res.HotelId = make([]string, 0)

	database := s.MongoClient.Database("reservation-db")
	resCollection := database.Collection("reservation")
	numCollection := database.Collection("number")

	inDate, _ := time.Parse(
		time.RFC3339,
		req.InDate+"T12:00:00+00:00")

	outDate, _ := time.Parse(
		time.RFC3339,
		req.OutDate+"T12:00:00+00:00")
	hotelId := req.HotelId[0]

	indate := inDate.String()[0:10]

	memc_date_num_map := make(map[string]int)

	for inDate.Before(outDate) {
		// check reservations
		count := 0
		inDate = inDate.AddDate(0, 0, 1)
		outdate := inDate.String()[0:10]

		// first check memc
		memc_key := hotelId + "_" + inDate.String()[0:10] + "_" + outdate
		item, err := s.MemcClient.Get(memc_key)
		if err == nil {
			// memcached hit
			count, _ = strconv.Atoi(string(item.Value))
			log.Trace().Msgf("memcached hit %s = %d", memc_key, count)
			memc_date_num_map[memc_key] = count + int(req.RoomNumber)

		} else if err == memcache.ErrCacheMiss {
			// memcached miss
			log.Trace().Msgf("memcached miss")
			var reserve []reservation

			filter := bson.D{{"hotelId", hotelId}, {"inDate", indate}, {"outDate", outdate}}
			curr, err := resCollection.Find(context.TODO(), filter)
			if err != nil {
				log.Error().Msgf("Failed get reservation data: %v", err)
			}
			curr.All(context.TODO(), &reserve)
			if err != nil {
				log.Panic().Msgf("Tried to find hotelId [%v] from date [%v] to date [%v], but got error %v", hotelId, indate, outdate, err.Error())
			}

			for _, r := range reserve {
				count += r.Number
			}

			memc_date_num_map[memc_key] = count + int(req.RoomNumber)

		} else {
			log.Panic().Msgf("Tried to get memc_key [%v], but got memmcached error = %s", memc_key, err)
		}

		// check capacity
		// check memc capacity
		memc_cap_key := hotelId + "_cap"
		item, err = s.MemcClient.Get(memc_cap_key)
		hotel_cap := 0
		if err == nil {
			// memcached hit
			hotel_cap, _ = strconv.Atoi(string(item.Value))
			log.Trace().Msgf("memcached hit %s = %d", memc_cap_key, hotel_cap)
		} else if err == memcache.ErrCacheMiss {
			// memcached miss
			var num number
			err = numCollection.FindOne(context.TODO(), &bson.D{{"hotelId", hotelId}}).Decode(&num)
			if err != nil {
				log.Panic().Msgf("Tried to find hotelId [%v], but got error %v", hotelId, err.Error())
			}
			hotel_cap = int(num.Number)

			// write to memcache
			s.MemcClient.Set(&memcache.Item{Key: memc_cap_key, Value: []byte(strconv.Itoa(hotel_cap))})
		} else {
			log.Panic().Msgf("Tried to get memc_cap_key [%v], but got memmcached error = %s", memc_cap_key, err)
		}

		if count+int(req.RoomNumber) > hotel_cap {
			return res, nil
		}
		indate = outdate
	}

	// only update reservation number cache after check succeeds
	for key, val := range memc_date_num_map {
		s.MemcClient.Set(&memcache.Item{Key: key, Value: []byte(strconv.Itoa(val))})
	}

	inDate, _ = time.Parse(
		time.RFC3339,
		req.InDate+"T12:00:00+00:00")

	indate = inDate.String()[0:10]

	for inDate.Before(outDate) {
		inDate = inDate.AddDate(0, 0, 1)
		outdate := inDate.String()[0:10]
		_, err := resCollection.InsertOne(
			context.TODO(),
			reservation{
				HotelId:      hotelId,
				CustomerName: req.CustomerName,
				InDate:       indate,
				OutDate:      outdate,
				Number:       int(req.RoomNumber),
			},
		)
		if err != nil {
			log.Panic().Msgf("Tried to insert hotel [hotelId %v], but got error %v", hotelId, err.Error())
		}
		indate = outdate
	}

	res.HotelId = append(res.HotelId, hotelId)

	return res, nil
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
		if item, ok := cacheMemRes[hotelId+"_cap"]; ok {
			hotelCap, _ := strconv.Atoi(string(item.Value))
			cacheCap[hotelId] = hotelCap
		} else {
			queryMissKeys = append(queryMissKeys, hotelId)
		}
	}
	if len(queryMissKeys) > 0 {
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
			// Add, not Set: never replace a value another request stored meanwhile.
			go s.MemcClient.Add(&memcache.Item{Key: num.HotelId + "_cap", Value: []byte(strconv.Itoa(num.Number))})
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
		if item, ok := itemsMap[comm]; ok {
			count, _ := strconv.Atoi(string(item.Value))
			check(queryItem["hotelId"], count)
			continue
		}
		// memcached miss: count the night's reservations in MongoDB
		wg.Add(1)
		go func(comm string, queryItem map[string]string) {
			defer wg.Done()

			var reserve []reservation
			resCollection := s.MongoClient.Database("reservation-db").Collection("reservation")
			filter := bson.D{{"hotelId", queryItem["hotelId"]}, {"inDate", queryItem["startDate"]}, {"outDate", queryItem["endDate"]}}

			reserveMongoSpan, _ := opentracing.StartSpanFromContext(ctx, "mongodb_capacity_get_multi_number"+comm)
			reserveMongoSpan.SetTag("span.kind", "client")
			curr, err := resCollection.Find(context.TODO(), filter)
			if err == nil {
				err = curr.All(context.TODO(), &reserve)
			}
			reserveMongoSpan.Finish()
			if err != nil {
				log.Panic().Msgf("Tried to find hotelId [%v] from date [%v] to date [%v], but got error %v",
					queryItem["hotelId"], queryItem["startDate"], queryItem["endDate"], err)
			}
			var count int
			for _, r := range reserve {
				count += r.Number
			}
			// Add, not Set: a reservation may have stored a newer count meanwhile.
			go s.MemcClient.Add(&memcache.Item{Key: comm, Value: []byte(strconv.Itoa(count))})
			check(queryItem["hotelId"], count)
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
