package rate

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/lease"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/registry"
	pb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/rate/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tls"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const name = "srv-rate"

// Server implements the rate service
type Server struct {
	pb.UnimplementedRateServer

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

	pb.RegisterRateServer(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

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

// GetRates gets rates for hotels for specific date range.
func (s *Server) GetRates(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	res := new(pb.Result)

	ratePlans := make(RatePlans, 0)

	hotelIds := []string{}
	rateMap := make(map[string]struct{})
	for _, hotelID := range req.HotelIds {
		hotelIds = append(hotelIds, hotelID)
		rateMap[hotelID] = struct{}{}
	}
	// first check memcached(get-multi)
	memSpan, _ := opentracing.StartSpanFromContext(ctx, "memcached_get_multi_rate")
	memSpan.SetTag("span.kind", "client")

	resMap, err := s.MemcClient.GetMulti(hotelIds)
	memSpan.Finish()

	var wg sync.WaitGroup
	var mutex sync.Mutex
	if err != nil && err != memcache.ErrCacheMiss {
		log.Panic().Msgf("Memmcached error while trying to get hotel [id: %v]= %s", hotelIds, err)
	} else {
		for hotelId, item := range resMap {
			if lease.IsLease(item) {
				continue // another request is filling it: a miss
			}
			rateStrs := strings.Split(string(item.Value), "\n")
			log.Trace().Msgf("memc hit, hotelId = %s,rate strings: %v", hotelId, rateStrs)

			for _, rateStr := range rateStrs {
				if len(rateStr) != 0 {
					rateP := new(pb.RatePlan)
					json.Unmarshal([]byte(rateStr), rateP)
					ratePlans = append(ratePlans, rateP)
				}
			}

			delete(rateMap, hotelId)
		}

		wg.Add(len(rateMap))
		for hotelId := range rateMap {
			go func(id string) {
				defer wg.Done()
				log.Trace().Msgf("memc miss, hotelId = %s", id)

				// Take the fill lease before reading MongoDB, so a rate
				// update between the read and the fill cannot be undone.
				fill := lease.Acquire(s.MemcClient, id)

				mongoSpan, _ := opentracing.StartSpanFromContext(ctx, "mongo_rate")
				mongoSpan.SetTag("span.kind", "client")

				// memcached miss, set up mongo connection
				collection := s.MongoClient.Database("rate-db").Collection("inventory")
				tmpRatePlans := make(RatePlans, 0)
				curr, err := collection.Find(context.TODO(), bson.D{{Key: "hotelId", Value: id}})
				if err == nil {
					err = curr.All(context.TODO(), &tmpRatePlans)
				}
				mongoSpan.Finish()
				if err != nil {
					log.Panic().Msgf("Tried to find hotelId [%v], but got error %v", id, err)
				}

				memcStr := ""
				for _, r := range tmpRatePlans {
					mutex.Lock()
					ratePlans = append(ratePlans, r)
					mutex.Unlock()
					rateJson, err := json.Marshal(r)
					if err != nil {
						log.Error().Msgf("Failed to marshal plan [Code: %v] with error: %s", r.Code, err)
					}
					memcStr = memcStr + string(rateJson) + "\n"
				}
				go fill.Fill([]byte(memcStr))
			}(hotelId)
		}
	}
	wg.Wait()

	sort.Sort(ratePlans)
	res.RatePlans = ratePlans

	return res, nil
}

type RatePlans []*pb.RatePlan

func (r RatePlans) Len() int {
	return len(r)
}

func (r RatePlans) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RatePlans) Less(i, j int) bool {
	return r[i].RoomType.TotalRate > r[j].RoomType.TotalRate
}

// UpdateRate sets the prices of the hotel's plan with the given code in
// MongoDB, then invalidates the hotel's cached plans.
func (s *Server) UpdateRate(ctx context.Context, req *pb.UpdateRateRequest) (*pb.UpdateRateResult, error) {
	if req.HotelId == "" || req.Code == "" {
		return nil, status.Error(codes.InvalidArgument, "hotelId and code are required")
	}
	collection := s.MongoClient.Database("rate-db").Collection("inventory")
	result, err := collection.UpdateOne(ctx,
		bson.D{{Key: "hotelId", Value: req.HotelId}, {Key: "code", Value: req.Code}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "roomType.bookableRate", Value: req.BookableRate},
			{Key: "roomType.totalRate", Value: req.TotalRate},
			{Key: "roomType.totalRateInclusive", Value: req.TotalRateInclusive},
		}}},
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update rate plan: %v", err)
	}
	if result.MatchedCount == 0 {
		return nil, status.Errorf(codes.NotFound, "hotel %s has no rate plan %s", req.HotelId, req.Code)
	}
	if err := lease.Invalidate(s.MemcClient, req.HotelId); err != nil {
		return nil, status.Errorf(codes.Unavailable, "invalidate cached rates: %v", err)
	}
	return &pb.UpdateRateResult{}, nil
}

// AddRates stores rate plans in MongoDB, then invalidates the cached plans
// of their hotels.
func (s *Server) AddRates(ctx context.Context, req *pb.AddRatesRequest) (*pb.AddRatesResult, error) {
	documents := make([]interface{}, 0, len(req.RatePlans))
	hotels := make(map[string]struct{})
	for _, plan := range req.RatePlans {
		if plan.HotelId == "" || plan.Code == "" || plan.RoomType == nil {
			return nil, status.Error(codes.InvalidArgument, "a rate plan needs hotelId, code and roomType")
		}
		hotels[plan.HotelId] = struct{}{}
		documents = append(documents, storedRatePlan{
			HotelId: plan.HotelId, Code: plan.Code, InDate: plan.InDate, OutDate: plan.OutDate,
			RoomType: &storedRoomType{
				BookableRate: plan.RoomType.BookableRate, Code: plan.RoomType.Code,
				RoomDescription: plan.RoomType.RoomDescription, TotalRate: plan.RoomType.TotalRate,
				TotalRateInclusive: plan.RoomType.TotalRateInclusive,
			},
		})
	}
	if len(documents) == 0 {
		return &pb.AddRatesResult{}, nil
	}
	collection := s.MongoClient.Database("rate-db").Collection("inventory")
	if _, err := collection.InsertMany(ctx, documents); err != nil {
		return nil, status.Errorf(codes.Internal, "insert rate plans: %v", err)
	}
	for hotelId := range hotels {
		if err := lease.Invalidate(s.MemcClient, hotelId); err != nil {
			return nil, status.Errorf(codes.Unavailable, "invalidate cached rates: %v", err)
		}
	}
	return &pb.AddRatesResult{}, nil
}

// storedRatePlan is a rate-db.inventory document (cmd/rate/db.go RatePlan).
type storedRatePlan struct {
	HotelId  string          `bson:"hotelId"`
	Code     string          `bson:"code"`
	InDate   string          `bson:"inDate"`
	OutDate  string          `bson:"outDate"`
	RoomType *storedRoomType `bson:"roomType"`
}

type storedRoomType struct {
	BookableRate       float64 `bson:"bookableRate"`
	Code               string  `bson:"code"`
	RoomDescription    string  `bson:"roomDescription"`
	TotalRate          float64 `bson:"totalRate"`
	TotalRateInclusive float64 `bson:"totalRateInclusive"`
}
