package recommendation

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/changes"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/registry"
	pb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/recommendation/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tls"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/hailocab/go-geoindex"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const name = "srv-recommendation"

// Server implements the recommendation service
type Server struct {
	pb.UnimplementedRecommendationServer

	// mu guards hotels, which the startup load, AddHotel and the refresh
	// from MongoDB (changes.Watch) update while requests read it.
	mu     sync.RWMutex
	hotels map[string]Hotel
	uuid   string

	Tracer      opentracing.Tracer
	Port        int
	IpAddr      string
	MongoClient *mongo.Client
	Registry    *registry.Client
}

// Run starts the server
func (s *Server) Run() error {
	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	if s.hotels == nil {
		database := s.MongoClient.Database("recommendation-db")
		// Read the change counter before loading, so no addition is missed.
		version, err := changes.Version(context.TODO(), database)
		if err != nil {
			return fmt.Errorf("read recommendation-db change counter: %v", err)
		}
		if err := changes.EnsureIndex(context.TODO(), database.Collection("recommendation")); err != nil {
			return fmt.Errorf("index added hotels: %v", err)
		}
		s.hotels = loadRecommendations(s.MongoClient)
		go changes.Watch(database, version, s.loadAdded)
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

	pb.RegisterRecommendationServer(srv, s)

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

// GiveRecommendation returns recommendations within a given requirement.
func (s *Server) GetRecommendations(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	res := new(pb.Result)
	log.Trace().Msgf("GetRecommendations")
	require := req.Require
	s.mu.RLock()
	defer s.mu.RUnlock()
	if require == "dis" {
		p1 := &geoindex.GeoPoint{
			Pid:  "",
			Plat: req.Lat,
			Plon: req.Lon,
		}
		min := math.MaxFloat64
		for _, hotel := range s.hotels {
			tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{
				Pid:  "",
				Plat: hotel.HLat,
				Plon: hotel.HLon,
			})) / 1000
			if tmp < min {
				min = tmp
			}
		}
		for _, hotel := range s.hotels {
			tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{
				Pid:  "",
				Plat: hotel.HLat,
				Plon: hotel.HLon,
			})) / 1000
			if tmp == min {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	} else if require == "rate" {
		max := 0.0
		for _, hotel := range s.hotels {
			if hotel.HRate > max {
				max = hotel.HRate
			}
		}
		for _, hotel := range s.hotels {
			if hotel.HRate == max {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	} else if require == "price" {
		min := math.MaxFloat64
		for _, hotel := range s.hotels {
			if hotel.HPrice < min {
				min = hotel.HPrice
			}
		}
		for _, hotel := range s.hotels {
			if hotel.HPrice == min {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	} else {
		log.Warn().Msgf("Wrong require parameter: %v", require)
	}

	return res, nil
}

// loadRecommendations loads hotel recommendations from mongodb.
func loadRecommendations(client *mongo.Client) map[string]Hotel {
	collection := client.Database("recommendation-db").Collection("recommendation")
	curr, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		log.Error().Msgf("Failed get hotels data: %v", err)
	}

	var hotels []Hotel
	curr.All(context.TODO(), &hotels)
	if err != nil {
		log.Error().Msgf("Failed get hotels data: %v", err)
	}

	profiles := make(map[string]Hotel)
	for _, hotel := range hotels {
		profiles[hotel.HId] = hotel
	}

	return profiles
}

type Hotel struct {
	HId    string  `bson:"hotelId"`
	HLat   float64 `bson:"lat"`
	HLon   float64 `bson:"lon"`
	HRate  float64 `bson:"rate"`
	HPrice float64 `bson:"price"`
}

// AddHotel stores a new hotel's attributes in MongoDB and signals the other
// replicas (changes.Bump), which add it to their lists.
func (s *Server) AddHotel(ctx context.Context, req *pb.Hotel) (*pb.AddResult, error) {
	if req.HotelId == "" {
		return nil, status.Error(codes.InvalidArgument, "hotelId is required")
	}
	database := s.MongoClient.Database("recommendation-db")
	_, err := database.Collection("recommendation").InsertOne(ctx, bson.D{
		{Key: "hotelId", Value: req.HotelId}, {Key: "lat", Value: req.Lat}, {Key: "lon", Value: req.Lon},
		{Key: "rate", Value: req.Rate}, {Key: "price", Value: req.Price}, {Key: changes.Added, Value: true},
	})
	if mongo.IsDuplicateKeyError(err) {
		return nil, status.Errorf(codes.AlreadyExists, "hotel %s has recommendation attributes", req.HotelId)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert recommendation attributes: %v", err)
	}
	if err := changes.Bump(ctx, database); err != nil {
		return nil, status.Errorf(codes.Internal, "signal the change: %v", err)
	}
	s.mu.Lock()
	s.hotels[req.HotelId] = Hotel{HId: req.HotelId, HLat: req.Lat, HLon: req.Lon, HRate: req.Rate, HPrice: req.Price}
	s.mu.Unlock()
	return &pb.AddResult{}, nil
}

// loadAdded puts every hotel added after seeding into the list.
func (s *Server) loadAdded(ctx context.Context) error {
	cursor, err := s.MongoClient.Database("recommendation-db").Collection("recommendation").Find(ctx, bson.D{{Key: changes.Added, Value: true}})
	if err != nil {
		return err
	}
	var hotels []Hotel
	if err := cursor.All(ctx, &hotels); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, hotel := range hotels {
		s.hotels[hotel.HId] = hotel
	}
	return nil
}
