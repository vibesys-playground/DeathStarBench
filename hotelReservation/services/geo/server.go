package geo

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/changes"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/registry"
	pb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/geo/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/tls"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/hailocab/go-geoindex"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	name             = "srv-geo"
	maxSearchRadius  = 10
	maxSearchResults = 5
)

// Server implements the geo service
type Server struct {
	pb.UnimplementedGeoServer

	// mu guards index, which the startup load, AddHotel and the refresh
	// from MongoDB (changes.Watch) update while requests read it.
	mu    sync.RWMutex
	index *geoindex.ClusteringIndex
	uuid  string

	Registry    *registry.Client
	Tracer      opentracing.Tracer
	Port        int
	IpAddr      string
	MongoClient *mongo.Client
}

// Run starts the server
func (s *Server) Run() error {
	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	if s.index == nil {
		database := s.MongoClient.Database("geo-db")
		// Read the change counter before loading, so no addition is missed.
		version, err := changes.Version(context.TODO(), database)
		if err != nil {
			return fmt.Errorf("read geo-db change counter: %v", err)
		}
		if err := changes.EnsureIndex(context.TODO(), database.Collection("geo")); err != nil {
			return fmt.Errorf("index added hotels: %v", err)
		}
		s.index = newGeoIndex(s.MongoClient)
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

	pb.RegisterGeoServer(srv, s)

	// listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
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

// Nearby returns all hotels within a given distance.
func (s *Server) Nearby(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	log.Trace().Msgf("In geo Nearby")

	var (
		points = s.getNearbyPoints(ctx, float64(req.Lat), float64(req.Lon))
		res    = &pb.Result{}
	)

	log.Trace().Msgf("geo after getNearbyPoints, len = %d", len(points))

	for _, p := range points {
		log.Trace().Msgf("In geo Nearby return hotelId = %s", p.Id())
		res.HotelIds = append(res.HotelIds, p.Id())
	}

	return res, nil
}

func (s *Server) getNearbyPoints(ctx context.Context, lat, lon float64) []geoindex.Point {
	log.Trace().Msgf("In geo getNearbyPoints, lat = %f, lon = %f", lat, lon)

	center := &geoindex.GeoPoint{
		Pid:  "",
		Plat: lat,
		Plon: lon,
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.KNearest(
		center,
		maxSearchResults,
		geoindex.Km(maxSearchRadius), func(p geoindex.Point) bool {
			return true
		},
	)
}

// newGeoIndex returns a geo index with points loaded
func newGeoIndex(client *mongo.Client) *geoindex.ClusteringIndex {
	log.Trace().Msg("new geo newGeoIndex")

	collection := client.Database("geo-db").Collection("geo")
	curr, err := collection.Find(context.TODO(), bson.D{})
	if err != nil {
		log.Error().Msgf("Failed get geo data: %v", err)
	}

	var points []*point
	curr.All(context.TODO(), &points)
	if err != nil {
		log.Error().Msgf("Failed get geo data: %v", err)
	}

	// add points to index
	index := geoindex.NewClusteringIndex()
	for _, point := range points {
		index.Add(point)
	}

	return index
}

type point struct {
	Pid  string  `bson:"hotelId"`
	Plat float64 `bson:"lat"`
	Plon float64 `bson:"lon"`
}

// Implement Point interface
func (p *point) Lat() float64 { return p.Plat }
func (p *point) Lon() float64 { return p.Plon }
func (p *point) Id() string   { return p.Pid }

// AddHotel stores the location of a new hotel in MongoDB and signals the
// other replicas (changes.Bump), which add it to their indexes.
func (s *Server) AddHotel(ctx context.Context, req *pb.Point) (*pb.AddResult, error) {
	if req.HotelId == "" {
		return nil, status.Error(codes.InvalidArgument, "hotelId is required")
	}
	database := s.MongoClient.Database("geo-db")
	_, err := database.Collection("geo").InsertOne(ctx, bson.D{
		{Key: "hotelId", Value: req.HotelId}, {Key: "lat", Value: req.Lat}, {Key: "lon", Value: req.Lon},
		{Key: changes.Added, Value: true},
	})
	if mongo.IsDuplicateKeyError(err) {
		return nil, status.Errorf(codes.AlreadyExists, "hotel %s has a location", req.HotelId)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert location: %v", err)
	}
	if err := changes.Bump(ctx, database); err != nil {
		return nil, status.Errorf(codes.Internal, "signal the change: %v", err)
	}
	s.mu.Lock()
	s.index.Add(&point{Pid: req.HotelId, Plat: req.Lat, Plon: req.Lon})
	s.mu.Unlock()
	return &pb.AddResult{}, nil
}

// loadAdded adds every hotel added after seeding to the index (adding a
// point that is indexed already replaces it).
func (s *Server) loadAdded(ctx context.Context) error {
	cursor, err := s.MongoClient.Database("geo-db").Collection("geo").Find(ctx, bson.D{{Key: changes.Added, Value: true}})
	if err != nil {
		return err
	}
	var points []*point
	if err := cursor.All(ctx, &points); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range points {
		s.index.Add(p)
	}
	return nil
}
