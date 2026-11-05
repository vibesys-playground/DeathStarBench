package recommendation

import (
	"context"
	"fmt"
	"math"
	"net"
	"sort"
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

	// mu guards hotels and its indexes, which the startup load, AddHotel and
	// the refresh from MongoDB (changes.Watch) update while requests read
	// them: byLat (the hotels sorted by latitude, for the nearest-hotel scan)
	// and the hotels of the highest rating and of the lowest price.
	mu        sync.RWMutex
	hotels    map[string]Hotel
	byLat     []Hotel
	maxRate   float64
	bestRated []string
	minPrice  float64
	cheapest  []string
	uuid      string

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
		if err := s.Load(); err != nil {
			return err
		}
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

// Load reads the catalog from MongoDB and keeps it current: it reads the
// change counter first, so no addition is missed, then polls it.
func (s *Server) Load() error {
	database := s.MongoClient.Database("recommendation-db")
	version, err := changes.Version(context.TODO(), database)
	if err != nil {
		return fmt.Errorf("read recommendation-db change counter: %v", err)
	}
	if err := changes.EnsureIndex(context.TODO(), database.Collection("recommendation")); err != nil {
		return fmt.Errorf("index added hotels: %v", err)
	}
	s.mu.Lock()
	s.index(loadRecommendations(s.MongoClient))
	s.mu.Unlock()
	go changes.Watch(database, version, s.loadAdded)
	return nil
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
		res.HotelIds = s.nearest(req.Lat, req.Lon)
	} else if require == "rate" {
		res.HotelIds = append(res.HotelIds, s.bestRated...)
	} else if require == "price" {
		res.HotelIds = append(res.HotelIds, s.cheapest...)
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
	s.add(Hotel{HId: req.HotelId, HLat: req.Lat, HLon: req.Lon, HRate: req.Rate, HPrice: req.Price})
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
		s.add(hotel)
	}
	return nil
}

// index builds the indexes over a loaded catalog. The caller holds mu.
func (s *Server) index(hotels map[string]Hotel) {
	s.hotels = make(map[string]Hotel, len(hotels))
	s.byLat = make([]Hotel, 0, len(hotels))
	// As the scans over every hotel did: the highest rating from 0, the lowest
	// price from MaxFloat64.
	s.maxRate, s.bestRated, s.minPrice, s.cheapest = 0, nil, math.MaxFloat64, nil
	for _, hotel := range hotels {
		s.hotels[hotel.HId] = hotel
		s.byLat = append(s.byLat, hotel)
		s.extremes(hotel)
	}
	sort.Slice(s.byLat, func(i, j int) bool { return s.byLat[i].HLat < s.byLat[j].HLat })
}

// add puts a hotel into the catalog and its indexes; a known hotel is
// replaced. The caller holds mu.
func (s *Server) add(hotel Hotel) {
	if old, ok := s.hotels[hotel.HId]; ok {
		if old == hotel {
			return
		}
		hotels := s.hotels
		hotels[hotel.HId] = hotel
		s.index(hotels)
		return
	}
	s.hotels[hotel.HId] = hotel
	at := sort.Search(len(s.byLat), func(i int) bool { return s.byLat[i].HLat >= hotel.HLat })
	s.byLat = append(s.byLat, Hotel{})
	copy(s.byLat[at+1:], s.byLat[at:])
	s.byLat[at] = hotel
	s.extremes(hotel)
}

func (s *Server) extremes(hotel Hotel) {
	switch {
	case hotel.HRate > s.maxRate:
		s.maxRate, s.bestRated = hotel.HRate, []string{hotel.HId}
	case hotel.HRate == s.maxRate:
		s.bestRated = append(s.bestRated, hotel.HId)
	}
	switch {
	case hotel.HPrice < s.minPrice:
		s.minPrice, s.cheapest = hotel.HPrice, []string{hotel.HId}
	case hotel.HPrice == s.minPrice:
		s.cheapest = append(s.cheapest, hotel.HId)
	}
}

// earthRadiusKm is go-geoindex's earth radius: its distance is a haversine
// over it.
const earthRadiusKm = 6371.0

// nearest returns every hotel at the minimum distance (go-geoindex's, in km)
// from the location. It scans outwards in latitude from the location and
// stops once the latitude difference alone is farther than the best: a
// great-circle distance is at least that arc. The caller holds mu (read).
func (s *Server) nearest(lat, lon float64) []string {
	p1 := &geoindex.GeoPoint{Pid: "", Plat: lat, Plon: lon}
	start := sort.Search(len(s.byLat), func(i int) bool { return s.byLat[i].HLat >= lat })
	best := math.MaxFloat64
	var ids []string
	visit := func(hotel *Hotel) {
		distance := float64(geoindex.Distance(p1, &geoindex.GeoPoint{Pid: "", Plat: hotel.HLat, Plon: hotel.HLon})) / 1000
		switch {
		case distance < best:
			best, ids = distance, append(ids[:0], hotel.HId)
		case distance == best:
			ids = append(ids, hotel.HId)
		}
	}
	beyond := func(hotel *Hotel) bool {
		bound := earthRadiusKm * math.Abs(hotel.HLat-lat) * math.Pi / 180
		return bound > best*(1+1e-9)+1e-9
	}
	for low, high := start-1, start; low >= 0 || high < len(s.byLat); {
		progressed := false
		if high < len(s.byLat) && !beyond(&s.byLat[high]) {
			visit(&s.byLat[high])
			high++
			progressed = true
		}
		if low >= 0 && !beyond(&s.byLat[low]) {
			visit(&s.byLat[low])
			low--
			progressed = true
		}
		if !progressed {
			break
		}
	}
	return ids
}
