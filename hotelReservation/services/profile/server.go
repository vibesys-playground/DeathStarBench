package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/lease"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/registry"
	pb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/profile/proto"
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

const name = "srv-profile"

// Server implements the profile service
type Server struct {
	pb.UnimplementedProfileServer

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

	log.Trace().Msgf("in run s.IpAddr = %s, port = %d", s.IpAddr, s.Port)

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

	pb.RegisterProfileServer(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to configure listener: %v", err)
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

// GetProfiles returns hotel profiles for requested IDs
func (s *Server) GetProfiles(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	log.Trace().Msgf("In GetProfiles")

	var wg sync.WaitGroup
	var mutex sync.Mutex

	// one hotel should only have one profile
	hotelIds := make([]string, 0)
	profileMap := make(map[string]struct{})
	for _, hotelId := range req.HotelIds {
		hotelIds = append(hotelIds, hotelId)
		profileMap[hotelId] = struct{}{}
	}

	memSpan, _ := opentracing.StartSpanFromContext(ctx, "memcached_get_profile")
	memSpan.SetTag("span.kind", "client")
	resMap, err := s.MemcClient.GetMulti(hotelIds)
	memSpan.Finish()

	res := new(pb.Result)
	hotels := make([]*pb.Hotel, 0)

	if err != nil && err != memcache.ErrCacheMiss {
		log.Panic().Msgf("Tried to get hotelIds [%v], but got memmcached error = %s", hotelIds, err)
	} else {
		for hotelId, item := range resMap {
			if lease.IsLease(item) {
				continue // another request is filling it: a miss
			}
			profileStr := string(item.Value)
			log.Trace().Msgf("memc hit with %v", profileStr)

			hotelProf := new(pb.Hotel)
			json.Unmarshal(item.Value, hotelProf)
			hotels = append(hotels, hotelProf)
			delete(profileMap, hotelId)
		}

		wg.Add(len(profileMap))
		for hotelId := range profileMap {
			go func(hotelId string) {
				defer wg.Done()
				var hotelProf *pb.Hotel

				// Take the fill lease before reading MongoDB, so a profile
				// update between the read and the fill cannot be undone.
				fill := lease.Acquire(s.MemcClient, hotelId)

				collection := s.MongoClient.Database("profile-db").Collection("hotels")

				mongoSpan, _ := opentracing.StartSpanFromContext(ctx, "mongo_profile")
				mongoSpan.SetTag("span.kind", "client")
				err := collection.FindOne(context.TODO(), bson.D{{"id", hotelId}}).Decode(&hotelProf)
				mongoSpan.Finish()

				if err != nil {
					log.Error().Msgf("Failed get hotels data: %v", err)
					return
				}

				mutex.Lock()
				hotels = append(hotels, hotelProf)
				mutex.Unlock()

				profJson, err := json.Marshal(hotelProf)
				if err != nil {
					log.Error().Msgf("Failed to marshal hotel [id: %v] with err: %v", hotelProf.Id, err)
					return
				}

				// write to memcached
				go fill.Fill(profJson)
			}(hotelId)
		}
	}
	wg.Wait()

	res.Hotels = hotels
	log.Trace().Msgf("In GetProfiles after getting resp")
	return res, nil
}

// UpdateProfile changes the given non-empty fields of a hotel's profile in
// MongoDB, then invalidates the cached profile.
func (s *Server) UpdateProfile(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResult, error) {
	fields := bson.D{}
	if req.Name != "" {
		fields = append(fields, bson.E{Key: "name", Value: req.Name})
	}
	if req.PhoneNumber != "" {
		fields = append(fields, bson.E{Key: "phoneNumber", Value: req.PhoneNumber})
	}
	if req.HotelId == "" || len(fields) == 0 {
		return nil, status.Error(codes.InvalidArgument, "hotelId and a field to change are required")
	}
	collection := s.MongoClient.Database("profile-db").Collection("hotels")
	result, err := collection.UpdateOne(ctx, bson.D{{Key: "id", Value: req.HotelId}}, bson.D{{Key: "$set", Value: fields}})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update profile: %v", err)
	}
	if result.MatchedCount == 0 {
		return nil, status.Errorf(codes.NotFound, "no hotel %s", req.HotelId)
	}
	if err := lease.Invalidate(s.MemcClient, req.HotelId); err != nil {
		return nil, status.Errorf(codes.Unavailable, "invalidate cached profile: %v", err)
	}
	return &pb.UpdateResult{}, nil
}

// AddProfile stores the profile of a new hotel. The collection's unique
// index on "id" rejects an ID that is taken.
func (s *Server) AddProfile(ctx context.Context, req *pb.Hotel) (*pb.UpdateResult, error) {
	if req.Id == "" || req.Address == nil {
		return nil, status.Error(codes.InvalidArgument, "id and address are required")
	}
	collection := s.MongoClient.Database("profile-db").Collection("hotels")
	_, err := collection.InsertOne(ctx, storedHotel{
		Id: req.Id, Name: req.Name, PhoneNumber: req.PhoneNumber, Description: req.Description,
		Address: &storedAddress{
			StreetNumber: req.Address.StreetNumber, StreetName: req.Address.StreetName,
			City: req.Address.City, State: req.Address.State, Country: req.Address.Country,
			PostalCode: req.Address.PostalCode, Lat: req.Address.Lat, Lon: req.Address.Lon,
		},
	})
	if mongo.IsDuplicateKeyError(err) {
		return nil, status.Errorf(codes.AlreadyExists, "hotel %s exists", req.Id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert profile: %v", err)
	}
	if err := lease.Invalidate(s.MemcClient, req.Id); err != nil {
		return nil, status.Errorf(codes.Unavailable, "invalidate cached profile: %v", err)
	}
	return &pb.UpdateResult{}, nil
}

// storedHotel is a profile-db.hotels document (cmd/profile/db.go Hotel).
type storedHotel struct {
	Id          string         `bson:"id"`
	Name        string         `bson:"name"`
	PhoneNumber string         `bson:"phoneNumber"`
	Description string         `bson:"description"`
	Address     *storedAddress `bson:"address"`
}

type storedAddress struct {
	StreetNumber string  `bson:"streetNumber"`
	StreetName   string  `bson:"streetName"`
	City         string  `bson:"city"`
	State        string  `bson:"state"`
	Country      string  `bson:"country"`
	PostalCode   string  `bson:"postalCode"`
	Lat          float32 `bson:"lat"`
	Lon          float32 `bson:"lon"`
}
