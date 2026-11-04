// Package writetest exercises the write paths of the services against a real
// MongoDB (and an in-process memcached). It needs a disposable MongoDB:
// HOTEL_TEST_MONGO=host:port go test ./internal/writetest/ (the test drops
// the services' databases there). Without the variable it is skipped.
package writetest

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/internal/memcfake"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/geo"
	geopb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/geo/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/profile"
	profilepb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/profile/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/rate"
	ratepb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/rate/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/recommendation"
	recpb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/recommendation/proto"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation"
	respb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation/proto"
	"github.com/opentracing/opentracing-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setup(t *testing.T) (*mongo.Client, *memcache.Client, *memcfake.Server) {
	t.Helper()
	address := os.Getenv("HOTEL_TEST_MONGO")
	if address == "" {
		t.Skip("HOTEL_TEST_MONGO is not set")
	}
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+address))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Disconnect(ctx) })
	for _, name := range []string{"geo-db", "profile-db", "rate-db", "recommendation-db", "reservation-db"} {
		if err := client.Database(name).Drop(ctx); err != nil {
			t.Fatal(err)
		}
	}
	unique := func(database, collection, key string) {
		_, err := client.Database(database).Collection(collection).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: key, Value: 1}}, Options: options.Index().SetUnique(true),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	unique("profile-db", "hotels", "id")
	unique("geo-db", "geo", "hotelId")
	unique("recommendation-db", "recommendation", "hotelId")
	unique("reservation-db", "number", "hotelId")
	insert := func(database, collection string, documents ...interface{}) {
		if _, err := client.Database(database).Collection(collection).InsertMany(ctx, documents); err != nil {
			t.Fatal(err)
		}
	}
	insert("profile-db", "hotels",
		bson.D{{Key: "id", Value: "1"}, {Key: "name", Value: "One"}, {Key: "phoneNumber", Value: "111"},
			{Key: "description", Value: "d"}, {Key: "address", Value: bson.D{{Key: "city", Value: "C"}, {Key: "lat", Value: float32(10)}, {Key: "lon", Value: float32(20)}}}})
	plan := func(hotel, code string, total float64) bson.D {
		return bson.D{{Key: "hotelId", Value: hotel}, {Key: "code", Value: code}, {Key: "inDate", Value: "2027-01-01"},
			{Key: "outDate", Value: "2028-01-01"}, {Key: "roomType", Value: bson.D{
				{Key: "bookableRate", Value: total}, {Key: "code", Value: "KNG"}, {Key: "roomDescription", Value: "King"},
				{Key: "totalRate", Value: total}, {Key: "totalRateInclusive", Value: total * 1.1}}}}
	}
	insert("rate-db", "inventory", plan("1", "RACK", 100), plan("1", "BAR", 90))
	insert("reservation-db", "number", bson.D{{Key: "hotelId", Value: "1"}, {Key: "numberOfRoom", Value: int32(10)}})
	insert("geo-db", "geo", bson.D{{Key: "hotelId", Value: "1"}, {Key: "lat", Value: 10.0}, {Key: "lon", Value: 20.0}})
	insert("recommendation-db", "recommendation", bson.D{{Key: "hotelId", Value: "1"}, {Key: "lat", Value: 10.0},
		{Key: "lon", Value: 20.0}, {Key: "rate", Value: 4.0}, {Key: "price", Value: 90.0}})
	cache, err := memcfake.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	opentracing.SetGlobalTracer(opentracing.NoopTracer{})
	return client, memcache.New(cache.Addr()), cache
}

func code(err error) codes.Code { return status.Code(err) }

func TestRateWrites(t *testing.T) {
	mongoClient, memc, cache := setup(t)
	server := &rate.Server{MongoClient: mongoClient, MemcClient: memc}
	ctx := context.Background()
	prices := func() map[string]float64 {
		t.Helper()
		result, err := server.GetRates(ctx, &ratepb.Request{HotelIds: []string{"1"}})
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]float64{}
		for _, plan := range result.RatePlans {
			out[plan.Code] = plan.RoomType.TotalRate
		}
		return out
	}
	waitCached := func(key string) {
		for start := time.Now(); time.Since(start) < 2*time.Second; time.Sleep(5 * time.Millisecond) {
			if value, ok := cache.Value(key); ok && !strings.HasPrefix(string(value), "\x00") {
				return
			}
		}
		t.Fatalf("%s was not cached", key)
	}
	if got := prices(); got["RACK"] != 100 || got["BAR"] != 90 {
		t.Fatalf("seeded %v", got)
	}
	waitCached("1")
	if _, err := server.UpdateRate(ctx, &ratepb.UpdateRateRequest{HotelId: "1", Code: "BAR", BookableRate: 80, TotalRate: 80, TotalRateInclusive: 88}); err != nil {
		t.Fatal(err)
	}
	if got := prices(); got["BAR"] != 80 || got["RACK"] != 100 {
		t.Fatalf("after the update %v", got)
	}
	if _, err := server.UpdateRate(ctx, &ratepb.UpdateRateRequest{HotelId: "1", Code: "NONE"}); code(err) != codes.NotFound {
		t.Fatalf("unknown plan: %v", err)
	}
	if _, err := server.AddRates(ctx, &ratepb.AddRatesRequest{RatePlans: []*ratepb.RatePlan{{
		HotelId: "2", Code: "RACK", InDate: "2027-01-01", OutDate: "2028-01-01",
		RoomType: &ratepb.RoomType{Code: "QN", BookableRate: 50, TotalRate: 50, TotalRateInclusive: 55},
	}}}); err != nil {
		t.Fatal(err)
	}
	result, err := server.GetRates(ctx, &ratepb.Request{HotelIds: []string{"2"}})
	if err != nil || len(result.RatePlans) != 1 || result.RatePlans[0].RoomType.TotalRateInclusive != 55 {
		t.Fatalf("added plans %v %v", result, err)
	}
}

func TestProfileWrites(t *testing.T) {
	mongoClient, memc, _ := setup(t)
	server := &profile.Server{MongoClient: mongoClient, MemcClient: memc}
	ctx := context.Background()
	get := func(id string) *profilepb.Hotel {
		t.Helper()
		result, err := server.GetProfiles(ctx, &profilepb.Request{HotelIds: []string{id}})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Hotels) == 0 {
			return nil
		}
		return result.Hotels[0]
	}
	if hotel := get("1"); hotel.PhoneNumber != "111" || hotel.Address.Lat != 10 {
		t.Fatalf("seeded %v", hotel)
	}
	time.Sleep(50 * time.Millisecond) // let the fill land
	if _, err := server.UpdateProfile(ctx, &profilepb.UpdateRequest{HotelId: "1", PhoneNumber: "222"}); err != nil {
		t.Fatal(err)
	}
	if hotel := get("1"); hotel.PhoneNumber != "222" || hotel.Name != "One" {
		t.Fatalf("after the update %v", hotel)
	}
	if _, err := server.UpdateProfile(ctx, &profilepb.UpdateRequest{HotelId: "9", Name: "x"}); code(err) != codes.NotFound {
		t.Fatalf("unknown hotel: %v", err)
	}
	if hotel := get("9"); hotel != nil {
		t.Fatalf("unknown hotel answered %v", hotel)
	}
	added := &profilepb.Hotel{Id: "9", Name: "Nine", PhoneNumber: "999", Address: &profilepb.Address{City: "N", Lat: 1.5, Lon: 2.5}}
	if _, err := server.AddProfile(ctx, added); err != nil {
		t.Fatal(err)
	}
	if hotel := get("9"); hotel == nil || hotel.Name != "Nine" || hotel.Address.Lon != 2.5 {
		t.Fatalf("added %v", hotel)
	}
	if _, err := server.AddProfile(ctx, added); code(err) != codes.AlreadyExists {
		t.Fatalf("taken ID: %v", err)
	}
}

func TestCapacityWrites(t *testing.T) {
	mongoClient, memc, _ := setup(t)
	server := &reservation.Server{MongoClient: mongoClient, MemcClient: memc}
	ctx := context.Background()
	reserve := func(rooms int32, in, out string) bool {
		t.Helper()
		result, err := server.MakeReservation(ctx, &respb.Request{
			CustomerName: "c", HotelId: []string{"1"}, InDate: in, OutDate: out, RoomNumber: rooms,
		})
		if err != nil {
			t.Fatal(err)
		}
		return len(result.HotelId) == 1
	}
	setCapacity := func(rooms int32) error {
		_, err := server.SetCapacity(ctx, &respb.CapacityRequest{HotelId: "1", Rooms: rooms})
		return err
	}
	available := func(in, out string) bool {
		t.Helper()
		result, err := server.CheckAvailability(ctx, &respb.Request{HotelId: []string{"1"}, InDate: in, OutDate: out, RoomNumber: 1})
		if err != nil {
			t.Fatal(err)
		}
		return len(result.HotelId) == 1
	}
	if !available("3000-01-01", "3000-01-03") || !reserve(6, "3000-01-01", "3000-01-02") || !reserve(2, "3000-01-02", "3000-01-03") {
		t.Fatal("setup reservations failed")
	}
	if err := setCapacity(5); code(err) != codes.FailedPrecondition {
		t.Fatalf("capacity below 6 reserved rooms: %v", err)
	}
	if err := setCapacity(6); err != nil {
		t.Fatal(err)
	}
	if reserve(1, "3000-01-01", "3000-01-02") {
		t.Fatal("reserved beyond the decreased capacity")
	}
	if available("3000-01-01", "3000-01-02") || !available("3000-01-02", "3000-01-03") {
		t.Fatal("availability does not follow the decreased capacity")
	}
	if err := setCapacity(9); err != nil {
		t.Fatal(err)
	}
	if !reserve(3, "3000-01-01", "3000-01-02") || reserve(1, "3000-01-01", "3000-01-02") {
		t.Fatal("the increased capacity was not used at once")
	}
	if _, err := server.SetCapacity(ctx, &respb.CapacityRequest{HotelId: "7", Rooms: 3}); code(err) != codes.NotFound {
		t.Fatalf("unknown hotel: %v", err)
	}
	if _, err := server.AddCapacity(ctx, &respb.CapacityRequest{HotelId: "7", Rooms: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.AddCapacity(ctx, &respb.CapacityRequest{HotelId: "7", Rooms: 3}); code(err) != codes.AlreadyExists {
		t.Fatalf("second capacity: %v", err)
	}
}

// Two replicas: the one that does not serve the addition picks it up from
// MongoDB within the poll interval.
func TestAdditionsReachEveryReplica(t *testing.T) {
	mongoClient, _, _ := setup(t)
	ctx := context.Background()
	geoReplicas := []*geo.Server{{MongoClient: mongoClient}, {MongoClient: mongoClient}}
	recReplicas := []*recommendation.Server{{MongoClient: mongoClient}, {MongoClient: mongoClient}}
	for _, replica := range geoReplicas {
		if err := replica.Load(); err != nil {
			t.Fatal(err)
		}
	}
	for _, replica := range recReplicas {
		if err := replica.Load(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := geoReplicas[0].AddHotel(ctx, &geopb.Point{HotelId: "2", Lat: 10.001, Lon: 20.001}); err != nil {
		t.Fatal(err)
	}
	if _, err := geoReplicas[0].AddHotel(ctx, &geopb.Point{HotelId: "2", Lat: 10.001, Lon: 20.001}); code(err) != codes.AlreadyExists {
		t.Fatalf("second addition: %v", err)
	}
	if _, err := recReplicas[0].AddHotel(ctx, &recpb.Hotel{HotelId: "2", Lat: 10.001, Lon: 20.001, Rate: 4.5, Price: 80}); err != nil {
		t.Fatal(err)
	}
	added := time.Now()
	for {
		nearby, err := geoReplicas[1].Nearby(ctx, &geopb.Request{Lat: 10, Lon: 20})
		if err != nil {
			t.Fatal(err)
		}
		best, err := recReplicas[1].GetRecommendations(ctx, &recpb.Request{Require: "rate"})
		if err != nil {
			t.Fatal(err)
		}
		ids := append([]string(nil), nearby.HotelIds...)
		sort.Strings(ids)
		if strings.Join(ids, ",") == "1,2" && strings.Join(best.HotelIds, ",") == "2" {
			t.Logf("second replicas serve the addition after %v", time.Since(added))
			break
		}
		if time.Since(added) > time.Second {
			t.Fatalf("after 1 s the second replicas answer %v and %v", ids, best.HotelIds)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A replica started later loads it with the seeded data.
	late := &geo.Server{MongoClient: mongoClient}
	if err := late.Load(); err != nil {
		t.Fatal(err)
	}
	if nearby, _ := late.Nearby(ctx, &geopb.Request{Lat: 10, Lon: 20}); len(nearby.HotelIds) != 2 {
		t.Fatalf("restarted replica answers %v", nearby.HotelIds)
	}
}
