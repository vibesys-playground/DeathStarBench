package reservation

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/internal/memcfake"
	pb "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation/proto"
	"github.com/opentracing/opentracing-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func newCachedServer(t *testing.T) (*Server, *memcfake.Server) {
	t.Helper()
	cache, err := memcfake.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	opentracing.SetGlobalTracer(opentracing.NoopTracer{})
	// A client that never connects: the cached paths must not need MongoDB.
	mongoClient, err := mongo.NewClient(options.Client().ApplyURI("mongodb://127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	return &Server{MemcClient: memcache.New(cache.Addr()), MongoClient: mongoClient}, cache
}

// countKey is the memcached key of a hotel's reservation count for the night
// that starts the day before night (MakeReservation's key).
func countKey(hotelID, night string) string { return hotelID + "_" + night + "_" + night }

func TestCheckAvailabilityUsesCachedCapacities(t *testing.T) {
	server, cache := newCachedServer(t)
	// Hotel 1 has 10 rooms and 3 reserved on the first night; hotel 2 has 4
	// rooms, all reserved on the second night; hotel 3 has nothing reserved.
	for id, capacity := range map[string]int{"1": 10, "2": 4, "3": 7} {
		cache.Put(id+"_cap", []byte(strconv.Itoa(capacity)))
	}
	counts := map[string]int{
		countKey("1", "2027-03-02"): 3, countKey("1", "2027-03-03"): 0,
		countKey("2", "2027-03-02"): 1, countKey("2", "2027-03-03"): 4,
		countKey("3", "2027-03-02"): 0, countKey("3", "2027-03-03"): 0,
	}
	for key, count := range counts {
		cache.Put(key, []byte(strconv.Itoa(count)))
	}
	check := func(rooms int32, want ...string) {
		t.Helper()
		result, err := server.CheckAvailability(context.Background(), &pb.Request{
			HotelId: []string{"1", "2", "3", "1"}, InDate: "2027-03-01", OutDate: "2027-03-03", RoomNumber: rooms,
		})
		if err != nil {
			t.Fatal(err)
		}
		got := append([]string(nil), result.HotelId...)
		sort.Strings(got)
		if len(got) != len(want) {
			t.Fatalf("%d rooms: available %v, want %v", rooms, got, want)
		}
		for index := range got {
			if got[index] != want[index] {
				t.Fatalf("%d rooms: available %v, want %v", rooms, got, want)
			}
		}
	}
	// Before the fix every hotel with a cached count was hidden: the cached
	// capacities were looked up by memcached key, so they read as 0.
	check(1, "1", "3")
	check(7, "1", "3")
	check(8)
}
