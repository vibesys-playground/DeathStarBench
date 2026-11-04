package lease

import (
	"sync"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/internal/memcfake"
)

func newClient(t *testing.T) (*memcache.Client, *memcfake.Server) {
	t.Helper()
	server, err := memcfake.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return memcache.New(server.Addr()), server
}

func TestFillStoresTheValueOfAnUninterruptedMiss(t *testing.T) {
	client, server := newClient(t)
	l := Acquire(client, "k")
	if !l.Held() {
		t.Fatal("lease on an absent key was not acquired")
	}
	item, err := client.Get("k")
	if err != nil || !IsLease(item) {
		t.Fatalf("placeholder not visible as a lease: %v %v", item, err)
	}
	if !l.Fill([]byte("v1")) {
		t.Fatal("fill failed")
	}
	if value, _ := server.Value("k"); string(value) != "v1" {
		t.Fatalf("cached %q", value)
	}
}

// The race a plain Set loses: the reader loads the old value, a writer
// updates the database and deletes the key, then the reader fills.
func TestFillAfterAnInvalidationStoresNothing(t *testing.T) {
	client, server := newClient(t)
	l := Acquire(client, "k")
	// ... reader loads "old" from the database; the writer commits "new" ...
	if err := Invalidate(client, "k"); err != nil {
		t.Fatal(err)
	}
	if l.Fill([]byte("old")) {
		t.Fatal("a fill after the invalidation succeeded")
	}
	if _, ok := server.Value("k"); ok {
		t.Fatal("stale value cached")
	}
	// A second reader that started after the write fills the new value.
	if !Acquire(client, "k").Fill([]byte("new")) {
		t.Fatal("fill after the write failed")
	}
	if value, _ := server.Value("k"); string(value) != "new" {
		t.Fatalf("cached %q", value)
	}
}

// The writer's delete lands between the reader's Get of the placeholder and
// its compare-and-swap.
func TestInvalidationBetweenGetAndSwapWins(t *testing.T) {
	client, server := newClient(t)
	l := Acquire(client, "k")
	writer := memcache.New(server.Addr())
	server.BeforeStore = func(verb, key string) {
		if verb == "cas" {
			server.BeforeStore = nil
			if err := Invalidate(writer, key); err != nil {
				t.Error(err)
			}
			// A newer reader re-acquires, so the key exists again but
			// with another placeholder (another CAS identifier).
			Acquire(writer, key)
		}
	}
	if l.Fill([]byte("old")) {
		t.Fatal("swap after an invalidation succeeded")
	}
	item, err := client.Get("k")
	if err != nil || !IsLease(item) {
		t.Fatalf("key holds %v (%v), want the newer placeholder", item, err)
	}
}

func TestOnlyOneReaderFills(t *testing.T) {
	client, server := newClient(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	filled := 0
	for n := 0; n < 16; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if Acquire(client, "k").Fill([]byte("v")) {
				mu.Lock()
				filled++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if filled != 1 {
		t.Fatalf("%d readers filled, want 1", filled)
	}
	if value, _ := server.Value("k"); string(value) != "v" {
		t.Fatalf("cached %q", value)
	}
}

func TestInvalidateOfAnAbsentKey(t *testing.T) {
	client, _ := newClient(t)
	if err := Invalidate(client, "absent"); err != nil {
		t.Fatal(err)
	}
}
