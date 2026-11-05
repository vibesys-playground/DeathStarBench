package snapshot

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

func generate(t *testing.T, spec dataset.Spec) *dataset.Dataset {
	t.Helper()
	d, err := dataset.Generate(spec)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func small() dataset.Spec {
	return dataset.Spec{Format: dataset.Format, Seed: 0x686F74656C2D7633, Hotels: 2_000, Users: 200}
}

// TestTheVersionNamesTheData: one spec has one version in every process (the builder's and
// the bench's agree), and any change of the spec names another snapshot.
func TestTheVersionNamesTheData(t *testing.T) {
	spec := small()
	version := Version(generate(t, spec))
	if len(version) != 20 || strings.Trim(version, "0123456789abcdef") != "" {
		t.Fatalf("version %q is not 20 hex digits", version)
	}
	if again := Version(generate(t, spec)); again != version {
		t.Fatalf("two generations of one spec have versions %s and %s", version, again)
	}
	seen := map[string]string{version: "the spec"}
	for name, change := range map[string]func(*dataset.Spec){
		"seed":   func(s *dataset.Spec) { s.Seed++ },
		"hotels": func(s *dataset.Spec) { s.Hotels++ },
		"users":  func(s *dataset.Spec) { s.Users++ },
	} {
		other := spec
		change(&other)
		v := Version(generate(t, other))
		if previous, ok := seen[v]; ok {
			t.Errorf("a change of the %s keeps version %s (%s's)", name, v, previous)
		}
		seen[v] = name
	}
}

// TestTheVersionPinsItsRecordedValue: the version of TestSpec's dataset is recorded, so a
// change of what the generator writes (which must name a new snapshot) shows up here and is
// made deliberately: re-record the value when the data or the collections change on purpose.
func TestTheVersionPinsItsRecordedValue(t *testing.T) {
	const recorded = "751a170140790e09a127"
	if got := Version(generate(t, small())); got != recorded {
		t.Fatalf("Version = %s, recorded %s: the generator's documents, the collections, Layout or MongoImage changed; "+
			"if on purpose, record the new value", got, recorded)
	}
}

func TestSpreadCoversBothEnds(t *testing.T) {
	for _, n := range []int{1, 2, 5, 62, 63, 64, 1000, 1_500_000} {
		got := spread(n)
		if got[0] != 0 || got[len(got)-1] != n-1 || !sort.IntsAreSorted(got) {
			t.Fatalf("spread(%d) = %v", n, got)
		}
		for k := 1; k < len(got); k++ {
			if got[k] == got[k-1] {
				t.Fatalf("spread(%d) repeats %d", n, got[k])
			}
		}
		if want := min(n, versionSamples+2); len(got) != want {
			t.Fatalf("spread(%d) has %d indices, want %d", n, len(got), want)
		}
	}
}

// TestCountsAreTheDocuments: Count (what Verify compares with the server's) is the number of
// documents the builder writes.
func TestCountsAreTheDocuments(t *testing.T) {
	d := generate(t, small())
	for _, c := range Collections() {
		total := int64(0)
		switch {
		case c.PerHotel != nil:
			for i := range d.HotelCount() {
				total += int64(len(c.PerHotel(d, i, nil)))
			}
		case c.PerUser != nil:
			for u := range d.Users {
				total += int64(len(c.PerUser(d, u, nil)))
			}
		}
		if got := c.Count(d); got != total {
			t.Errorf("%s.%s: Count %d, documents %d", c.Database, c.Name, got, total)
		}
	}
}

func TestKeyOrderIsTheIDsOrder(t *testing.T) {
	d := generate(t, small())
	order := KeyOrder(d)
	if len(order) != d.HotelCount() {
		t.Fatalf("%d of %d hotels", len(order), d.HotelCount())
	}
	seen := make([]bool, d.HotelCount())
	for k, i := range order {
		if seen[i] {
			t.Fatalf("hotel index %d twice", i)
		}
		seen[i] = true
		if k > 0 && dataset.HotelID(order[k-1]) >= dataset.HotelID(i) {
			t.Fatalf("%s before %s", dataset.HotelID(order[k-1]), dataset.HotelID(i))
		}
	}
	users := UserOrder(d)
	for k := 1; k < len(users); k++ {
		if d.Users[users[k-1]].Username >= d.Users[users[k]].Username {
			t.Fatal("UserOrder is not username order")
		}
	}
}

func TestPasswordHashIsTheUserServices(t *testing.T) {
	// sha256("password"), as hotelReservation's user service stores it.
	if got := PasswordHash("password"); got != "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8" {
		t.Fatal(got)
	}
}

// TestSweepDeletesOnlyStaleUnheldVersions: a version unused for staleAfter goes, a recent one
// and the version being ensured stay.
func TestSweepDeletesOnlyStaleUnheldVersions(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	for name, used := range map[string]time.Time{"keep": now.Add(-10 * staleAfter), "old": now.Add(-2 * staleAfter),
		"recent": now.Add(-staleAfter / 2)} {
		if err := os.MkdirAll(filepath.Join(root, name, "geo-db"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := touch(filepath.Join(root, name, usedFile), used); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "unused-marker-missing"), 0o755); err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.Log = t.Logf
	sweepStale(root, "keep", now, opts)
	for name, want := range map[string]bool{"keep": true, "old": false, "recent": true, "unused-marker-missing": true} {
		if _, err := os.Stat(filepath.Join(root, name)); (err == nil) != want {
			t.Errorf("%s: exists %v, want %v", name, err == nil, want)
		}
	}
}

// TestBuildRestoreVerify builds a small snapshot with a real mongod ($HOTEL_MONGOD names the
// binary of the data tier's image, MongoImage), starts a mongod per database on a copy of its
// files as the manifest's restore does (journaled), and verifies it; a second Ensure builds
// nothing.
func TestBuildRestoreVerify(t *testing.T) {
	mongod := os.Getenv("HOTEL_MONGOD")
	if mongod == "" {
		t.Skip("$HOTEL_MONGOD names no mongod binary")
	}
	d := generate(t, small())
	root := t.TempDir()
	opts := DefaultOptions()
	opts.Mongod, opts.Log = mongod, t.Logf
	for database := range opts.CacheGB {
		opts.CacheGB[database] = 0.25
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	records, err := Ensure(ctx, d, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	version := Version(d)
	for k, record := range records {
		if record.Version != version || record.Database != Databases[k] {
			t.Fatalf("record %d: %+v", k, record)
		}
	}
	again, err := Ensure(ctx, d, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	for k := range again {
		if !again[k].Built.Equal(records[k].Built) {
			t.Fatalf("the second Ensure rebuilt %s", Databases[k])
		}
	}

	clients := map[string]*mongo.Client{}
	for k, database := range Databases {
		restored := filepath.Join(t.TempDir(), "db")
		if out, err := exec.Command("cp", "-a", filepath.Join(root, version, database, DataDir), restored).CombinedOutput(); err != nil {
			t.Fatalf("copy %s: %v %s", database, err, out)
		}
		port := 28100 + k
		server := exec.CommandContext(ctx, mongod, "--dbpath", restored, "--port", fmt.Sprint(port), "--bind_ip", "127.0.0.1",
			"--wiredTigerCacheSizeGB", "0.25")
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = server.Process.Kill(); _ = server.Wait() })
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(fmt.Sprintf("mongodb://127.0.0.1:%d", port)))
		if err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(time.Minute); client.Ping(ctx, nil) != nil; time.Sleep(200 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: the restored mongod does not answer", database)
			}
		}
		clients[database] = client
	}
	if err := Verify(ctx, d, clients, 100, 1); err != nil {
		t.Fatal(err)
	}
	// A server holding other data fails the check.
	if _, err := clients["user-db"].Database("user-db").Collection("user").DeleteOne(ctx, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if err := Verify(ctx, d, clients, 100, 1); err == nil || !strings.Contains(err.Error(), "user-db.user") {
		t.Fatalf("Verify after a deletion: %v", err)
	}
}
