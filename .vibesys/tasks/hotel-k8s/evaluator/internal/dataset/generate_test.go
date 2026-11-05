package dataset

import (
	"reflect"
	"strings"
	"testing"
)

func small(seed uint64) Spec { return Spec{Format: Format, Seed: seed, Hotels: 3_000, Users: 300} }

// TestGenerateIsAFunctionOfTheSpec: the generator, the oracle and the snapshot builder each
// generate the data from the spec, so two generations of one spec must agree on every hotel,
// night and user, and another seed must give other data.
func TestGenerateIsAFunctionOfTheSpec(t *testing.T) {
	a, err := Generate(small(7))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Generate(small(7))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.hotels, b.hotels) || !reflect.DeepEqual(a.Users, b.Users) || !reflect.DeepEqual(a.Cities, b.Cities) {
		t.Fatal("two generations of one spec differ")
	}
	for i := range a.HotelCount() {
		if !reflect.DeepEqual(a.Hotel(i), b.Hotel(i)) || !reflect.DeepEqual(a.Occupancy(i), b.Occupancy(i)) {
			t.Fatalf("hotel index %d differs between two generations", i)
		}
	}
	c, err := Generate(small(8))
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(a.hotels, c.hotels) || reflect.DeepEqual(a.Occupancy(0), c.Occupancy(0)) {
		t.Fatal("another seed gave the same data")
	}
}

// TestHotelAgreesWithTheCore: Hotel(i) regenerates a hotel's full record; what it says must be
// what the compact accessors (the read paths' reference) say.
func TestHotelAgreesWithTheCore(t *testing.T) {
	d := Default()
	for i := range d.HotelCount() {
		h := d.Hotel(i)
		lat, lon := d.Location(i)
		switch {
		case h.ID != HotelID(i):
			t.Fatalf("hotel index %d has ID %q", i, h.ID)
		case h.Lat != lat || h.Lon != lon || h.Rating != d.Rating(i) || h.Price != d.Price(i):
			t.Fatalf("hotel %s: record and core disagree", h.ID)
		case h.Capacity != d.Capacity(i) || h.City != d.CityOf(i) || len(h.RatePlans) != d.PlanCount(i):
			t.Fatalf("hotel %s: capacity, city or plans disagree", h.ID)
		}
		if got, ok := d.HotelIndex(h.ID); !ok || got != i {
			t.Fatalf("HotelIndex(%q) = %d, %v", h.ID, got, ok)
		}
	}
	for _, id := range []string{"0", "", "01", "x", HotelID(d.HotelCount())} {
		if _, ok := d.HotelIndex(id); ok {
			t.Fatalf("HotelIndex(%q) names a hotel", id)
		}
	}
}

// TestTheSeededNightsAreNeverFull: every night keeps a room free, so an availability search
// over the seeded data answers as over an empty calendar, and SeededMaxRooms bounds them.
func TestTheSeededNightsAreNeverFull(t *testing.T) {
	d := Default()
	booked, nights := 0.0, 0.0
	for i := range d.HotelCount() {
		rooms := d.Occupancy(i)
		if len(rooms) != RateWindowDays {
			t.Fatalf("hotel index %d has %d nights", i, len(rooms))
		}
		most := 0
		for n, r := range rooms {
			if r < 0 || int(r) >= d.Capacity(i) {
				t.Fatalf("hotel index %d night %s: %d of %d rooms booked", i, Night(n), r, d.Capacity(i))
			}
			most = max(most, int(r))
			booked += float64(r)
			nights += float64(d.Capacity(i))
		}
		if d.SeededMaxRooms(i) != most {
			t.Fatalf("hotel index %d: SeededMaxRooms %d, nights' most %d", i, d.SeededMaxRooms(i), most)
		}
	}
	if share := booked / nights; share < 0.3 || share > 0.6 {
		t.Fatalf("%.2f of the rooms are booked, want a busy but open calendar", share)
	}
	if Night(0) != "2027-01-01" || Night(RateWindowDays-1) != "2028-12-30" {
		t.Fatalf("the window runs %s to %s", Night(0), Night(RateWindowDays-1))
	}
}

func TestParseSpecRejectsWhatItDoesNotKnow(t *testing.T) {
	if _, err := ParseSpec([]byte(`{"format":3,"seed":1,"hotels":30,"users":1}`)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`{"format":3,"seed":1,"hotels":30,"users":1,"nights":5}`,
		`{"format":2,"seed":1,"hotels":30,"users":1}`,
		`{"format":3,"seed":1,"hotels":3,"users":1}`,
		`{"format":3,"seed":1,"hotels":30,"users":0}`,
	} {
		if _, err := ParseSpec([]byte(raw)); err == nil {
			t.Errorf("ParseSpec(%s) accepted it", raw)
		} else if strings.TrimSpace(err.Error()) == "" {
			t.Errorf("ParseSpec(%s): empty error", raw)
		}
	}
}
