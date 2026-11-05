package hotel

import (
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

func sortedCopy(ids []string) string {
	ids = append([]string(nil), ids...)
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

// Added points must behave as if the index had been built with them: compare
// kNearest over (seeded + extra) with an index built from all the points.
func TestAddedPointsJoinTheGeoIndex(t *testing.T) {
	reference := DefaultReference()
	random := rand.New(rand.NewSource(3))
	points := make([]geoPoint, 0, reference.Data.HotelCount()+8)
	for _, hotel := range allHotels(reference.Data) {
		points = append(points, geoPoint{id: hotel.ID, lat: hotel.Lat, lon: hotel.Lon})
	}
	var added []*dataset.Hotel
	for n := 0; n < 8; n++ {
		anchor := reference.Data.Hotel(random.Intn(reference.Data.HotelCount()))
		hotel := dataset.AddedHotel(dataset.AddedHotelID(1, n), uint64(n), anchor.Lat+0.0007, anchor.Lon-0.0004)
		added = append(added, &hotel)
		points = append(points, geoPoint{id: hotel.ID, lat: hotel.Lat, lon: hotel.Lon})
	}
	combined := newGeoIndex(points)
	additions := NewAdditions(added...)
	changed := 0
	for n := 0; n < 400; n++ {
		anchor := added[n%len(added)]
		lat := anchor.Lat + (random.Float64()-0.5)/200
		lon := anchor.Lon + (random.Float64()-0.5)/200
		want, wantExact := combined.kNearest(lat, lon, nil)
		got, gotExact := reference.geo.kNearest(lat, lon, additions.index)
		if wantExact != gotExact || (wantExact && sortedCopy(want) != sortedCopy(got)) {
			t.Fatalf("query %v,%v: got %v (%v), want %v (%v)", lat, lon, got, gotExact, want, wantExact)
		}
		base, _ := reference.geo.kNearest(lat, lon, nil)
		if sortedCopy(base) != sortedCopy(got) {
			changed++
		}
	}
	if changed < 100 {
		t.Fatalf("only %d of 400 searches near added hotels changed", changed)
	}
}

func TestStateAppliesWrites(t *testing.T) {
	reference := DefaultReference()
	state := NewState(reference)
	seeded := reference.Data.Hotel(41)

	if err := state.SetProfile(seeded.ID, "", "+1 555"); err != nil {
		t.Fatal(err)
	}
	if profile, _ := state.Profile(seeded.ID); profile.Name != seeded.Name || profile.Phone != "+1 555" {
		t.Fatalf("profile %+v", profile)
	}
	code := seeded.RatePlans[len(seeded.RatePlans)-1].Code
	if err := state.SetPrices(seeded.ID, code, Prices{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	for _, plan := range state.Plans(seeded.ID) {
		changed := plan.BookableRate == 1 && plan.TotalRate == 2 && plan.TotalRateInclusive == 3
		if changed != (plan.Code == code) {
			t.Fatalf("plan %+v", plan)
		}
	}
	if seeded.RatePlans[len(seeded.RatePlans)-1].TotalRate == 2 {
		t.Fatal("a write changed the dataset")
	}
	if err := state.SetPrices(seeded.ID, "NOPE", Prices{}); err == nil {
		t.Fatal("a write to an unknown plan was accepted")
	}
	if err := state.SetCapacity(seeded.ID, 3); err != nil {
		t.Fatal(err)
	}
	if rooms, _ := state.Capacity(seeded.ID); rooms != 3 {
		t.Fatalf("capacity %d", rooms)
	}
	if err := state.SetProfile("nope", "x", ""); err == nil {
		t.Fatal("a write to an unknown hotel was accepted")
	}

	// An added hotel in a sandbox: search and the distance recommendation
	// there answer it; rating and price above the extremes take over the
	// "rate" and "price" recommendations.
	lat, lon := dataset.SandboxPoint(5, 99)
	added := dataset.AddedHotel(dataset.AddedHotelID(7, 1), 1, lat, lon)
	added.Rating, added.Price = reference.MaxRating()+0.01, reference.MinPrice()-0.01
	if err := state.Add(added); err != nil {
		t.Fatal(err)
	}
	if err := state.Add(added); err == nil {
		t.Fatal("a taken ID was added twice")
	}
	text := func(value float64) string { return strconv.FormatFloat(value, 'f', 6, 64) }
	ids, exact, err := state.SearchCandidates(text(lat+0.001), text(lon))
	if err != nil || !exact || sortedCopy(ids) != added.ID {
		t.Fatalf("sandbox search %v %v %v", ids, exact, err)
	}
	if base, _, _ := reference.SearchCandidates(text(lat), text(lon)); len(base) != 0 {
		t.Fatalf("the seeded data has hotels in the sandbox: %v", base)
	}
	if got := state.NearestHotels(lat, lon+0.01); sortedCopy(got) != added.ID {
		t.Fatalf("nearest %v", got)
	}
	if got := state.BestRated(); sortedCopy(got) != added.ID {
		t.Fatalf("best rated %v", got)
	}
	if got := state.Cheapest(); sortedCopy(got) != added.ID {
		t.Fatalf("cheapest %v", got)
	}
	// Seeded answers elsewhere are unchanged.
	city := reference.Data.Cities[0]
	if sortedCopy(state.NearestHotels(city.Lat, city.Lon)) != sortedCopy(reference.NearestHotels(city.Lat, city.Lon)) {
		t.Fatal("an addition in a sandbox changed a city's nearest hotel")
	}
	if profile, ok := state.Profile(added.ID); !ok || profile.Name != added.Name {
		t.Fatalf("added profile %+v", profile)
	}
	if got := strings.Join(state.WrittenProfiles(), ","); got != seeded.ID {
		t.Fatalf("written profiles %s", got)
	}
}
