package dataset

import (
	"math"
	"strconv"
	"testing"
	"time"
)

// datasetDigest pins Generate(Seed). Any change to the generator that moves
// it changes the task's data, and results measured before and after do not
// compare: bump the task version, not just this constant.
const datasetDigest = "7fcac12bab424667ef693f97f9ca337e44fde04e5dc3fb0b0306a194e47db63f"

func TestGenerateIsDeterministicAndPinned(t *testing.T) {
	first, second := Generate(Seed), Generate(Seed)
	if first.Digest() != second.Digest() {
		t.Fatal("two generations from the same seed differ")
	}
	if got := first.Digest(); got != datasetDigest {
		t.Fatalf("dataset digest = %s, want %s", got, datasetDigest)
	}
	if Default().Digest() != datasetDigest {
		t.Fatal("Default() is not Generate(Seed)")
	}
	if Generate(Seed+1).Digest() == datasetDigest {
		t.Fatal("a different seed produced the same dataset")
	}
}

func TestShape(t *testing.T) {
	data := Default()
	if len(data.Cities) != 20 || len(data.Hotels) != HotelCount || len(data.Users) != UserCount {
		t.Fatalf("cities=%d hotels=%d users=%d", len(data.Cities), len(data.Hotels), len(data.Users))
	}

	// Zipf over cities: counts fall with rank, the first holds about 1/H(20).
	total := 0
	for index, city := range data.Cities {
		total += len(city.Hotels)
		if index > 0 && len(city.Hotels) > len(data.Cities[index-1].Hotels) {
			t.Fatalf("city %s has more hotels than a more popular city", city.Name)
		}
		expected := float64(HotelCount) / float64(index+1) / 3.5977396571436819
		if math.Abs(float64(len(city.Hotels))-expected) > 1 {
			t.Fatalf("city %s has %d hotels, Zipf share %.1f", city.Name, len(city.Hotels), expected)
		}
	}
	if total != HotelCount {
		t.Fatalf("cities hold %d hotels", total)
	}

	window := map[string]bool{}
	for day := 0; day <= RateWindowDays; day++ {
		window[RateWindowStart.AddDate(0, 0, day).Format(time.DateOnly)] = true
	}
	ids := map[string]bool{}
	locations := map[[2]float64]bool{}
	ranks := make([]map[int]bool, len(data.Cities))
	for index := range ranks {
		ranks[index] = map[int]bool{}
	}
	plans, maxRating, minPrice := 0, 0.0, math.MaxFloat64
	for index, hotel := range data.Hotels {
		if hotel.ID != strconv.Itoa(index+1) || ids[hotel.ID] {
			t.Fatalf("hotel %d has ID %q", index, hotel.ID)
		}
		ids[hotel.ID] = true
		city := data.Cities[hotel.City]
		dy := (hotel.Lat - city.Lat) * kmPerDegreeLat
		dx := (hotel.Lon - city.Lon) * city.KmPerDegreeLon
		if limit := 2 * math.Sqrt(3) * outskirtsSpread * city.SpreadKm * math.Sqrt2; math.Hypot(dx, dy) > limit {
			t.Fatalf("hotel %s is %.1f km from %s centre", hotel.ID, math.Hypot(dx, dy), city.Name)
		}
		key := [2]float64{hotel.Lat, hotel.Lon}
		if locations[key] {
			t.Fatalf("hotel %s shares its location", hotel.ID)
		}
		locations[key] = true
		if hotel.Lat != math.Round(hotel.Lat*1e6)/1e6 || hotel.Lon != math.Round(hotel.Lon*1e6)/1e6 {
			t.Fatalf("hotel %s coordinates are not rounded to 6 decimals", hotel.ID)
		}
		if hotel.Name == "" || hotel.Phone == "" || len(hotel.Description) < 80 || hotel.Address.StreetName == "" ||
			hotel.Address.City != city.Name || hotel.Address.Country != city.Country {
			t.Fatalf("hotel %s profile is incomplete: %+v", hotel.ID, hotel)
		}
		if hotel.Stars < 1 || hotel.Stars > 5 || hotel.Rating < 1 || hotel.Rating > 5 ||
			hotel.Capacity < 10 || hotel.Capacity > 800 {
			t.Fatalf("hotel %s attributes out of range: %+v", hotel.ID, hotel)
		}
		if hotel.Popularity < 1 || hotel.Popularity > len(city.Hotels) || ranks[hotel.City][hotel.Popularity] ||
			city.Hotels[hotel.Popularity-1] != index {
			t.Fatalf("hotel %s popularity rank %d is not a permutation slot", hotel.ID, hotel.Popularity)
		}
		ranks[hotel.City][hotel.Popularity] = true
		if len(hotel.RatePlans) < 1 || len(hotel.RatePlans) > 5 || hotel.RatePlans[0].Code != "RACK" {
			t.Fatalf("hotel %s has rate plans %+v", hotel.ID, hotel.RatePlans)
		}
		codes := map[string]bool{}
		cheapest := math.MaxFloat64
		for _, plan := range hotel.RatePlans {
			if codes[plan.Code] || !window[plan.InDate] || !window[plan.OutDate] || plan.InDate >= plan.OutDate {
				t.Fatalf("hotel %s plan %+v", hotel.ID, plan)
			}
			codes[plan.Code] = true
			if plan.TotalRate <= 0 || plan.BookableRate != plan.TotalRate || plan.TotalRateInclusive < plan.TotalRate {
				t.Fatalf("hotel %s plan prices %+v", hotel.ID, plan)
			}
			cheapest = math.Min(cheapest, plan.TotalRate)
		}
		if hotel.Price != cheapest {
			t.Fatalf("hotel %s price %v, cheapest plan %v", hotel.ID, hotel.Price, cheapest)
		}
		plans += len(hotel.RatePlans)
		maxRating = math.Max(maxRating, hotel.Rating)
		minPrice = math.Min(minPrice, hotel.Price)
	}
	if mean := float64(plans) / HotelCount; mean < 2.3 || mean > 3.0 {
		t.Fatalf("mean rate plans per hotel %.2f", mean)
	}
	top, bottom := 0, 0
	for _, hotel := range data.Hotels {
		if hotel.Rating == maxRating {
			top++
		}
		if hotel.Price == minPrice {
			bottom++
		}
	}
	if top < 1 || top > 10 || bottom < 1 || bottom > 10 {
		t.Fatalf("best-rated tie group %d, cheapest tie group %d: recommendation answers must stay small", top, bottom)
	}
	t.Logf("%d rate plans; best rating %.2f (%d hotels); lowest price %.2f (%d hotels)", plans, maxRating, top, minPrice, bottom)

	users := map[string]bool{}
	for _, user := range data.Users {
		if users[user.Username] || user.Username == "" || len(user.Password) < 10 {
			t.Fatalf("user %+v", user)
		}
		users[user.Username] = true
	}
}

func TestZipf(t *testing.T) {
	zipf := NewZipf(20, ZipfOne)
	counts := make([]int, 20)
	const samples = 200000
	for n := uint64(0); n < samples; n++ {
		counts[zipf.Rank(Unit(n, 7))]++
	}
	weights := ZipfWeights(20, ZipfOne)
	for rank, count := range counts {
		if share := float64(count) / samples; math.Abs(share-weights[rank]) > 0.005 {
			t.Fatalf("rank %d share %.4f, want %.4f", rank, share, weights[rank])
		}
	}
	if zipf.Rank(0) != 0 || zipf.Rank(math.Nextafter(1, 0)) != 19 {
		t.Fatal("Rank does not cover [0, 1)")
	}
	for _, exponent := range []ZipfExponent{ZipfHalf, ZipfThreeQuarters, ZipfOne} {
		weights := ZipfWeights(1000, exponent)
		for rank := 1; rank < len(weights); rank++ {
			if weights[rank] >= weights[rank-1] {
				t.Fatalf("exponent %d weights do not fall at rank %d", exponent, rank)
			}
		}
	}
	if got := ZipfWeights(2, ZipfThreeQuarters); math.Abs(got[1]/got[0]-math.Pow(2, -0.75)) > 1e-15 {
		t.Fatalf("s=0.75 weight ratio %v", got[1]/got[0])
	}
}

func TestCosDegrees(t *testing.T) {
	for degrees := -60.0; degrees <= 60; degrees += 0.5 {
		if math.Abs(cosDegrees(degrees)-math.Cos(degrees*math.Pi/180)) > 1e-14 {
			t.Fatalf("cosDegrees(%v) = %v", degrees, cosDegrees(degrees))
		}
	}
}
