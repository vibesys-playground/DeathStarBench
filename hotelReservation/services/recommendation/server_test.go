package recommendation

import (
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/hailocab/go-geoindex"
)

// bruteForce is what GetRecommendations computed before the indexes: two scans over every
// hotel.
func bruteForce(hotels map[string]Hotel, require string, lat, lon float64) []string {
	var ids []string
	switch require {
	case "dis":
		p1 := &geoindex.GeoPoint{Plat: lat, Plon: lon}
		min := math.MaxFloat64
		for _, h := range hotels {
			min = math.Min(min, float64(geoindex.Distance(p1, &geoindex.GeoPoint{Plat: h.HLat, Plon: h.HLon}))/1000)
		}
		for _, h := range hotels {
			if float64(geoindex.Distance(p1, &geoindex.GeoPoint{Plat: h.HLat, Plon: h.HLon}))/1000 == min {
				ids = append(ids, h.HId)
			}
		}
	case "rate":
		max := 0.0
		for _, h := range hotels {
			max = math.Max(max, h.HRate)
		}
		for _, h := range hotels {
			if h.HRate == max {
				ids = append(ids, h.HId)
			}
		}
	case "price":
		min := math.MaxFloat64
		for _, h := range hotels {
			min = math.Min(min, h.HPrice)
		}
		for _, h := range hotels {
			if h.HPrice == min {
				ids = append(ids, h.HId)
			}
		}
	}
	sort.Strings(ids)
	return ids
}

func indexed(s *Server, require string, lat, lon float64) []string {
	var ids []string
	switch require {
	case "dis":
		ids = s.nearest(lat, lon)
	case "rate":
		ids = append(ids, s.bestRated...)
	case "price":
		ids = append(ids, s.cheapest...)
	}
	ids = append([]string(nil), ids...)
	sort.Strings(ids)
	return ids
}

// The indexes answer what the scans over every hotel answered, with ties (duplicate
// locations, ratings and prices) and with hotels added after the load.
func TestIndexesAnswerAsTheFullScans(t *testing.T) {
	random := rand.New(rand.NewSource(7))
	hotels := map[string]Hotel{}
	point := func() (float64, float64) {
		return 51.5 + random.NormFloat64()*0.3, -0.1 + random.NormFloat64()*0.4
	}
	for i := 0; i < 5000; i++ {
		lat, lon := point()
		if i%50 == 1 { // a duplicate location
			lat, lon = hotels["0"].HLat, hotels["0"].HLon
		}
		id := strconv.Itoa(i)
		hotels[id] = Hotel{HId: id, HLat: lat, HLon: lon, HRate: math.Round(random.Float64()*400) / 100, HPrice: math.Round(random.Float64()*30000) / 100}
	}
	s := &Server{}
	s.index(hotels)
	all := map[string]Hotel{}
	for id, h := range hotels {
		all[id] = h
	}
	check := func(round int) {
		for q := 0; q < 300; q++ {
			lat, lon := point()
			if q%30 == 0 {
				lat, lon = all["0"].HLat, all["0"].HLon
			}
			for _, require := range []string{"dis", "rate", "price"} {
				got, want := indexed(s, require, lat, lon), bruteForce(all, require, lat, lon)
				if len(got) != len(want) {
					t.Fatalf("round %d %s at %v,%v: %v, want %v", round, require, lat, lon, got, want)
				}
				for i := range got {
					if got[i] != want[i] {
						t.Fatalf("round %d %s at %v,%v: %v, want %v", round, require, lat, lon, got, want)
					}
				}
			}
		}
	}
	check(0)
	for round := 1; round <= 3; round++ {
		for i := 0; i < 20; i++ {
			lat, lon := point()
			id := "a" + strconv.Itoa(round) + "-" + strconv.Itoa(i)
			h := Hotel{HId: id, HLat: lat, HLon: lon, HRate: 4 + float64(i%3), HPrice: float64(i % 2)}
			s.add(h)
			s.add(h) // a refresh re-adds what it knows
			all[id] = h
		}
		check(round)
	}
}
