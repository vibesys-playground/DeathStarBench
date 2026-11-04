package hotel

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

var geoindexInput = flag.String("geoindex-input", "", "write the go-geoindex golden input file here (see testdata/geoindexgolden)")

const goldenPath = "testdata/geoindex_golden.json"

type goldenAnswer struct {
	Lat string   `json:"lat"`
	Lon string   `json:"lon"`
	IDs []string `json:"ids"`
}

type geoindexGolden struct {
	PointsDigest string         `json:"points_digest"`
	Nearby       []goldenAnswer `json:"nearby"`
	Nearest      []goldenAnswer `json:"nearest"`
}

type goldenPoint struct {
	ID  string  `json:"id"`
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

func geoPoints(data *dataset.Dataset) ([]goldenPoint, string) {
	points := make([]goldenPoint, len(data.Hotels))
	for index, hotel := range data.Hotels {
		points[index] = goldenPoint{hotel.ID, hotel.Lat, hotel.Lon}
	}
	encoded, err := json.Marshal(points)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(encoded)
	return points, hex.EncodeToString(sum[:])
}

// goldenQueries covers city-centre traffic, hotel locations, sparse outskirts
// and points beyond every hotel's 10 km radius.
func goldenQueries(data *dataset.Dataset) (nearby, nearest [][2]string) {
	zipf := dataset.NewZipf(len(data.Cities), dataset.ZipfOne)
	near := func(random uint64, spread float64, decimals int) [2]string {
		city := data.Cities[zipf.Rank(dataset.Unit(random, 0))]
		lat := city.Lat + spread*city.SpreadKm*dataset.Normal(random, 1)/111.32
		lon := city.Lon + spread*city.SpreadKm*dataset.Normal(random, 5)/city.KmPerDegreeLon
		return [2]string{strconv.FormatFloat(lat, 'f', decimals, 64), strconv.FormatFloat(lon, 'f', decimals, 64)}
	}
	for n := uint64(0); n < 1500; n++ {
		nearby = append(nearby, near(n, 1, 5))
	}
	for n := uint64(0); n < 300; n++ {
		hotel := data.Hotels[int(dataset.Mix(n, 9)%uint64(len(data.Hotels)))]
		nearby = append(nearby, [2]string{strconv.FormatFloat(hotel.Lat, 'f', -1, 64), strconv.FormatFloat(hotel.Lon, 'f', -1, 64)})
	}
	for n := uint64(0); n < 200; n++ {
		nearby = append(nearby, near(n+10000, 4, 5))
	}
	for n := uint64(0); n < 100; n++ {
		nearby = append(nearby, near(n+20000, 12, 4))
	}
	for n := uint64(0); n < 400; n++ {
		nearest = append(nearest, near(n+30000, 1, 5))
	}
	for n := uint64(0); n < 100; n++ {
		hotel := data.Hotels[int(dataset.Mix(n, 11)%uint64(len(data.Hotels)))]
		nearest = append(nearest, [2]string{strconv.FormatFloat(hotel.Lat, 'f', -1, 64), strconv.FormatFloat(hotel.Lon, 'f', -1, 64)})
	}
	return nearby, nearest
}

// TestNearbyMatchesGeoindexGolden checks the emulated geo search and the
// distance recommendation against answers of the real go-geoindex library
// (testdata/geoindexgolden).
func TestNearbyMatchesGeoindexGolden(t *testing.T) {
	data := dataset.Default()
	points, digest := geoPoints(data)
	nearby, nearest := goldenQueries(data)
	if *geoindexInput != "" {
		encoded, err := json.Marshal(map[string]any{
			"points_digest": digest, "points": points, "nearby": nearby, "nearest": nearest,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(*geoindexInput, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Skipf("wrote %s; now run testdata/geoindexgolden", *geoindexInput)
	}
	raw, err := os.ReadFile(filepath.FromSlash(goldenPath))
	if err != nil {
		t.Fatal(err)
	}
	var golden geoindexGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if golden.PointsDigest != digest || len(golden.Nearby) != len(nearby) || len(golden.Nearest) != len(nearest) {
		t.Fatalf("%s was made for another dataset or query set; regenerate it (testdata/geoindexgolden)", goldenPath)
	}
	reference := DefaultReference()
	empty, full := 0, 0
	for index, answer := range golden.Nearby {
		if answer.Lat != nearby[index][0] || answer.Lon != nearby[index][1] {
			t.Fatalf("golden query %d is %v, want %v", index, answer, nearby[index])
		}
		got, exact, err := reference.SearchCandidates(answer.Lat, answer.Lon)
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)
		if !exact {
			t.Fatalf("query %v ranks two candidates equally", nearby[index])
		}
		if !reflect.DeepEqual(got, answer.IDs) && !(len(got) == 0 && len(answer.IDs) == 0) {
			t.Fatalf("geo at %s,%s: emulation %v, go-geoindex %v", answer.Lat, answer.Lon, got, answer.IDs)
		}
		switch len(got) {
		case 0:
			empty++
		case geoMaxResults:
			full++
		}
	}
	t.Logf("%d geo queries match go-geoindex: %d with 5 hotels, %d with none", len(golden.Nearby), full, empty)
	for index, answer := range golden.Nearest {
		lat, _ := strconv.ParseFloat(answer.Lat, 64)
		lon, _ := strconv.ParseFloat(answer.Lon, 64)
		got := reference.NearestHotels(lat, lon)
		sort.Strings(got)
		if !reflect.DeepEqual(got, answer.IDs) {
			t.Fatalf("nearest %d at %s,%s: emulation %v, library %v", index, answer.Lat, answer.Lon, got, answer.IDs)
		}
	}
}

// bruteForceNearby is an exact search: the hotels within 10 km by haversine
// distance, nearest first, at most five.
func bruteForceNearby(data *dataset.Dataset, lat, lon float64) []string {
	type candidate struct {
		id       string
		distance float64
	}
	var candidates []candidate
	for _, hotel := range data.Hotels {
		if distance := haversineMeters(lat, lon, hotel.Lat, hotel.Lon); distance <= 10000 {
			candidates = append(candidates, candidate{hotel.ID, distance})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].distance < candidates[j].distance })
	ids := []string{}
	for index := 0; index < len(candidates) && index < 5; index++ {
		ids = append(ids, candidates[index].id)
	}
	sort.Strings(ids)
	return ids
}

func haversineMeters(lat1, lon1, lat2, lon2 float64) float64 {
	rad := math.Pi / 180
	s1 := math.Sin((lat2 - lat1) * rad / 2)
	s2 := math.Sin((lon2 - lon1) * rad / 2)
	a := s1*s1 + math.Cos(lat1*rad)*math.Cos(lat2*rad)*s2*s2
	return 2 * 6371000 * math.Asin(math.Min(1, math.Sqrt(a)))
}

// TestNearbyAgreesWithExactSearch measures how often go-geoindex's
// approximation differs from an exact nearest-five search on canonical
// traffic. The oracle must use the emulation, not the exact search.
func TestNearbyAgreesWithExactSearch(t *testing.T) {
	data := dataset.Default()
	reference := DefaultReference()
	zipf := dataset.NewZipf(len(data.Cities), dataset.ZipfOne)
	const samples = 1500
	agree, ambiguous := 0, 0
	for n := uint64(0); n < samples; n++ {
		city := data.Cities[zipf.Rank(dataset.Unit(n, 100))]
		lat := strconv.FormatFloat(city.Lat+city.SpreadKm*dataset.Normal(n, 101)/111.32, 'f', 5, 64)
		lon := strconv.FormatFloat(city.Lon+city.SpreadKm*dataset.Normal(n, 105)/city.KmPerDegreeLon, 'f', 5, 64)
		got, exact, err := reference.SearchCandidates(lat, lon)
		if err != nil {
			t.Fatal(err)
		}
		if !exact {
			ambiguous++
		}
		sort.Strings(got)
		qLat, _ := ParseSearchCoordinate(lat)
		qLon, _ := ParseSearchCoordinate(lon)
		want := bruteForceNearby(data, qLat, qLon)
		if reflect.DeepEqual(append([]string{}, got...), want) {
			agree++
		}
	}
	t.Logf("go-geoindex emulation equals exact 5-nearest in %d of %d canonical searches; %d ambiguous", agree, samples, ambiguous)
	if agree < samples*95/100 || ambiguous > 0 {
		t.Fatalf("agreement %d/%d, ambiguous %d", agree, samples, ambiguous)
	}
}

func TestNearestHotelsMatchesBruteForce(t *testing.T) {
	data := dataset.Default()
	reference := DefaultReference()
	brute := func(lat, lon float64) []string {
		query := geoPoint{lat: lat, lon: lon}
		best := math.MaxFloat64
		var ids []string
		for _, hotel := range data.Hotels {
			distance := geoDistance(query, geoPoint{lat: hotel.Lat, lon: hotel.Lon}) / 1000
			if distance < best {
				best, ids = distance, []string{hotel.ID}
			} else if distance == best {
				ids = append(ids, hotel.ID)
			}
		}
		sort.Strings(ids)
		return ids
	}
	for n := uint64(0); n < 1200; n++ {
		var lat, lon float64
		switch n % 3 {
		case 0: // anywhere on the globe
			lat, lon = -80+160*dataset.Unit(n, 1), -180+360*dataset.Unit(n, 2)
		case 1: // a hotel location
			hotel := data.Hotels[int(dataset.Mix(n, 3)%uint64(len(data.Hotels)))]
			lat, lon = hotel.Lat, hotel.Lon
		default: // near a city centre
			city := data.Cities[int(dataset.Mix(n, 4)%uint64(len(data.Cities)))]
			lat, lon = city.Lat+0.05*dataset.Normal(n, 5), city.Lon+0.05*dataset.Normal(n, 9)
		}
		got := reference.NearestHotels(lat, lon)
		sort.Strings(got)
		if want := brute(lat, lon); !reflect.DeepEqual(got, want) {
			t.Fatalf("NearestHotels(%v, %v) = %v, brute force %v", lat, lon, got, want)
		}
	}
}

func TestRecommendationsByRateAndPriceMatchBruteForce(t *testing.T) {
	data := dataset.Default()
	maxRating, minPrice := 0.0, math.MaxFloat64
	for _, hotel := range data.Hotels {
		maxRating, minPrice = math.Max(maxRating, hotel.Rating), math.Min(minPrice, hotel.Price)
	}
	var best, cheapest []string
	for _, hotel := range data.Hotels {
		if hotel.Rating == maxRating {
			best = append(best, hotel.ID)
		}
		if hotel.Price == minPrice {
			cheapest = append(cheapest, hotel.ID)
		}
	}
	reference := DefaultReference()
	if !reflect.DeepEqual(reference.BestRated(), best) || !reflect.DeepEqual(reference.Cheapest(), cheapest) {
		t.Fatalf("best rated %v (want %v), cheapest %v (want %v)", reference.BestRated(), best, reference.Cheapest(), cheapest)
	}
}

func TestSearchCoordinatesAreParsedAsFloat32(t *testing.T) {
	for _, text := range []string{"51.50740", "-0.12780", "37.7867", "1.29030", "-33.86880"} {
		got, err := ParseSearchCoordinate(text)
		if err != nil {
			t.Fatal(err)
		}
		value, _ := strconv.ParseFloat(text, 32)
		if got != float64(float32(value)) || float64(float32(got)) != got {
			t.Fatalf("ParseSearchCoordinate(%s) = %v", text, got)
		}
	}
	if _, _, err := DefaultReference().SearchCandidates("north", "0"); err == nil {
		t.Fatal("unparsable latitude accepted")
	}
}
