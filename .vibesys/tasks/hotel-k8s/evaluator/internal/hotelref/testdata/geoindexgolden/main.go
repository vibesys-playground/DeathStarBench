//go:build ignore

// Command geoindexgolden answers the queries of a golden input file with the
// real go-geoindex library, the way the pinned geo and recommendation services
// do, and writes the golden file reference_test.go checks the emulation
// against. It builds only inside hotelReservation/, which vendors the library:
//
//	go test ./internal/hotelref -run TestNearbyMatchesGeoindexGolden -args -geoindex-input=IN.json
//	cd <repo>/hotelReservation && go run -mod=vendor <evaluator>/internal/hotelref/testdata/geoindexgolden/main.go IN.json OUT.json
//	cp OUT.json <evaluator>/internal/hotelref/testdata/geoindex_golden.json
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"

	"github.com/hailocab/go-geoindex"
)

type input struct {
	PointsDigest string      `json:"points_digest"`
	Points       []point     `json:"points"`
	Nearby       [][2]string `json:"nearby"`
	Nearest      [][2]string `json:"nearest"`
}

// point is services/geo's point.
type point struct {
	Pid  string  `json:"id"`
	Plat float64 `json:"lat"`
	Plon float64 `json:"lon"`
}

func (p *point) Lat() float64 { return p.Plat }
func (p *point) Lon() float64 { return p.Plon }
func (p *point) Id() string   { return p.Pid }

type answer struct {
	Lat string   `json:"lat"`
	Lon string   `json:"lon"`
	IDs []string `json:"ids"`
}

type golden struct {
	PointsDigest string   `json:"points_digest"`
	Nearby       []answer `json:"nearby"`
	Nearest      []answer `json:"nearest"`
}

func main() {
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var in input
	if err := json.Unmarshal(raw, &in); err != nil {
		panic(err)
	}
	// services/geo: newGeoIndex and getNearbyPoints.
	index := geoindex.NewClusteringIndex()
	for i := range in.Points {
		index.Add(&in.Points[i])
	}
	out := golden{PointsDigest: in.PointsDigest}
	for _, query := range in.Nearby {
		// services/frontend parses with ParseFloat(_, 32) into float32.
		lat, lon := parse(query[0], 32), parse(query[1], 32)
		center := &geoindex.GeoPoint{Pid: "", Plat: lat, Plon: lon}
		found := index.KNearest(center, 5, geoindex.Km(10), func(geoindex.Point) bool { return true })
		ids := []string{}
		for _, p := range found {
			ids = append(ids, p.Id())
		}
		sort.Strings(ids)
		out.Nearby = append(out.Nearby, answer{query[0], query[1], ids})
	}
	for _, query := range in.Nearest {
		// services/recommendation GetRecommendations, require=dis.
		p1 := &geoindex.GeoPoint{Pid: "", Plat: parse(query[0], 64), Plon: parse(query[1], 64)}
		min := math.MaxFloat64
		for i := range in.Points {
			tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{Pid: "", Plat: in.Points[i].Plat, Plon: in.Points[i].Plon})) / 1000
			if tmp < min {
				min = tmp
			}
		}
		ids := []string{}
		for i := range in.Points {
			tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{Pid: "", Plat: in.Points[i].Plat, Plon: in.Points[i].Plon})) / 1000
			if tmp == min {
				ids = append(ids, in.Points[i].Pid)
			}
		}
		sort.Strings(ids)
		out.Nearest = append(out.Nearest, answer{query[0], query[1], ids})
	}
	encoded, err := json.MarshalIndent(out, "", " ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(os.Args[2], append(encoded, '\n'), 0o644); err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stderr, "%d nearby and %d nearest answers\n", len(out.Nearby), len(out.Nearest))
}

// parse is strconv.ParseFloat(text, bits) as the frontend calls it; with 32
// bits the frontend then converts to float32 and geo widens back to float64.
func parse(text string, bits int) float64 {
	value, err := strconv.ParseFloat(text, bits)
	if err != nil {
		panic(err)
	}
	if bits == 32 {
		return float64(float32(value))
	}
	return value
}
