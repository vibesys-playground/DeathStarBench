package hotelref

import (
	"math"
	"sort"
)

// This file reproduces, operation for operation, what the pinned geo service
// computes: github.com/hailocab/go-geoindex v0.0.0-20160127134810-64631bfe9711
// (vendored in hotelReservation/) as services/geo/server.go uses it, a
// ClusteringIndex whose KNearest is a PointsIndex with 0.5 km cells, asked for
// the 5 nearest points within 10 km. The search is approximate (it stops
// widening the ring of cells once it holds more than k points beyond the
// query's cell, ranks by an equirectangular distance and cuts at the first
// ranked point beyond 10 km by haversine), so an exact nearest-neighbour
// search would disagree with the service. Floating-point expressions keep the
// library's operand order; testdata/geoindex_golden.json pins the results of
// the real library.
const (
	geoResolution     = 500.0    // NewPointsIndex(Km(0.5))
	geoLatDegree      = 111000.0 // latDegreeLength = Km(111.0)
	geoLonDegree      = 85000.0  // lonDegreeLength = Km(85.0)
	geoEarthRadius    = 6371000.0
	geoMaxDistance    = 10000.0 // maxSearchRadius = 10 (km)
	geoMaxResults     = 5       // maxSearchResults
	geoCoarseDistance = 20000.0 // math.Max(maxDistance*2, resolution*2+0.01)
)

type geoCell struct{ x, y int }

type geoPoint struct {
	id       string
	lat, lon float64
}

type geoIndex struct{ cells map[geoCell][]geoPoint }

func newGeoIndex(points []geoPoint) *geoIndex {
	index := &geoIndex{cells: make(map[geoCell][]geoPoint)}
	for _, point := range points {
		cell := geoCellOf(point.lat, point.lon)
		index.cells[cell] = append(index.cells[cell], point)
	}
	return index
}

func geoCellOf(lat, lon float64) geoCell {
	minLat, minLon := -90.0, -180.0
	return geoCell{
		x: int((-minLat + lat) * geoLatDegree / geoResolution),
		y: int((-minLon + lon) * geoLonDegree / geoResolution),
	}
}

// appendCells appends the points of the cells in the rectangle, from g and
// then from extra (points added to the index after g; nil for none).
func (g *geoIndex) appendCells(points []geoPoint, extra *geoIndex, minX, maxX, minY, maxY int) []geoPoint {
	for x := minX; x <= maxX; x++ {
		for y := minY; y <= maxY; y++ {
			points = append(points, g.cells[geoCell{x, y}]...)
			if extra != nil {
				points = append(points, extra.cells[geoCell{x, y}]...)
			}
		}
	}
	return points
}

// kNearest returns the IDs the geo service answers for a query point (the
// service receives float32 coordinates and widens them to float64). exact is
// false when two candidates tie on the ranking distance among the first k+1:
// the library's unstable sort may then order them either way, so the set is
// not determined.
//
// extra holds points added after g was built (the index is the union); the
// geo service adds a point to its cell like a loaded one, and only the order
// within a cell, which matters only for ties, can differ.
func (g *geoIndex) kNearest(lat, lon float64, extra *geoIndex) (ids []string, exact bool) {
	k := geoMaxResults
	query := geoPoint{lat: lat, lon: lon}
	center := geoCellOf(lat, lon)
	nearby := g.appendCells(nil, extra, center.x, center.x, center.y, center.y)
	totalCount := 0
	for d := 1; float64(d)*geoResolution <= geoCoarseDistance; d++ {
		oldCount := len(nearby)
		nearby = g.appendCells(nearby, extra, center.x-d, center.x+d, center.y+d, center.y+d)
		nearby = g.appendCells(nearby, extra, center.x-d, center.x+d, center.y-d, center.y-d)
		nearby = g.appendCells(nearby, extra, center.x-d, center.x-d, center.y-d+1, center.y+d-1)
		nearby = g.appendCells(nearby, extra, center.x+d, center.x+d, center.y-d+1, center.y+d-1)
		totalCount += len(nearby) - oldCount
		if totalCount > k {
			break
		}
	}
	ranks := make([]float64, len(nearby))
	for index, point := range nearby {
		ranks[index] = geoApproximateSquareDistance(point, query)
	}
	order := make([]int, len(nearby))
	for index := range order {
		order[index] = index
	}
	sort.Slice(order, func(i, j int) bool { return ranks[order[i]] < ranks[order[j]] })
	exact = true
	for index := 1; index < len(order) && index <= k; index++ {
		if ranks[order[index]] == ranks[order[index-1]] {
			exact = false
		}
	}
	k = min(k, len(order))
	for index, candidate := range order {
		if geoDistance(query, nearby[candidate]) > geoMaxDistance || index == k {
			k = index
			break
		}
	}
	ids = make([]string, 0, k)
	for _, candidate := range order[:k] {
		ids = append(ids, nearby[candidate].id)
	}
	return ids, exact
}

func geoToRadians(x float64) float64 { return x * math.Pi / 180.0 }

// geoDistance is go-geoindex's haversine distance(p1, p2) in meters; the
// recommendation service divides it by 1000.
func geoDistance(p1, p2 geoPoint) float64 {
	dLat := geoToRadians(p2.lat - p1.lat)
	dLng := geoToRadians(p2.lon - p1.lon)
	sindLat := math.Sin(dLat / 2)
	sindLng := math.Sin(dLng / 2)
	a := math.Pow(sindLat, 2) + math.Pow(sindLng, 2)*math.Cos(geoToRadians(p1.lat))*math.Cos(geoToRadians(p2.lat))
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return geoEarthRadius * c
}

// geoLonLength is lonDegreeDistance.get: the length of one degree of
// longitude at the latitude truncated to a tenth of a degree. Like the
// library's own cache, it computes each tenth once (geoLonLengths).
func geoLonLength(lat float64) float64 {
	latIndex := int(lat * 10)
	if offset := latIndex + geoLonLengthOffset; offset >= 0 && offset < len(geoLonLengths) {
		return geoLonLengths[offset]
	}
	return geoLonLengthAt(latIndex)
}

func geoLonLengthAt(latIndex int) float64 {
	latRounded := float64(latIndex) / 10
	return geoDistance(geoPoint{lat: latRounded, lon: 0.0}, geoPoint{lat: latRounded, lon: 1.0})
}

// geoLonLengths[i] is geoLonLengthAt(i - geoLonLengthOffset), for every tenth
// of a degree of latitude.
const geoLonLengthOffset = 900

var geoLonLengths = func() []float64 {
	lengths := make([]float64, 2*geoLonLengthOffset+1)
	for offset := range lengths {
		lengths[offset] = geoLonLengthAt(offset - geoLonLengthOffset)
	}
	return lengths
}()

func geoApproximateSquareDistance(p1, p2 geoPoint) float64 {
	avgLat := (p1.lat + p2.lat) / 2.0
	latLen := math.Abs(p1.lat-p2.lat) * geoLatDegree
	lonLen := math.Abs(p1.lon-p2.lon) * geoLonLength(avgLat)
	return latLen*latLen + lonLen*lonLen
}
