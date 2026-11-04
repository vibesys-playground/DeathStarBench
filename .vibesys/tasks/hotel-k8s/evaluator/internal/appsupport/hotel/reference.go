package hotel

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// Reference answers what the pinned application returns for the task's
// dataset, for the read paths whose answers depend only on the seeded data:
// the geo part of search, the three recommendations, profiles, logins and
// room capacities. The benchmark adapter and the accuracy oracle both compute
// expected answers from it; availability (reservations) is theirs to model.
type Reference struct {
	Data *dataset.Dataset

	byID      map[string]int
	geo       *geoIndex
	byLat     []int // hotel indices sorted by latitude, for nearest-hotel scans
	bestRated []string
	cheapest  []string
	passwords map[string]string
}

var (
	defaultReferenceOnce sync.Once
	defaultReference     *Reference
)

// DefaultReference is the reference over dataset.Default(), built once.
func DefaultReference() *Reference {
	defaultReferenceOnce.Do(func() { defaultReference = NewReference(dataset.Default()) })
	return defaultReference
}

// NewReference indexes a dataset.
func NewReference(data *dataset.Dataset) *Reference {
	r := &Reference{
		Data:      data,
		byID:      make(map[string]int, len(data.Hotels)),
		byLat:     make([]int, len(data.Hotels)),
		passwords: make(map[string]string, len(data.Users)),
	}
	points := make([]geoPoint, 0, len(data.Hotels))
	maxRating, minPrice := 0.0, math.MaxFloat64
	for index := range data.Hotels {
		hotel := &data.Hotels[index]
		r.byID[hotel.ID] = index
		r.byLat[index] = index
		points = append(points, geoPoint{id: hotel.ID, lat: hotel.Lat, lon: hotel.Lon})
		// services/recommendation: max rate from 0.0, min price from MaxFloat64.
		if hotel.Rating > maxRating {
			maxRating = hotel.Rating
		}
		if hotel.Price < minPrice {
			minPrice = hotel.Price
		}
	}
	for index := range data.Hotels {
		if data.Hotels[index].Rating == maxRating {
			r.bestRated = append(r.bestRated, data.Hotels[index].ID)
		}
		if data.Hotels[index].Price == minPrice {
			r.cheapest = append(r.cheapest, data.Hotels[index].ID)
		}
	}
	sort.SliceStable(r.byLat, func(i, j int) bool { return data.Hotels[r.byLat[i]].Lat < data.Hotels[r.byLat[j]].Lat })
	r.geo = newGeoIndex(points)
	for _, user := range data.Users {
		r.passwords[user.Username] = user.Password
	}
	return r
}

// Hotel returns the hotel with the given ID.
func (r *Reference) Hotel(id string) (*dataset.Hotel, bool) {
	index, ok := r.byID[id]
	if !ok {
		return nil, false
	}
	return &r.Data.Hotels[index], true
}

// Capacity is the hotel's seeded room count.
func (r *Reference) Capacity(id string) (int, error) {
	hotel, ok := r.Hotel(id)
	if !ok {
		return 0, fmt.Errorf("unknown Hotel hotel ID %q", id)
	}
	return hotel.Capacity, nil
}

// HotelIDs returns every hotel ID in ID order.
func (r *Reference) HotelIDs() []string {
	ids := make([]string, len(r.Data.Hotels))
	for index := range r.Data.Hotels {
		ids[index] = r.Data.Hotels[index].ID
	}
	return ids
}

// User returns account index modulo the number of users.
func (r *Reference) User(index int) (username, password string) {
	user := r.Data.Users[((index%len(r.Data.Users))+len(r.Data.Users))%len(r.Data.Users)]
	return user.Username, user.Password
}

// ValidLogin is the user service's check: a seeded user and its password.
func (r *Reference) ValidLogin(username, password string) bool {
	want, ok := r.passwords[username]
	return ok && want == password
}

// ParseSearchCoordinate parses a search coordinate as the frontend does:
// strconv.ParseFloat(text, 32), then float32, widened to float64 by geo.
func ParseSearchCoordinate(text string) (float64, error) {
	value, err := strconv.ParseFloat(text, 32)
	if err != nil {
		return 0, err
	}
	return float64(float32(value)), nil
}

// SearchCandidates returns the hotels search can answer for a location
// before availability: geo's nearest hotels that have rate plans. exact is
// false when the geo service's own ordering is ambiguous for this location.
func (r *Reference) SearchCandidates(latText, lonText string) (ids []string, exact bool, err error) {
	lat, err := ParseSearchCoordinate(latText)
	if err != nil {
		return nil, false, fmt.Errorf("parse search latitude %q: %w", latText, err)
	}
	lon, err := ParseSearchCoordinate(lonText)
	if err != nil {
		return nil, false, fmt.Errorf("parse search longitude %q: %w", lonText, err)
	}
	nearby, exact := r.geo.kNearest(lat, lon)
	ids = nearby[:0:0]
	for _, id := range nearby {
		if hotel, ok := r.Hotel(id); ok && len(hotel.RatePlans) > 0 {
			ids = append(ids, id)
		}
	}
	return ids, exact, nil
}

// NearestHotels is the "dis" recommendation: every hotel at the minimum of
// geo distance / 1000 from the location (parsed as float64), over all hotels.
func (r *Reference) NearestHotels(lat, lon float64) []string {
	query := geoPoint{lat: lat, lon: lon}
	hotels := r.Data.Hotels
	start := sort.Search(len(r.byLat), func(i int) bool { return hotels[r.byLat[i]].Lat >= lat })
	best := math.MaxFloat64
	var ids []string
	visit := func(index int) {
		hotel := &hotels[index]
		distance := geoDistance(query, geoPoint{lat: hotel.Lat, lon: hotel.Lon}) / 1000
		switch {
		case distance < best:
			best, ids = distance, append(ids[:0], hotel.ID)
		case distance == best:
			ids = append(ids, hotel.ID)
		}
	}
	// A great-circle distance is at least the latitude difference's arc, so
	// the scan stops once a band's latitude alone is farther than the best.
	beyond := func(index int) bool {
		bound := geoEarthRadius * geoToRadians(math.Abs(hotels[index].Lat-lat)) / 1000
		return bound > best*(1+1e-9)+1e-9
	}
	for low, high := start-1, start; low >= 0 || high < len(r.byLat); {
		progressed := false
		if high < len(r.byLat) && !beyond(r.byLat[high]) {
			visit(r.byLat[high])
			high++
			progressed = true
		}
		if low >= 0 && !beyond(r.byLat[low]) {
			visit(r.byLat[low])
			low--
			progressed = true
		}
		if !progressed {
			break
		}
	}
	return ids
}

// BestRated is the "rate" recommendation: the hotels with the highest rating.
func (r *Reference) BestRated() []string { return append([]string(nil), r.bestRated...) }

// Cheapest is the "price" recommendation: the hotels with the lowest price.
func (r *Reference) Cheapest() []string { return append([]string(nil), r.cheapest...) }

// EncodedCoordinates returns a hotel's GeoJSON coordinates as the frontend
// writes them (the profile's float32 values) and a JSON decoder reads them.
func EncodedCoordinates(hotel *dataset.Hotel) (lon, lat float64) {
	return encodedFloat32(float32(hotel.Lon)), encodedFloat32(float32(hotel.Lat))
}

func encodedFloat32(value float32) float64 {
	parsed, err := strconv.ParseFloat(strconv.FormatFloat(float64(value), 'g', -1, 32), 64)
	if err != nil {
		panic(err)
	}
	return parsed
}
