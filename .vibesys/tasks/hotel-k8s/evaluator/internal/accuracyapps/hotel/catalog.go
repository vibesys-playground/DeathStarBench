package hotel

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// profile is what the frontend returns for one hotel: the profile service's
// name, phone and float32 coordinates, and the float64 location that geo and
// recommendation store.
type profile struct {
	id, name, phone string
	lat, lon        float32
	recommendLat    float64
	recommendLon    float64
}

// catalog is the seeded profiles, generated per hotel on demand from the task's dataset (the
// snapshot the data tier restores before the services start): at the task's scale a map of
// every profile would not fit the oracle's pod.
type catalog struct{ reference *hotelsupport.Reference }

// get is the profile of a seeded hotel ID, or false.
func (c catalog) get(id string) (profile, bool) {
	hotel, ok := c.reference.Hotel(id)
	if !ok {
		return profile{}, false
	}
	return profile{
		id: hotel.ID, name: hotel.Name, phone: hotel.Phone,
		lat: float32(hotel.Lat), lon: float32(hotel.Lon),
		recommendLat: hotel.Lat, recommendLon: hotel.Lon,
	}, true
}

// at is the profile of a seeded hotel ID (the zero profile for any other).
func (c catalog) at(id string) profile {
	p, _ := c.get(id)
	return p
}

// size is the number of seeded hotels.
func (c catalog) size() int { return c.reference.Data.HotelCount() }

// seedProfiles is the catalog of the task's dataset.
func seedProfiles() (catalog, error) {
	c := catalog{reference: hotelsupport.DefaultReference()}
	if c.size() == 0 {
		return catalog{}, fmt.Errorf("Hotel seed catalog is empty")
	}
	return c, nil
}

// searchableSample is how many hotels sortedRateBackedIDs tries.
const searchableSample = 20_000

var (
	searchableOnce sync.Once
	searchable     []string
)

// sortedRateBackedIDs returns, in ID order, a spread sample of the hotels with rate plans whose
// own location gives an unambiguous search that contains them: the hotels a
// check can fill and then observe through search at their coordinates.
func sortedRateBackedIDs() []string {
	searchableOnce.Do(func() {
		reference := hotelsupport.DefaultReference()
		data := reference.Data
		// A spread sample of the hotels is enough: the checks use a few hundred.
		step := max(1, data.HotelCount()/searchableSample)
		for index := 0; index < data.HotelCount(); index += step {
			if data.PlanCount(index) == 0 {
				continue
			}
			hotelLat, hotelLon := data.Location(index)
			lat := strconv.FormatFloat(hotelLat, 'f', -1, 64)
			lon := strconv.FormatFloat(hotelLon, 'f', -1, 64)
			ids, exact, err := reference.SearchCandidates(lat, lon)
			if err != nil || !exact {
				continue
			}
			for _, id := range ids {
				if id == dataset.HotelID(index) {
					searchable = append(searchable, id)
					break
				}
			}
		}
		sort.Strings(searchable)
	})
	return append([]string(nil), searchable...)
}
