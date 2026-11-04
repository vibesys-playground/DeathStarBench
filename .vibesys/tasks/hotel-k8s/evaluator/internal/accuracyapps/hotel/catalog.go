package hotel

import (
	"fmt"
	"strconv"
	"sync"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
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

// seedProfiles builds the catalog from the task's dataset, which the seed
// Job writes into every database before the services start.
func seedProfiles() (map[string]profile, error) {
	data := hotelsupport.DefaultReference().Data
	catalog := make(map[string]profile, len(data.Hotels))
	for _, hotel := range data.Hotels {
		catalog[hotel.ID] = profile{
			id: hotel.ID, name: hotel.Name, phone: hotel.Phone,
			lat: float32(hotel.Lat), lon: float32(hotel.Lon),
			recommendLat: hotel.Lat, recommendLon: hotel.Lon,
		}
	}
	if len(catalog) != len(data.Hotels) || len(catalog) == 0 {
		return nil, fmt.Errorf("Hotel seed catalog has %d profiles for %d hotels", len(catalog), len(data.Hotels))
	}
	return catalog, nil
}

var (
	searchableOnce sync.Once
	searchable     []string
)

// sortedRateBackedIDs returns, in ID order, the hotels with rate plans whose
// own location gives an unambiguous search that contains them: the hotels a
// check can fill and then observe through search at their coordinates.
func sortedRateBackedIDs() []string {
	searchableOnce.Do(func() {
		reference := hotelsupport.DefaultReference()
		for _, hotel := range reference.Data.Hotels {
			if len(hotel.RatePlans) == 0 {
				continue
			}
			lat := strconv.FormatFloat(hotel.Lat, 'f', -1, 64)
			lon := strconv.FormatFloat(hotel.Lon, 'f', -1, 64)
			ids, exact, err := reference.SearchCandidates(lat, lon)
			if err != nil || !exact {
				continue
			}
			for _, id := range ids {
				if id == hotel.ID {
					searchable = append(searchable, hotel.ID)
					break
				}
			}
		}
	})
	return append([]string(nil), searchable...)
}
