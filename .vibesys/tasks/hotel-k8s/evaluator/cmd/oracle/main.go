// Command oracle is the hotel-k8s task's accuracy oracle on vibesys-evaluator's checker
// (go/check): it checks a captured history of the accuracy workload against the semantics in
// internal/hotelspec (OBJECTIVE.md, "Writes and their visibility").
//
//	oracle --history DIR [--out /vibesys/artifacts/accuracy] [budget flags]
//
// DIR is the history directory the accuracy capture wrote (vibesys-evaluator's spec.md,
// "Accuracy history"), with the dataset file the workload read as its dataset. The exit status
// is 0 for PASS, 1 for FAIL and 2 for UNKNOWN, with the reason on stderr.
package main

import (
	"errors"
	"sort"
	"sync"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"

	"vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/hotel-k8s-evaluator/internal/hotelspec"
)

// datasetCatalog is the task's dataset as the model's catalog: hotels are generated from the
// dataset's random streams when asked for (a few microseconds) and kept.
type datasetCatalog struct {
	ref *hotel.Reference
	mu  sync.Mutex
	got map[string]*hotelspec.Hotel
}

func newCatalog(data *dataset.Dataset) *datasetCatalog {
	return &datasetCatalog{ref: hotel.NewReference(data), got: map[string]*hotelspec.Hotel{}}
}

func (c *datasetCatalog) Hotel(id string) (*hotelspec.Hotel, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h, ok := c.got[id]; ok {
		return h, h != nil
	}
	full, ok := c.ref.Hotel(id)
	if !ok {
		c.got[id] = nil
		return nil, false
	}
	capacity, _ := c.ref.Capacity(id)
	lon, lat := hotel.EncodedCoordinates(full)
	h := &hotelspec.Hotel{
		ID: id, Capacity: capacity, SeededMaxRooms: c.ref.SeededMaxRooms(id),
		Profile: hotelspec.Profile{Name: full.Name, Phone: full.Phone}, Lon: lon, Lat: lat,
	}
	for _, p := range full.RatePlans {
		h.Plans = append(h.Plans, hotelspec.Plan{
			Code: p.Code, InDate: p.InDate, OutDate: p.OutDate, RoomCode: p.RoomCode, RoomDescription: p.RoomDescription,
			Prices: hotelspec.Prices{Bookable: p.BookableRate, Total: p.TotalRate, Inclusive: p.TotalRateInclusive},
		})
	}
	sort.Slice(h.Plans, func(i, j int) bool { return h.Plans[i].Code < h.Plans[j].Code })
	c.got[id] = h
	return h, true
}

// specs is Main's oracle: the hotel spec over the history's dataset.
func specs(h *check.History) ([]check.AnySpec, error) {
	if h.Dataset == "" {
		return nil, errors.New("the history has no dataset file")
	}
	data, err := dataset.Read(h.Dataset)
	if err != nil {
		return nil, err
	}
	return []check.AnySpec{hotelspec.Spec(newCatalog(data))}, nil
}

func main() { check.Main(specs) }
