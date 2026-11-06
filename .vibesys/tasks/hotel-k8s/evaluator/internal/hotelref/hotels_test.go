package hotelref

import (
	"sync"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

var allHotelsCache sync.Map // *dataset.Dataset -> []dataset.Hotel

// allHotels generates every hotel of a (test-sized) dataset, once per dataset.
func allHotels(data *dataset.Dataset) []dataset.Hotel {
	if hotels, ok := allHotelsCache.Load(data); ok {
		return hotels.([]dataset.Hotel)
	}
	hotels := make([]dataset.Hotel, data.HotelCount())
	for index := range hotels {
		hotels[index] = *data.Hotel(index)
	}
	allHotelsCache.Store(data, hotels)
	return hotels
}
