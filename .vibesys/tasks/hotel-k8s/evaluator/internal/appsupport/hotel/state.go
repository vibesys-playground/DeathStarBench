package hotel

import (
	"fmt"
	"sort"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// Profile is the part of a hotel's profile that writes change and the
// frontend returns (GeoJSON properties).
type Profile struct{ Name, Phone string }

// Prices are the amounts of one rate plan that a rate write sets.
type Prices struct{ Bookable, Total, Inclusive float64 }

// PlanKey identifies one rate plan.
type PlanKey struct{ HotelID, Code string }

// State is the task's data after a sequence of acknowledged writes: the
// dataset with changed profiles, prices and capacities, and added hotels.
// It is the model that expected answers after writes are computed from; it
// does not model reservations (callers track the rooms they reserved).
type State struct {
	ref      *Reference
	profiles map[string]Profile
	prices   map[PlanKey]Prices
	capacity map[string]int
	added    *Additions
}

// NewState is the seeded data of ref, before any write.
func NewState(ref *Reference) *State {
	return &State{
		ref: ref, profiles: map[string]Profile{}, prices: map[PlanKey]Prices{},
		capacity: map[string]int{}, added: NewAdditions(),
	}
}

// Reference is the seeded data the state starts from.
func (s *State) Reference() *Reference { return s.ref }

// Hotel returns a seeded or added hotel as it was seeded or added.
func (s *State) Hotel(id string) (*dataset.Hotel, bool) {
	if hotel, ok := s.ref.Hotel(id); ok {
		return hotel, true
	}
	return s.added.Hotel(id)
}

// Added returns the added hotels.
func (s *State) Added() *Additions { return s.added }

// Profile is a hotel's current name and phone.
func (s *State) Profile(id string) (Profile, bool) {
	if profile, ok := s.profiles[id]; ok {
		return profile, true
	}
	hotel, ok := s.Hotel(id)
	if !ok {
		return Profile{}, false
	}
	return Profile{Name: hotel.Name, Phone: hotel.Phone}, true
}

// Plans returns a hotel's rate plans with their current prices.
func (s *State) Plans(id string) []dataset.RatePlan {
	hotel, ok := s.Hotel(id)
	if !ok {
		return nil
	}
	plans := append([]dataset.RatePlan(nil), hotel.RatePlans...)
	for index := range plans {
		if prices, ok := s.prices[PlanKey{id, plans[index].Code}]; ok {
			plans[index].BookableRate, plans[index].TotalRate, plans[index].TotalRateInclusive =
				prices.Bookable, prices.Total, prices.Inclusive
		}
	}
	return plans
}

// Capacity is a hotel's current number of rooms.
func (s *State) Capacity(id string) (int, bool) {
	if rooms, ok := s.capacity[id]; ok {
		return rooms, true
	}
	hotel, ok := s.Hotel(id)
	if !ok {
		return 0, false
	}
	return hotel.Capacity, true
}

// SetProfile applies a profile write; an empty field is unchanged.
func (s *State) SetProfile(id, name, phone string) error {
	profile, ok := s.Profile(id)
	if !ok {
		return fmt.Errorf("unknown hotel %q", id)
	}
	if name != "" {
		profile.Name = name
	}
	if phone != "" {
		profile.Phone = phone
	}
	s.profiles[id] = profile
	return nil
}

// SetPrices applies a rate write.
func (s *State) SetPrices(id, code string, prices Prices) error {
	for _, plan := range s.Plans(id) {
		if plan.Code == code {
			s.prices[PlanKey{id, code}] = prices
			return nil
		}
	}
	return fmt.Errorf("hotel %q has no rate plan %q", id, code)
}

// SetCapacity applies an accepted capacity write. The caller decides
// acceptance: the application rejects a capacity below the rooms reserved
// on any night.
func (s *State) SetCapacity(id string, rooms int) error {
	if _, ok := s.Hotel(id); !ok {
		return fmt.Errorf("unknown hotel %q", id)
	}
	s.capacity[id] = rooms
	return nil
}

// Add applies a hotel addition.
func (s *State) Add(hotel dataset.Hotel) error {
	if _, taken := s.Hotel(hotel.ID); taken {
		return fmt.Errorf("hotel %q exists", hotel.ID)
	}
	s.added.add(&hotel)
	return nil
}

// SearchCandidates is Reference.SearchCandidates over the current hotels.
func (s *State) SearchCandidates(latText, lonText string) ([]string, bool, error) {
	return s.ref.SearchCandidatesWith(latText, lonText, s.added)
}

// NearestHotels, BestRated and Cheapest are the recommendations over the
// current hotels.
func (s *State) NearestHotels(lat, lon float64) []string {
	return s.ref.NearestHotelsWith(lat, lon, s.added)
}

func (s *State) BestRated() []string { return s.ref.BestRatedWith(s.added) }

func (s *State) Cheapest() []string { return s.ref.CheapestWith(s.added) }

// WrittenProfiles, WrittenPlans and WrittenCapacities list the items that
// writes changed, in a stable order.
func (s *State) WrittenProfiles() []string { return sortedKeys(s.profiles) }

func (s *State) WrittenCapacities() []string { return sortedKeys(s.capacity) }

func (s *State) WrittenPlans() []PlanKey {
	keys := make([]PlanKey, 0, len(s.prices))
	for key := range s.prices {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].HotelID != keys[j].HotelID {
			return keys[i].HotelID < keys[j].HotelID
		}
		return keys[i].Code < keys[j].Code
	})
	return keys
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
