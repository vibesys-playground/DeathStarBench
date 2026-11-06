// Package hotelaccuracy is the hotel-k8s accuracy check's workload on the vibesys-evaluator Go
// SDK: requests that contend on a few hotels, so that the oracle (internal/hotelspec) sees
// reservations race for the last rooms, reads follow writes within the 1 s bound, and the state
// outlives the faults between phases. cmd/accuracy-generator runs it (accuracy.py names it); the
// accuracy run captures every answer and the oracle judges the history.
//
// Each request's Info is the oracle's contract (hotelspec's info.go), one HTTP step each:
//
//   - search: a stay of 1-2 nights at a hot hotel's location (geo's nearest hotels with rate
//     plans there are the candidates);
//   - reserve: one night of a contention hotel; reserve_capacity_hotel: one night of a capacity
//     hotel; rooms in chunks of a third to a sixth of the capacity (70%) or one room, 10% of them
//     with a wrong password (the pinned frontend reserves anyway);
//   - set_capacity: a capacity hotel's rooms, mostly above the fullest seeded night, 15% below it
//     (409 then);
//   - set_profile, set_rate, rates: a hot hotel's phone (20% a name too), a rate plan's prices,
//     its rates;
//   - recommend_dis at a hot hotel's location, recommend_rate and recommend_price; login, 60% with
//     the right password.
//
// The contract's two rules on the requests (README, "What the accuracy check does"):
//
//   - a reservation is one night: the base takes a stay's nights one by one and gives them back
//     when one is full, so a multi-night stay is not atomic to concurrent observers;
//   - a capacity write never overlaps a reservation of the same hotel: the base orders them only
//     when they do not overlap. The capacity hotels are two groups that swap roles every phase:
//     in phase p, group p%2 takes capacity writes and the other group reservations. Phases never
//     overlap (each ends when its last answer is in), so neither do the two.
//
// Reservations go to a window of two nights that slides every windowOps operations of the phase,
// so each night fills up and later ones are refused; 20% of the stays (from phase 1) are nights an
// earlier phase booked, read and reserved again after the fault between them.
package hotelaccuracy

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/workload"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/hotel-k8s-evaluator/internal/hotelref"
	"vibesys/hotel-k8s-evaluator/internal/hotelspec"
)

const (
	// ContentionHotels take reservations in every phase; CapacityHotels (two groups of
	// CapacityHotels/2) take capacity writes or reservations, by phase.
	ContentionHotels = 6
	CapacityHotels   = 4
	windowOps        = 768  // operations of a phase per window of two reservation nights
	phaseNights      = 1000 // each phase books its own block of nights
	earlierShare     = 0.2  // stays in an earlier phase's block (from phase 1)
	earlierWindows   = 40   // ... among its first windows
	firstNightYear   = 3000 // far beyond the rate window and the seeded bookings
	nightsRange      = 300000
)

// Model is what a phase draws from: the dataset, the hot hotels (the same in every phase of a
// sample), the phase and how many operations it has built.
type Model struct {
	data       *dataset.Dataset
	ref        *hotelref.Reference
	contention []int // hotel indices
	capacity   []int // the two groups: capacity[:CapacityHotels/2] and capacity[CapacityHotels/2:]
	hot        []int // contention, then capacity
	firstNight time.Time
	phase      int
	count      int
}

func coordinate(v float64) string { return strconv.FormatFloat(v, 'f', 5, 64) }

func day(t time.Time) string { return t.Format(time.DateOnly) }

// NewModel picks the hot hotels from the sample's model stream: hotels with rate plans whose
// own location is a search with an unambiguous answer that holds them.
func NewModel(data *dataset.Dataset, rand *workload.Rand) (*Model, error) {
	m := &Model{data: data, ref: hotelref.NewReference(data)}
	m.firstNight = time.Date(firstNightYear, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, rand.Below(nightsRange))
	used := map[int]bool{}
	pick := func() (int, error) {
		for range 100000 {
			i := rand.Below(data.HotelCount())
			if used[i] || data.PlanCount(i) == 0 {
				continue
			}
			lat, lon := data.Location(i)
			candidates, exact, err := m.ref.SearchCandidates(coordinate(lat), coordinate(lon))
			if err != nil || !exact || !slices.Contains(candidates, dataset.HotelID(i)) {
				continue
			}
			used[i] = true
			return i, nil
		}
		return 0, fmt.Errorf("no hot hotel in 100000 draws")
	}
	for range ContentionHotels + CapacityHotels {
		i, err := pick()
		if err != nil {
			return nil, err
		}
		m.hot = append(m.hot, i)
	}
	m.contention, m.capacity = m.hot[:ContentionHotels], m.hot[ContentionHotels:]
	return m, nil
}

// BeginPhase keeps the phase: its block of nights and which capacity group writes.
func BeginPhase(m *Model, phase int) error {
	m.phase, m.count = phase, 0
	return nil
}

// CapacityWriters are the capacity hotels whose capacity this phase writes; ReservedCapacity the
// ones it reserves.
func (m *Model) CapacityWriters() []int {
	half := CapacityHotels / 2
	if m.phase%2 == 0 {
		return m.capacity[:half]
	}
	return m.capacity[half:]
}

func (m *Model) ReservedCapacity() []int {
	half := CapacityHotels / 2
	if m.phase%2 == 0 {
		return m.capacity[half:]
	}
	return m.capacity[:half]
}

// night is the n-th night of phase p's block.
func (m *Model) night(p, n int) time.Time { return m.firstNight.AddDate(0, 0, p*phaseNights+n) }

// nights draws a stay of 1 to most nights: in the phase's current window or, from phase 1, an
// earlier phase's first windows.
func (m *Model) nights(rand *workload.Rand, most int) []string {
	p, base := m.phase, m.count/windowOps
	if m.phase > 0 && rand.Random() < earlierShare {
		p = rand.Below(m.phase)
		base = rand.Below(earlierWindows)
	}
	start := m.night(p, base+rand.Below(2))
	out := make([]string, 1+rand.Below(most))
	for i := range out {
		out[i] = day(start.AddDate(0, 0, i))
	}
	return out
}

func (m *Model) user(rand *workload.Rand) *dataset.User {
	return &m.data.Users[rand.Below(len(m.data.Users))]
}

var (
	query = workload.Query[string]
	get   = workload.Get[string]
	post  = workload.Post[string]
)

func stayQuery(nights []string) []workload.Option {
	in, _ := time.Parse(time.DateOnly, nights[0])
	return []workload.Option{query("inDate", nights[0]), query("outDate", day(in.AddDate(0, 0, len(nights))))}
}

// op is an operation that counts toward the phase's windows.
func op(name string, weight float64, build func(*Model, *workload.Rand) workload.Request) workload.Operation[*Model] {
	return workload.Operation[*Model]{Name: name, Weight: weight, Build: func(m *Model, rand *workload.Rand) workload.Request {
		defer func() { m.count++ }()
		return build(m, rand)
	}}
}

func (m *Model) anyHot(rand *workload.Rand) int { return m.hot[rand.Below(len(m.hot))] }

func search(m *Model, rand *workload.Rand) workload.Request {
	i := m.anyHot(rand.Child("hotel"))
	lat, lon := m.data.Location(i)
	candidates, _, err := m.ref.SearchCandidates(coordinate(lat), coordinate(lon))
	if err != nil {
		panic(err)
	}
	nights := m.nights(rand.Child("nights"), 2)
	options := append(stayQuery(nights), query("lat", coordinate(lat)), query("lon", coordinate(lon)))
	return workload.Request{Steps: []workload.Step{get("/hotels", options...)},
		Info: hotelspec.SearchInfo{Nights: nights, Candidates: candidates}}
}

// reserve books one night of one of hotels.
func reserve(hotels func(*Model) []int) func(*Model, *workload.Rand) workload.Request {
	return func(m *Model, rand *workload.Rand) workload.Request {
		list := hotels(m)
		i := list[rand.Below(len(list))]
		nights := m.nights(rand.Child("nights"), 1)
		rooms := 1
		if rand.Random() < 0.7 {
			rooms = int(math.Ceil(float64(m.data.Capacity(i)) / float64(3+rand.Below(4))))
		}
		u := m.user(rand.Child("user"))
		valid := rand.Random() >= 0.1
		password := u.Password
		if !valid {
			password += "-invalid"
		}
		id := dataset.HotelID(i)
		options := append(stayQuery(nights), query("hotelId", id),
			query("customerName", fmt.Sprintf("check-%016x", rand.Uint64()&(1<<63-1))),
			query("username", u.Username), query("password", password), query("number", strconv.Itoa(rooms)))
		return workload.Request{Steps: []workload.Step{get("/reservation", options...)},
			Info: hotelspec.ReserveInfo{Hotel: id, Nights: nights, Rooms: rooms, Valid: valid}}
	}
}

func setCapacity(m *Model, rand *workload.Rand) workload.Request {
	writers := m.CapacityWriters()
	i := writers[rand.Below(len(writers))]
	id := dataset.HotelID(i)
	c, seeded := m.data.Capacity(i), m.ref.SeededMaxRooms(id)
	rooms := seeded + 1 + rand.Below(max(1, 2*c-seeded))
	if rand.Random() < 0.15 {
		rooms = rand.Below(max(1, seeded))
	}
	return workload.Request{
		Steps: []workload.Step{post("/admin/capacity", query("hotelId", id), query("rooms", strconv.Itoa(rooms)), workload.Expect(200, 409))},
		Info:  hotelspec.CapacityInfo{Hotel: id, Rooms: rooms},
	}
}

func setProfile(m *Model, rand *workload.Rand) workload.Request {
	h := m.data.Hotel(m.anyHot(rand.Child("hotel")))
	phone := dataset.NewPhoneFrom(m.data, h, rand.Child("phone"))
	options := []workload.Option{query("hotelId", h.ID), query("phone", phone)}
	name := ""
	if rand.Random() < 0.2 {
		name = dataset.NewNameFrom(m.data, h, rand.Child("name"))
		options = append(options, query("name", name))
	}
	return workload.Request{Steps: []workload.Step{post("/admin/profile", options...)},
		Info: hotelspec.ProfileInfo{Hotel: h.ID, Name: name, Phone: phone}}
}

func number(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

func setRate(m *Model, rand *workload.Rand) workload.Request {
	h := m.data.Hotel(m.anyHot(rand.Child("hotel")))
	p := &h.RatePlans[rand.Below(len(h.RatePlans))]
	total := dataset.Round(p.TotalRate*(0.8+0.45*rand.Random()), 100)
	prices := [3]float64{total, total, dataset.Round(total*p.TotalRateInclusive/p.TotalRate, 100)}
	return workload.Request{
		Steps: []workload.Step{post("/admin/rate", query("hotelId", h.ID), query("code", p.Code),
			query("bookableRate", number(prices[0])), query("totalRate", number(prices[1])), query("totalRateInclusive", number(prices[2])))},
		Info: hotelspec.RateInfo{Hotel: h.ID, Code: p.Code, Prices: prices},
	}
}

func rates(m *Model, rand *workload.Rand) workload.Request {
	id := dataset.HotelID(m.anyHot(rand))
	return workload.Request{Steps: []workload.Step{get("/rates", query("hotelId", id))}, Info: hotelspec.HotelInfo{Hotel: id}}
}

func recommend(require string) func(*Model, *workload.Rand) workload.Request {
	return func(m *Model, rand *workload.Rand) workload.Request {
		var lat, lon float64
		var expected []string
		switch require {
		case "dis": // at a hot hotel's exact location: geo's nearest hotels there
			lat, lon = m.data.Location(m.anyHot(rand.Child("hotel")))
			expected = m.ref.NearestHotels(lat, lon)
		case "rate":
			lat, lon, expected = 1, 1, m.ref.BestRated()
		default:
			lat, lon, expected = 1, 1, m.ref.Cheapest()
		}
		return workload.Request{
			Steps: []workload.Step{get("/recommendations", query("require", require),
				query("lat", strconv.FormatFloat(lat, 'f', -1, 64)), query("lon", strconv.FormatFloat(lon, 'f', -1, 64)))},
			Info: hotelspec.RecommendInfo{Require: require, Expected: expected},
		}
	}
}

func login(m *Model, rand *workload.Rand) workload.Request {
	u := m.user(rand)
	valid := rand.Random() < 0.6
	password := u.Password
	if !valid {
		password += "-invalid"
	}
	return workload.Request{Steps: []workload.Step{get("/user", query("username", u.Username), query("password", password))},
		Info: hotelspec.LoginInfo{Valid: valid}}
}

// Workload is the accuracy check's mix.
func Workload() workload.Workload[*dataset.Dataset, *Model] {
	return workload.Workload[*dataset.Dataset, *Model]{
		Dataset:    dataset.Read,
		Model:      NewModel,
		BeginPhase: BeginPhase,
		Operations: []workload.Operation[*Model]{
			op("search", 40, search),
			op("reserve", 25, reserve(func(m *Model) []int { return m.contention })),
			op("reserve_capacity_hotel", 4, reserve((*Model).ReservedCapacity)),
			op("set_capacity", 3, setCapacity),
			op("set_profile", 5, setProfile),
			op("set_rate", 4, setRate),
			op("rates", 8, rates),
			op("recommend_dis", 5, recommend("dis")),
			op("recommend_rate", 1, recommend("rate")),
			op("recommend_price", 1, recommend("price")),
			op("login", 3, login),
		},
	}
}
