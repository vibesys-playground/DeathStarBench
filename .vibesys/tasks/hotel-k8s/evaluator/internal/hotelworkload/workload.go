// Package hotelworkload is the hotel-k8s benchmark's workload on the vibesys-evaluator Go SDK:
// the operations of the mix with their weights and how each request is drawn. cmd/generator runs
// it as the generator that loadgen reads the request stream from (bench.py names it). A benchmark
// only measures: whether the answers are right is the accuracy check's (accuracy.py, with its own
// workload in internal/hotelaccuracy and the oracle in internal/hotelspec).
//
// Request distributions (each draw from the operation's own stream): locations by city
// popularity (Zipf, s = 1) with normal offsets of the city's hotel spread, written with 5
// decimals; stays of 1-7 nights (skewed short) in the 2027-2028 rate window; users by Zipf rank
// (s = 0.75); hotels for rates, reservations and writes by popularity (a city by Zipf rank, then
// a hotel by Zipf rank, s = 0.75, within it). About 1% of the mix are admin writes.
//
// Every phase builds its model afresh (the SDK keeps nothing between phases): the reservation
// nights and the added hotels' numbers of a phase are its own block (BeginPhase), so no phase
// reserves a slot or adds a hotel an earlier one did.
package hotelworkload

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/workload"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/hotel-k8s-evaluator/internal/hotelref"
)

const (
	// A sample books far-future nights only, outside the rate window that searches use: a
	// block of reservationNights from a night drawn per sample, at most reservationsPerNight
	// distinct hotels per night, so every reservation finds its hotel's whole capacity free
	// (the application cannot delete reservations). Phase p starts at night p*phaseNights.
	reservationNights    = 512 * 16
	reservationsPerNight = 128
	maxPhases            = 16
	phaseNights          = reservationNights / maxPhases
	additionsPerPhase    = 1 << 20     // added hotel n of phase p is p*additionsPerPhase + n
	reservationRange     = 146097 * 17 // days of seventeen 400-year cycles: four-digit years
	reservationAttempts  = 16          // popularity draws of a hotel free that night, then the city's next
	writeAttempts        = 16          // popularity draws of a capacity-managed hotel, then the city's next
	sandboxShare         = 0.03        // searches and distance recommendations near an addition
	sandboxJitterKm      = 0.5
	searchAttempts       = 8 // redraws of a location where geo's own ranking ties
	customerMask         = 1<<63 - 1
)

var (
	firstReservationNight = time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)
	// stayNights is the relative weight of a stay of n+1 nights.
	stayNights = []float64{30, 24, 16, 11, 8, 6, 5}
)

// Model is what a phase draws its requests from: the seeded data, its reference answers, and
// the reservation slots and added hotels this phase has used so far.
type Model struct {
	data *dataset.Dataset
	ref  *hotelref.Reference
	// managed[i]: a quarter of the hotels, fixed by index, whose capacity set_capacity changes
	// and that reservations never book.
	managed []bool

	added      []*dataset.Hotel // this phase's additions, in order
	firstNight time.Time
	nextSlot   int
	used       map[int]map[int]bool // night -> hotel indices reserved on it
	// capacitySet: the hotels this phase has written a capacity to. A hotel takes at most one
	// capacity write a phase, so no two of them overlap: a probe that follows its write could
	// otherwise see a concurrent, larger capacity, take its rooms, and make later writes 409.
	capacitySet  map[int]bool
	nextAddition int
	cell         uint64 // the sandbox cell of this sample's additions
	tag          uint64 // added hotels are a<tag>-<n>
	ratingCap    float64
	priceFloor   float64
}

// NewModel is the model of a sample: rand is the sample's model stream.
func NewModel(data *dataset.Dataset, rand *workload.Rand) (*Model, error) {
	ref := hotelref.NewReference(data)
	m := &Model{
		data: data, ref: ref,
		managed:      make([]bool, data.HotelCount()),
		used:         map[int]map[int]bool{},
		capacitySet:  map[int]bool{},
		nextAddition: 1,
		// An addition's rating stays below the best and its price above the cheapest, so the
		// "rate" and "price" recommendations never change.
		ratingCap:  dataset.Round(ref.MaxRating()-0.01, 100),
		priceFloor: dataset.Round(ref.MinPrice()+0.01, 100),
	}
	for index := range m.managed {
		m.managed[index] = CapacityManaged(index)
	}
	m.firstNight = firstReservationNight.AddDate(0, 0, rand.Below(reservationRange-reservationNights))
	m.cell = uint64(rand.Below(int(dataset.SandboxCount())))
	m.tag = uint64(rand.Below(1 << 48))
	return m, nil
}

// BeginPhase moves the model to the phase's own block of reservation nights and addition numbers.
func BeginPhase(m *Model, phase int) error {
	if phase < 0 || phase >= maxPhases {
		return fmt.Errorf("phase %d: a sample has at most %d phases", phase, maxPhases)
	}
	m.nextSlot = phase * phaseNights * reservationsPerNight
	m.nextAddition = 1 + phase*additionsPerPhase
	clear(m.capacitySet)
	return nil
}

// CapacityManaged says whether set_capacity changes the hotel of this index (and reservations
// never book it): a quarter of the hotels, fixed by index.
func CapacityManaged(index int) bool {
	return workload.NewRand(uint64(index), "capacity-managed").Below(4) == 0
}

// ----------------------------------------------------------------- request distributions

func day(t time.Time) string { return t.Format(time.DateOnly) }

func coordinate(value float64) string { return strconv.FormatFloat(value, 'f', 5, 64) }

// number is a price as Go's strconv.FormatFloat(v, 'f', -1, 64) writes it (the admin routes
// parse it so).
func number(value float64) string { return strconv.FormatFloat(value, 'f', -1, 64) }

// location is a city by Zipf rank, then normal offsets of the city's hotel spread from its centre.
func (m *Model) location(rand *workload.Rand) (lat, lon string) {
	city := &m.data.Cities[rand.Zipf(len(m.data.Cities), 1)]
	latitude := city.Lat + city.SpreadKm*rand.Normal()/dataset.KmPerDegreeLat
	longitude := city.Lon + city.SpreadKm*rand.Normal()/city.KmPerDegreeLon
	return coordinate(latitude), coordinate(longitude)
}

func stay(rand *workload.Rand) (in, out string) {
	nights := 1 + rand.Weighted(stayNights)
	start := dataset.RateWindowStart.AddDate(0, 0, rand.Below(dataset.RateWindowDays-nights+1))
	return day(start), day(start.AddDate(0, 0, nights))
}

// popular is (city, rank within it): a city by Zipf rank (s = 1), then a hotel by Zipf rank
// (s = 0.75).
func (m *Model) popular(rand *workload.Rand) (city, rank int) {
	city = rand.Zipf(len(m.data.Cities), 1)
	return city, rand.Zipf(len(m.data.Cities[city].Hotels), 0.75)
}

func (m *Model) popularHotel(rand *workload.Rand) *dataset.Hotel {
	city, rank := m.popular(rand)
	return m.data.Hotel(m.data.Cities[city].Hotels[rank])
}

// pickHotel is a popular hotel that eligible accepts: attempts draws, then the first draw's
// city's next. It returns the hotel's index.
func (m *Model) pickHotel(rand *workload.Rand, attempts int, eligible func(int) bool) int {
	for attempt := range attempts {
		city, rank := m.popular(rand.Child("hotel", attempt))
		if index := m.data.Cities[city].Hotels[rank]; eligible(index) {
			return index
		}
	}
	city, rank := m.popular(rand.Child("hotel", 0))
	hotels := m.data.Cities[city].Hotels
	for offset := 1; offset <= len(hotels); offset++ {
		if index := hotels[(rank+offset)%len(hotels)]; eligible(index) {
			return index
		}
	}
	panic(fmt.Sprintf("no eligible hotel in city %d", city))
}

func (m *Model) user(rand *workload.Rand) *dataset.User {
	return &m.data.Users[rand.Zipf(len(m.data.Users), 0.75)]
}

// sandboxLocation is, 3% of the time, a point within 0.5 km of an addition this phase sent
// earlier.
func (m *Model) sandboxLocation(rand *workload.Rand) (lat, lon string, ok bool) {
	if rand.Random() >= sandboxShare || len(m.added) == 0 {
		return "", "", false
	}
	anchor := m.added[rand.Below(len(m.added))]
	_, _, kmPerLon := dataset.Sandbox(m.cell)
	dy := (2*rand.Random() - 1) * sandboxJitterKm
	dx := (2*rand.Random() - 1) * sandboxJitterKm
	return coordinate(anchor.Lat + dy/dataset.KmPerDegreeLat), coordinate(anchor.Lon + dx/kmPerLon), true
}

// addition is the n-th hotel this sample adds, from its seed, in the sample's sandbox cell.
func (m *Model) addition(n int, seed uint64) *dataset.Hotel {
	lat, lon := dataset.SandboxPointFrom(m.cell, workload.NewRand(seed, "point"))
	added := dataset.AddedHotelFrom(dataset.AddedHotelID(m.tag, n), workload.NewRand(seed, "hotel"), lat, lon)
	added.Rating = math.Min(added.Rating, m.ratingCap)
	added.Price = math.Max(added.Price, m.priceFloor)
	return &added
}

// ------------------------------------------------------------------------ operations

var (
	query  = workload.Query[string]
	get    = workload.Get[string]
	post   = workload.Post[string]
	locale = query("locale", "en")
)

func request(steps ...workload.Step) workload.Request { return workload.Request{Steps: steps} }

// search is a stay at a location; 3% of them near an addition. A location where geo's own
// ranking ties is redrawn (searchAttempts), as when the benchmark checked the answers, so the
// request distribution is the one the objectives' baselines were measured with.
func (m *Model) search(rand *workload.Rand) workload.Request {
	nearLat, nearLon, near := m.sandboxLocation(rand.Child("sandbox"))
	in, out := stay(rand.Child("stay"))
	if near {
		return request(get("/hotels", query("inDate", in), query("outDate", out), query("lat", nearLat), query("lon", nearLon)))
	}
	for attempt := range searchAttempts {
		lat, lon := m.location(rand.Child("location", attempt))
		_, exact, err := m.ref.SearchCandidates(lat, lon)
		if err != nil {
			panic(err)
		}
		if exact {
			options := []workload.Option{query("inDate", in), query("outDate", out), query("lat", lat), query("lon", lon)}
			if rand.Random() < 0.5 {
				options = append(options, locale)
			}
			return request(get("/hotels", options...))
		}
	}
	panic(fmt.Sprintf("no unambiguous search location in %d draws", searchAttempts))
}

func recommend(require string) func(*Model, *workload.Rand) workload.Request {
	return func(m *Model, rand *workload.Rand) workload.Request {
		if require == "dis" {
			if lat, lon, near := m.sandboxLocation(rand.Child("sandbox")); near {
				return request(get("/recommendations", query("require", require), query("lat", lat), query("lon", lon)))
			}
		}
		lat, lon := m.location(rand.Child("location"))
		options := []workload.Option{query("require", require), query("lat", lat), query("lon", lon)}
		if rand.Random() < 0.5 {
			options = append(options, locale)
		}
		return request(get("/recommendations", options...))
	}
}

func login(valid bool) func(*Model, *workload.Rand) workload.Request {
	return func(m *Model, rand *workload.Rand) workload.Request {
		u := m.user(rand)
		password := u.Password
		if !valid {
			password += "-invalid"
		}
		return request(get("/user", query("username", u.Username), query("password", password)))
	}
}

// reserveCapacity reserves a hotel's whole capacity on a night of the phase's block, then one
// room more (rejected while the night is full). Each takes its own (night, hotel) slot, so no two
// reservations of a sample book the same hotel on the same night and they may run concurrently.
func (m *Model) reserveCapacity(rand *workload.Rand) workload.Request {
	night, index := m.reservationSlot(rand)
	id, capacity := dataset.HotelID(index), m.data.Capacity(index)
	u := m.user(rand.Child("user"))
	in := m.firstNight.AddDate(0, 0, night)
	common := []workload.Option{query("inDate", day(in)), query("outDate", day(in.AddDate(0, 0, 1))),
		query("hotelId", id), query("customerName", fmt.Sprintf("vibesys-%016x", rand.Uint64()&customerMask)),
		query("username", u.Username), query("password", u.Password)}
	with := func(rooms string) []workload.Option {
		return append(append([]workload.Option(nil), common...), query("number", rooms))
	}
	return request(get("/reservation", with(strconv.Itoa(capacity))...), get("/reservation", with("1")...))
}

// reservationSlot takes the next slot: its night (from the sample's first) and a popular hotel
// not reserved on that night and not capacity-managed.
func (m *Model) reservationSlot(rand *workload.Rand) (night, index int) {
	night = m.nextSlot / reservationsPerNight
	if night >= reservationNights {
		panic(fmt.Sprintf("the sample used its %d reservation slots", reservationNights*reservationsPerNight))
	}
	if m.used[night] == nil {
		m.used[night] = map[int]bool{}
	}
	used := m.used[night]
	index = m.pickHotel(rand, reservationAttempts, func(i int) bool { return !used[i] && !m.managed[i] })
	m.nextSlot++
	used[index] = true
	return night, index
}

// updateRate changes one plan of a popular hotel, its prices scaled by 0.8-1.25.
func (m *Model) updateRate(rand *workload.Rand) workload.Request {
	h := m.popularHotel(rand.Child("hotel"))
	plan := &h.RatePlans[rand.Below(len(h.RatePlans))]
	total := dataset.Round(plan.TotalRate*(0.8+float64(0.45*rand.Random())), 100)
	prices := [3]float64{total, total, dataset.Round(total*plan.TotalRateInclusive/plan.TotalRate, 100)}
	return request(post("/admin/rate", query("hotelId", h.ID), query("code", plan.Code),
		query("bookableRate", number(prices[0])), query("totalRate", number(prices[1])),
		query("totalRateInclusive", number(prices[2]))))
}

// updateProfile writes a new phone in the city's format; in 10% of the updates a new name too.
func (m *Model) updateProfile(rand *workload.Rand) workload.Request {
	h := m.popularHotel(rand.Child("hotel"))
	phone := dataset.NewPhoneFrom(m.data, h, rand.Child("phone"))
	options := []workload.Option{query("hotelId", h.ID), query("phone", phone)}
	if rand.Random() < 0.1 {
		options = append(options, query("name", dataset.NewNameFrom(m.data, h, rand.Child("name"))))
	}
	return request(post("/admin/profile", options...))
}

// setCapacity sets a capacity-managed hotel's rooms to 0.5-2x the seeded ones, and above the
// most rooms its seeded bookings hold on a night (so the write is accepted and every night
// keeps a room), then reserves one room more: rejected at once.
func (m *Model) setCapacity(rand *workload.Rand) workload.Request {
	index, rooms := m.capacityWrite(rand)
	id := dataset.HotelID(index)
	u := m.user(rand.Child("user"))
	probe := []workload.Option{query("inDate", day(m.firstNight)), query("outDate", day(m.firstNight.AddDate(0, 0, 1))),
		query("hotelId", id), query("customerName", fmt.Sprintf("vibesys-capacity-%016x", rand.Uint64()&customerMask)),
		query("username", u.Username), query("password", u.Password), query("number", strconv.Itoa(rooms+1))}
	return request(post("/admin/capacity", query("hotelId", id), query("rooms", strconv.Itoa(rooms))), get("/reservation", probe...))
}

// capacityWrite is a capacity-managed hotel this phase has not written yet, and its new rooms.
func (m *Model) capacityWrite(rand *workload.Rand) (index, rooms int) {
	index = m.pickHotel(rand, writeAttempts, func(i int) bool { return m.managed[i] && !m.capacitySet[i] })
	m.capacitySet[index] = true
	capacity := m.data.Capacity(index)
	low, high := max(1, capacity/2, m.ref.SeededMaxRooms(dataset.HotelID(index))+1), 2*capacity
	return index, low + int(rand.Random()*float64(high-low+1))
}

// The admin route's body of an added hotel, its fields in the route's order.
type (
	addedAddress struct {
		StreetNumber string `json:"streetNumber"`
		StreetName   string `json:"streetName"`
		City         string `json:"city"`
		State        string `json:"state"`
		Country      string `json:"country"`
		PostalCode   string `json:"postalCode"`
	}
	addedRoom struct {
		Code               string  `json:"code"`
		RoomDescription    string  `json:"roomDescription"`
		BookableRate       float64 `json:"bookableRate"`
		TotalRate          float64 `json:"totalRate"`
		TotalRateInclusive float64 `json:"totalRateInclusive"`
	}
	addedPlan struct {
		Code     string    `json:"code"`
		InDate   string    `json:"inDate"`
		OutDate  string    `json:"outDate"`
		RoomType addedRoom `json:"roomType"`
	}
	addedHotel struct {
		HotelID     string       `json:"hotelId"`
		Name        string       `json:"name"`
		PhoneNumber string       `json:"phoneNumber"`
		Description string       `json:"description"`
		Address     addedAddress `json:"address"`
		Lat         float64      `json:"lat"`
		Lon         float64      `json:"lon"`
		Rating      float64      `json:"rating"`
		Price       float64      `json:"price"`
		Rooms       int          `json:"rooms"`
		RatePlans   []addedPlan  `json:"ratePlans"`
	}
)

func (m *Model) addHotel(rand *workload.Rand) workload.Request {
	n, seed := m.nextAddition, uint64(rand.Below(1<<62))
	m.nextAddition++
	h := m.addition(n, seed)
	m.added = append(m.added, h)
	a := h.Address
	body := addedHotel{
		HotelID: h.ID, Name: h.Name, PhoneNumber: h.Phone, Description: h.Description,
		Address: addedAddress{StreetNumber: a.StreetNumber, StreetName: a.StreetName, City: a.City,
			State: a.State, Country: a.Country, PostalCode: a.PostalCode},
		Lat: h.Lat, Lon: h.Lon, Rating: h.Rating, Price: h.Price, Rooms: h.Capacity,
	}
	for _, p := range h.RatePlans {
		body.RatePlans = append(body.RatePlans, addedPlan{Code: p.Code, InDate: p.InDate, OutDate: p.OutDate,
			RoomType: addedRoom{Code: p.RoomCode, RoomDescription: p.RoomDescription, BookableRate: p.BookableRate,
				TotalRate: p.TotalRate, TotalRateInclusive: p.TotalRateInclusive}})
	}
	return request(post("/admin/hotel", workload.JSON(body)))
}

func (m *Model) rates(rand *workload.Rand) workload.Request {
	return request(get("/rates", query("hotelId", m.popularHotel(rand).ID)))
}

// ------------------------------------------------------------------------------ workload

// Workload is the benchmark's mix: the weights of the task's earlier closed.toml and open.toml.
func Workload() workload.Workload[*dataset.Dataset, *Model] {
	return workload.Workload[*dataset.Dataset, *Model]{
		Dataset:    dataset.Read,
		Model:      NewModel,
		BeginPhase: BeginPhase,
		Operations: []workload.Operation[*Model]{
			{Name: "search_hotels", Weight: 6000, Build: (*Model).search},
			{Name: "recommend_distance", Weight: 1300, Build: recommend("dis")},
			{Name: "recommend_rate", Weight: 1300, Build: recommend("rate")},
			{Name: "recommend_price", Weight: 1300, Build: recommend("price")},
			{Name: "login_valid", Weight: 30, Build: login(true)},
			{Name: "login_invalid", Weight: 20, Build: login(false)},
			{Name: "reserve_capacity", Weight: 50, Build: (*Model).reserveCapacity},
			{Name: "hotel_rates", Weight: 400, Build: (*Model).rates},
			{Name: "update_rate", Weight: 42, Build: (*Model).updateRate},
			{Name: "update_profile", Weight: 42, Build: (*Model).updateProfile},
			{Name: "set_capacity", Weight: 12, Build: (*Model).setCapacity},
			{Name: "add_hotel", Weight: 8, Build: (*Model).addHotel},
		},
	}
}
