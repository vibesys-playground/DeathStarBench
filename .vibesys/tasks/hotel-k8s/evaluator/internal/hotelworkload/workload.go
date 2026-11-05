// Package hotelworkload is the hotel-k8s benchmark's workload on the vibesys-evaluator Go SDK:
// the operations of the mix with their weights, how each request is drawn, how a write changes
// the model, and how each response is checked after the phase. cmd/generator runs it as the
// generator that loadgen reads the request stream from (bench.py names it).
//
// The model is the accuracy oracle's: the dataset file dataset.py writes (internal/dataset),
// the reference answers of the pinned application over it (hotel.Reference) and the state that
// writes change (hotel.State), with the visibility rule of OBJECTIVE.md (a read may show any
// value written within the last second) kept per item in workload.Versions.
//
// Request distributions (each draw from the operation's own stream): locations by city
// popularity (Zipf, s = 1) with normal offsets of the city's hotel spread, written with 5
// decimals; stays of 1-7 nights (skewed short) in the 2027 rate window; users by Zipf rank
// (s = 0.75); hotels for rates, reservations and writes by popularity (a city by Zipf rank, then
// a hotel by Zipf rank, s = 0.75, within it). About 1% of the mix are admin writes.
//
// The stream is the one the task's earlier Python workload (bench.py) wrote, draw for draw:
// cmd/generator's stream test pins it.
package hotelworkload

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/workload"

	"vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

const (
	// StalenessBound is the visibility contract of OBJECTIVE.md.
	StalenessBound = time.Second

	// A sample books far-future nights only, outside the rate window that searches use: a
	// block of reservationNights from a night drawn per sample, at most reservationsPerNight
	// distinct hotels per night, so every reservation finds its hotel's whole capacity free
	// (the application cannot delete reservations).
	reservationNights    = 512 * 16
	reservationsPerNight = 128
	reservationRange     = 146097 * 17 // days of seventeen 400-year cycles: four-digit years
	reservationAttempts  = 16          // popularity draws of a hotel free that night, then the city's next
	writeAttempts        = 16          // popularity draws of a capacity-managed hotel, then the city's next
	sandboxShare         = 0.03        // searches and distance recommendations near an addition
	sandboxJitterKm      = 0.5
	maxOptionalAdditions = 8 // beyond it a sandbox answer is only checked for membership
	searchAttempts       = 8 // redraws of a location where geo's own ranking ties
	geoMaxResults        = 5 // the geo service's maxSearchResults
	customerMask         = 1<<63 - 1
)

var (
	firstReservationNight = time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)
	// stayNights is the relative weight of a stay of n+1 nights.
	stayNights = []float64{30, 24, 16, 11, 8, 6, 5}
)

// Model is the seeded data plus this sample's writes, with their times.
//
// It is built in each phase pod from the dataset and the sample's stream, then every earlier
// write of the sample is replayed into it through its Apply, so a phase continues from the
// values the earlier ones wrote. Generation applies a write as pending as soon as it is built;
// the check sees every write with its outcome and times.
type Model struct {
	data *dataset.Dataset
	ref  *hotel.Reference
	// state is the oracle's model with every write applied, pending ones included: what
	// generation builds on (a profile write keeps the latest name).
	state *hotel.State
	// managed[i]: a quarter of the hotels, fixed by index, whose capacity set_capacity changes
	// and that reservations never book.
	managed []bool

	profiles  *workload.Versions[string, hotel.Profile]
	prices    *workload.Versions[hotel.PlanKey, hotel.Prices]
	rooms     *workload.Versions[string, int]
	additions *workload.Versions[string, bool] // true once the hotel is added
	added     []*dataset.Hotel                 // additions built or sent, in order
	confirmed []*dataset.Hotel                 // additions acknowledged in earlier phases

	firstNight   time.Time
	nextSlot     int
	used         map[int]map[int]bool // night -> hotel indices reserved on it
	nextAddition int
	cell         uint64 // the sandbox cell of this sample's additions
	tag          uint64 // added hotels are a<tag>-<n>
	ratingCap    float64
	priceFloor   float64
}

// NewModel is the model of a sample: rand is the sample's model stream.
func NewModel(data *dataset.Dataset, rand *workload.Rand) (*Model, error) {
	ref := hotel.NewReference(data)
	m := &Model{
		data: data, ref: ref, state: hotel.NewState(ref),
		managed:      make([]bool, len(data.Hotels)),
		profiles:     workload.NewVersions[string, hotel.Profile](StalenessBound),
		prices:       workload.NewVersions[hotel.PlanKey, hotel.Prices](StalenessBound),
		rooms:        workload.NewVersions[string, int](StalenessBound),
		additions:    workload.NewVersions[string, bool](StalenessBound),
		used:         map[int]map[int]bool{},
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
	return &m.data.Hotels[m.data.Cities[city].Hotels[rank]]
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

// sandboxLocation is, 3% of the time, a point within 0.5 km of an addition acknowledged in an
// earlier phase.
func (m *Model) sandboxLocation(rand *workload.Rand) (lat, lon string, ok bool) {
	if rand.Random() >= sandboxShare || len(m.confirmed) == 0 {
		return "", "", false
	}
	anchor := m.confirmed[rand.Below(len(m.confirmed))]
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

// The infos: what each request was built from, as its Call's Info.
type (
	expectedInfo struct {
		Expected []string `json:"expected"`
	}
	nearInfo struct {
		Near [2]string `json:"near"`
	}
	searchInfo struct {
		Expected *[]string `json:"expected"`
		Near     [2]string `json:"near"`
	}
	locationInfo struct {
		Lat     string `json:"lat"`
		Lon     string `json:"lon"`
		Sandbox bool   `json:"sandbox,omitempty"`
	}
	reservationInfo struct {
		Slot  int `json:"slot"`
		Hotel int `json:"hotel"`
	}
	rateInfo struct {
		Hotel  string     `json:"hotel"`
		Code   string     `json:"code"`
		Prices [3]float64 `json:"prices"`
	}
	profileInfo struct {
		Hotel string `json:"hotel"`
		Name  string `json:"name"`
		Phone string `json:"phone"`
	}
	capacityInfo struct {
		Hotel string `json:"hotel"`
		Rooms int    `json:"rooms"`
	}
	additionInfo struct {
		N    int    `json:"n"`
		Seed uint64 `json:"seed"`
	}
	hotelInfo struct {
		Hotel string `json:"hotel"`
	}
)

var (
	query  = workload.Query[string]
	get    = workload.Get[string]
	post   = workload.Post[string]
	locale = query("locale", "en")
)

func (m *Model) search(rand *workload.Rand) workload.Request {
	nearLat, nearLon, near := m.sandboxLocation(rand.Child("sandbox"))
	in, out := stay(rand.Child("stay"))
	if near {
		return workload.Request{
			Steps: []workload.Step{get("/hotels", query("inDate", in), query("outDate", out),
				query("lat", nearLat), query("lon", nearLon))},
			Info: nearInfo{Near: [2]string{nearLat, nearLon}},
		}
	}
	for attempt := range searchAttempts {
		lat, lon := m.location(rand.Child("location", attempt))
		expected, exact, err := m.ref.SearchCandidates(lat, lon)
		if err != nil {
			panic(err)
		}
		if exact {
			options := []workload.Option{query("inDate", in), query("outDate", out), query("lat", lat), query("lon", lon)}
			if rand.Random() < 0.5 {
				options = append(options, locale)
			}
			if expected == nil {
				expected = []string{}
			}
			return workload.Request{Steps: []workload.Step{get("/hotels", options...)}, Info: expectedInfo{expected}}
		}
	}
	panic(fmt.Sprintf("no unambiguous search location in %d draws", searchAttempts))
}

func recommend(require string) func(*Model, *workload.Rand) workload.Request {
	return func(m *Model, rand *workload.Rand) workload.Request {
		if require == "dis" {
			if lat, lon, near := m.sandboxLocation(rand.Child("sandbox")); near {
				return workload.Request{
					Steps: []workload.Step{get("/recommendations", query("require", require), query("lat", lat), query("lon", lon))},
					Info:  locationInfo{Lat: lat, Lon: lon, Sandbox: true},
				}
			}
		}
		lat, lon := m.location(rand.Child("location"))
		options := []workload.Option{query("require", require), query("lat", lat), query("lon", lon)}
		if rand.Random() < 0.5 {
			options = append(options, locale)
		}
		return workload.Request{Steps: []workload.Step{get("/recommendations", options...)}, Info: locationInfo{Lat: lat, Lon: lon}}
	}
}

func login(valid bool) func(*Model, *workload.Rand) workload.Request {
	return func(m *Model, rand *workload.Rand) workload.Request {
		u := m.user(rand)
		password := u.Password
		if !valid {
			password += "-invalid"
		}
		return workload.Request{Steps: []workload.Step{get("/user", query("username", u.Username), query("password", password))}}
	}
}

// reserveCapacity reserves a hotel's whole capacity on a sample-owned night, then one room
// more (rejected). Exclusive serializes every reservation: the service's capacity check and
// write are not transactional, so two concurrent fills could both see free rooms and the
// rejection would not be determined.
func (m *Model) reserveCapacity(rand *workload.Rand) workload.Request {
	slot := m.nextSlot
	night := slot / reservationsPerNight
	if night >= reservationNights {
		panic(fmt.Sprintf("the sample used its %d reservation slots", reservationNights*reservationsPerNight))
	}
	used := m.used[night]
	index := m.pickHotel(rand, reservationAttempts, func(i int) bool { return !used[i] && !m.managed[i] })
	h := &m.data.Hotels[index]
	u := m.user(rand.Child("user"))
	in := m.firstNight.AddDate(0, 0, night)
	common := []workload.Option{query("inDate", day(in)), query("outDate", day(in.AddDate(0, 0, 1))),
		query("hotelId", h.ID), query("customerName", fmt.Sprintf("vibesys-%016x", rand.Uint64()&customerMask)),
		query("username", u.Username), query("password", u.Password)}
	with := func(rooms string) []workload.Option {
		return append(append([]workload.Option(nil), common...), query("number", rooms))
	}
	return workload.Request{
		Steps:     []workload.Step{get("/reservation", with(strconv.Itoa(h.Capacity))...), get("/reservation", with("1")...)},
		Info:      reservationInfo{Slot: slot, Hotel: index},
		Exclusive: "reservation",
	}
}

// updateRate changes one plan of a popular hotel, its prices scaled by 0.8-1.25.
func (m *Model) updateRate(rand *workload.Rand) workload.Request {
	h := m.popularHotel(rand.Child("hotel"))
	plan := &h.RatePlans[rand.Below(len(h.RatePlans))]
	total := dataset.Round(plan.TotalRate*(0.8+float64(0.45*rand.Random())), 100)
	prices := [3]float64{total, total, dataset.Round(total*plan.TotalRateInclusive/plan.TotalRate, 100)}
	// Writes to one item never overlap, so its versions are in application order.
	return workload.Request{
		Steps: []workload.Step{post("/admin/rate", query("hotelId", h.ID), query("code", plan.Code),
			query("bookableRate", number(prices[0])), query("totalRate", number(prices[1])),
			query("totalRateInclusive", number(prices[2])))},
		Exclusive: "rate:" + h.ID + ":" + plan.Code,
		Info:      rateInfo{Hotel: h.ID, Code: plan.Code, Prices: prices},
	}
}

// updateProfile writes a new phone in the city's format; in 10% of the updates a new name too.
func (m *Model) updateProfile(rand *workload.Rand) workload.Request {
	h := m.popularHotel(rand.Child("hotel"))
	profile, _ := m.state.Profile(h.ID)
	name := profile.Name
	phone := dataset.NewPhoneFrom(m.data, h, rand.Child("phone"))
	options := []workload.Option{query("hotelId", h.ID), query("phone", phone)}
	if rand.Random() < 0.1 {
		name = dataset.NewNameFrom(m.data, h, rand.Child("name"))
		options = append(options, query("name", name))
	}
	return workload.Request{
		Steps:     []workload.Step{post("/admin/profile", options...)},
		Exclusive: "profile:" + h.ID,
		Info:      profileInfo{Hotel: h.ID, Name: name, Phone: phone},
	}
}

// setCapacity sets a capacity-managed hotel's rooms to 0.5-2x the seeded ones, then reserves
// one room more: rejected at once.
func (m *Model) setCapacity(rand *workload.Rand) workload.Request {
	h := &m.data.Hotels[m.pickHotel(rand, writeAttempts, func(i int) bool { return m.managed[i] })]
	low, high := max(1, h.Capacity/2), 2*h.Capacity
	rooms := low + int(rand.Random()*float64(high-low+1))
	u := m.user(rand.Child("user"))
	probe := []workload.Option{query("inDate", day(m.firstNight)), query("outDate", day(m.firstNight.AddDate(0, 0, 1))),
		query("hotelId", h.ID), query("customerName", fmt.Sprintf("vibesys-capacity-%016x", rand.Uint64()&customerMask)),
		query("username", u.Username), query("password", u.Password), query("number", strconv.Itoa(rooms+1))}
	return workload.Request{
		Steps: []workload.Step{
			post("/admin/capacity", query("hotelId", h.ID), query("rooms", strconv.Itoa(rooms))),
			get("/reservation", probe...),
		},
		Exclusive: "capacity:" + h.ID,
		Info:      capacityInfo{Hotel: h.ID, Rooms: rooms},
	}
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
	h := m.addition(n, seed)
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
	return workload.Request{Steps: []workload.Step{post("/admin/hotel", workload.JSON(body))}, Info: additionInfo{N: n, Seed: seed}}
}

func (m *Model) rates(rand *workload.Rand) workload.Request {
	h := m.popularHotel(rand)
	return workload.Request{Steps: []workload.Step{get("/rates", query("hotelId", h.ID))}, Info: hotelInfo{h.ID}}
}

// ------------------------------------------------------------------ apply: writes -> model
// Pending (no times) while the phase is generated; with the outcome and times when the history
// is replayed. A failed write counts as possibly applied (Versions).

func decode[T any](call workload.Call) (T, error) {
	var info T
	err := call.DecodeInfo(&info)
	return info, err
}

func (m *Model) applyRate(call workload.Call) error {
	info, err := decode[rateInfo](call)
	if err != nil {
		return err
	}
	prices := hotel.Prices{Bookable: info.Prices[0], Total: info.Prices[1], Inclusive: info.Prices[2]}
	m.prices.Write(hotel.PlanKey{HotelID: info.Hotel, Code: info.Code}, prices, call)
	return m.state.SetPrices(info.Hotel, info.Code, prices)
}

func (m *Model) applyProfile(call workload.Call) error {
	info, err := decode[profileInfo](call)
	if err != nil {
		return err
	}
	m.profiles.Write(info.Hotel, hotel.Profile{Name: info.Name, Phone: info.Phone}, call)
	return m.state.SetProfile(info.Hotel, info.Name, info.Phone)
}

func (m *Model) applyCapacity(call workload.Call) error {
	info, err := decode[capacityInfo](call)
	if err != nil {
		return err
	}
	m.rooms.Write(info.Hotel, info.Rooms, call)
	return m.state.SetCapacity(info.Hotel, info.Rooms)
}

func (m *Model) applyReservation(call workload.Call) error {
	info, err := decode[reservationInfo](call)
	if err != nil {
		return err
	}
	if info.Hotel < 0 || info.Hotel >= len(m.data.Hotels) {
		return fmt.Errorf("reservation of hotel index %d", info.Hotel)
	}
	m.nextSlot = max(m.nextSlot, info.Slot+1)
	night := info.Slot / reservationsPerNight
	if m.used[night] == nil {
		m.used[night] = map[int]bool{}
	}
	m.used[night][info.Hotel] = true
	return nil
}

func (m *Model) applyAddition(call workload.Call) error {
	info, err := decode[additionInfo](call)
	if err != nil {
		return err
	}
	h := m.addition(info.N, info.Seed)
	m.nextAddition = max(m.nextAddition, info.N+1)
	m.added = append(m.added, h)
	m.additions.Write(h.ID, true, call)
	if call.OK {
		m.confirmed = append(m.confirmed, h)
	}
	return m.state.Add(*h)
}

// ------------------------------------------------------------------------------ workload

// Workload is the benchmark's mix: the weights of the task's earlier closed.toml and open.toml.
func Workload() workload.Workload[*dataset.Dataset, *Model] {
	return workload.Workload[*dataset.Dataset, *Model]{
		Dataset: dataset.Read,
		Model:   NewModel,
		Operations: []workload.Operation[*Model]{
			{Name: "search_hotels", Weight: 6000, Build: (*Model).search, Check: (*Model).checkSearch},
			{Name: "recommend_distance", Weight: 1300, Build: recommend("dis"), Check: checkRecommendation("dis")},
			{Name: "recommend_rate", Weight: 1300, Build: recommend("rate"), Check: checkRecommendation("rate")},
			{Name: "recommend_price", Weight: 1300, Build: recommend("price"), Check: checkRecommendation("price")},
			{Name: "login_valid", Weight: 30, Build: login(true), Check: checkMessage(0, loginSuccess)},
			{Name: "login_invalid", Weight: 20, Build: login(false), Check: checkMessage(0, loginFailure)},
			{Name: "reserve_capacity", Weight: 50, Build: (*Model).reserveCapacity,
				Check: checkMessages(reservationSuccess, reservationFailure), Apply: (*Model).applyReservation},
			{Name: "hotel_rates", Weight: 400, Build: (*Model).rates, Check: (*Model).checkRates},
			{Name: "update_rate", Weight: 42, Build: (*Model).updateRate,
				Check: checkMessage(0, "Rate updated."), Apply: (*Model).applyRate},
			{Name: "update_profile", Weight: 42, Build: (*Model).updateProfile,
				Check: checkMessage(0, "Profile updated."), Apply: (*Model).applyProfile},
			{Name: "set_capacity", Weight: 12, Build: (*Model).setCapacity,
				Check: checkMessages("Capacity updated.", reservationFailure), Apply: (*Model).applyCapacity},
			{Name: "add_hotel", Weight: 8, Build: (*Model).addHotel,
				Check: checkMessage(0, "Hotel added."), Apply: (*Model).applyAddition},
		},
	}
}
