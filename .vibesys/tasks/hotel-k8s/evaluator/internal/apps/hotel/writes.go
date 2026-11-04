package hotel

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/api"
)

// Admin writes. About 1% of the canonical mix: rate and profile updates of
// hotels drawn by popularity (as reservations draw them, so hot data
// changes), a few capacity changes and rare hotel additions.
//
//   - update_rate: one plan of the hotel, its prices scaled by 0.8-1.25.
//   - update_profile: a new phone in the city's format, and in 10% of the
//     updates a new name.
//   - set_capacity: a capacity between half and twice the seeded one, on a
//     capacity-managed hotel (a quarter of the hotels, fixed by ID, that
//     reserve_capacity never books, so no night of theirs holds reservations
//     and every change is accepted), followed by a reservation of one room
//     more than the new capacity on a trial night, which must be rejected:
//     capacity changes apply to reservations at once.
//   - add_hotel: a generated hotel in the trial's sandbox (dataset.Sandbox),
//     with a rating below the best and a price above the cheapest, so the
//     "rate" and "price" recommendations do not change. A small share of the
//     searches and distance recommendations then query the sandbox.
//
// The model (writeModel) records every write this load generator made and
// validates reads against the values the staleness bound allows at the time
// (history.allowed). Writes to one item are serialized: a plan draws another
// hotel while one is in flight. Reset restores every written item to its
// seeded value, so the next trial and the next load generator (the open
// loop runs in the same namespace) start from the dataset; added hotels
// stay, in sandboxes no other trial queries.
const (
	operationRates         = "hotel_rates"
	operationUpdateRate    = "update_rate"
	operationUpdateProfile = "update_profile"
	operationSetCapacity   = "set_capacity"
	operationAddHotel      = "add_hotel"

	rateUpdated     = "Rate updated."
	profileUpdated  = "Profile updated."
	capacityUpdated = "Capacity updated."
	hotelAdded      = "Hotel added."

	// sandboxShare of the searches and distance recommendations query the
	// trial's sandbox once it holds a visible addition.
	sandboxShare = 0.03
	// sandboxJitterKm is the largest offset of a sandbox query from the
	// addition it starts from.
	sandboxJitterKm = 0.5
	// writeAttempts bounds popularity draws of an item without a write in
	// flight, before falling back to the city's next hotels.
	writeAttempts = 16
	// maxOptionalAdditions bounds the subsets of in-flight additions a
	// sandbox search answer is compared with.
	maxOptionalAdditions = 8
	// restoreWorkers issue Reset's restoring writes.
	restoreWorkers = 8
)

const (
	streamWriteValue  = 40
	streamWriteName   = 41
	streamWritePlan   = 42
	streamWriteNames  = 43
	streamAddPoint    = 44
	streamAddHotel    = 45
	streamSandbox     = 46
	streamSandboxPick = 47
	streamSandboxLat  = 48
	streamSandboxLon  = 49
)

// writeModel is what this load generator wrote, by item.
type writeModel struct {
	mu       sync.Mutex
	profiles map[string]*history[hotelsupport.Profile]
	prices   map[hotelsupport.PlanKey]*history[hotelsupport.Prices]
	capacity map[string]*history[int]
}

func newWriteModel() *writeModel {
	return &writeModel{
		profiles: map[string]*history[hotelsupport.Profile]{},
		prices:   map[hotelsupport.PlanKey]*history[hotelsupport.Prices]{},
		capacity: map[string]*history[int]{},
	}
}

func (m *writeModel) profile(hotel *dataset.Hotel) *history[hotelsupport.Profile] {
	h, ok := m.profiles[hotel.ID]
	if !ok {
		h = &history[hotelsupport.Profile]{base: hotelsupport.Profile{Name: hotel.Name, Phone: hotel.Phone}}
		m.profiles[hotel.ID] = h
	}
	return h
}

func (m *writeModel) plan(hotel *dataset.Hotel, plan *dataset.RatePlan) *history[hotelsupport.Prices] {
	key := hotelsupport.PlanKey{HotelID: hotel.ID, Code: plan.Code}
	h, ok := m.prices[key]
	if !ok {
		h = &history[hotelsupport.Prices]{base: seededPrices(plan)}
		m.prices[key] = h
	}
	return h
}

func (m *writeModel) rooms(hotel *dataset.Hotel) *history[int] {
	h, ok := m.capacity[hotel.ID]
	if !ok {
		h = &history[int]{base: hotel.Capacity}
		m.capacity[hotel.ID] = h
	}
	return h
}

func seededPrices(plan *dataset.RatePlan) hotelsupport.Prices {
	return hotelsupport.Prices{Bookable: plan.BookableRate, Total: plan.TotalRate, Inclusive: plan.TotalRateInclusive}
}

// allowedProfiles returns the profiles a read of a seeded hotel may observe.
func (m *writeModel) allowedProfiles(hotel *dataset.Hotel, start, end time.Time) []hotelsupport.Profile {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.profiles[hotel.ID]; ok {
		return h.allowed(start, end)
	}
	return []hotelsupport.Profile{{Name: hotel.Name, Phone: hotel.Phone}}
}

// allowedPrices returns the prices a read of a plan may observe.
func (m *writeModel) allowedPrices(hotel *dataset.Hotel, plan *dataset.RatePlan, start, end time.Time) []hotelsupport.Prices {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.prices[hotelsupport.PlanKey{HotelID: hotel.ID, Code: plan.Code}]; ok {
		return h.allowed(start, end)
	}
	return []hotelsupport.Prices{seededPrices(plan)}
}

// addition is a hotel this trial added.
type addition struct {
	hotel        *dataset.Hotel
	built, acked time.Time
	ok, done     bool
}

// additionsAt splits the trial's additions for a read built at start and
// validated at end: required (acknowledged more than StalenessBound before
// start) and optional (built before end, not required).
func (m *writeModel) additionsAt(data *fixture, start, end time.Time) (required, optional []*dataset.Hotel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, item := range data.added {
		switch {
		case item.ok && item.acked.Before(start.Add(-StalenessBound)):
			required = append(required, item.hotel)
		case item.built.Before(end):
			optional = append(optional, item.hotel)
		}
	}
	return required, optional
}

// capacityManaged reports whether set_capacity may change a seeded hotel's
// capacity; reserve_capacity never books these hotels.
func capacityManaged(index int) bool { return dataset.Mix(uint64(index), 0x636170)%4 == 0 }

// writeTarget draws a seeded hotel by popularity that eligible accepts.
func (a *Application) writeTarget(random uint64, eligible func(index int) bool) int {
	for attempt := uint64(0); attempt < writeAttempts; attempt++ {
		city, rank := a.popularHotel(random, attempt)
		if candidate := a.reference.Data.Cities[city].Hotels[rank]; eligible(candidate) {
			return candidate
		}
	}
	city, rank := a.popularHotel(random, 0)
	hotels := a.reference.Data.Cities[city].Hotels
	for offset := 1; offset <= len(hotels); offset++ {
		if candidate := hotels[(rank+offset)%len(hotels)]; eligible(candidate) {
			return candidate
		}
	}
	return -1
}

func postPlan(operation api.Operation, request api.HTTPRequestSpec, state *operationState) api.OperationPlan {
	return api.OperationPlan{
		Invocations: []api.Invocation{{Target: operation.Target, Operation: operation.Name, Payload: request}},
		State:       state,
	}
}

func (a *Application) profileWritePlan(operation api.Operation, sample api.Sample) (api.OperationPlan, error) {
	built := time.Now()
	a.writes.mu.Lock()
	defer a.writes.mu.Unlock()
	index := a.writeTarget(sample.Random, func(index int) bool {
		return !a.writes.profile(&a.reference.Data.Hotels[index]).busy
	})
	if index < 0 {
		return api.OperationPlan{}, fmt.Errorf("every profile has a write in flight")
	}
	hotel := &a.reference.Data.Hotels[index]
	h := a.writes.profile(hotel)
	value := h.latest()
	value.Phone = dataset.NewPhone(hotel, dataset.Mix(sample.Random, streamWriteValue))
	query := map[string]string{"hotelId": hotel.ID, "phone": value.Phone}
	if dataset.Unit(sample.Random, streamWriteName) < 0.1 {
		value.Name = dataset.NewName(hotel, dataset.Mix(sample.Random, streamWriteNames))
		query["name"] = value.Name
	}
	h.begin(value, built)
	return postPlan(operation, api.HTTPRequestSpec{Method: http.MethodPost, Path: "/admin/profile", Query: query},
		&operationState{kind: operation.Name, built: built, messages: []string{profileUpdated},
			finish: a.finisher(func(acked time.Time, ok bool) { h.finish(acked, ok) })}), nil
}

func (a *Application) rateWritePlan(operation api.Operation, sample api.Sample) (api.OperationPlan, error) {
	built := time.Now()
	a.writes.mu.Lock()
	defer a.writes.mu.Unlock()
	pick := func(hotel *dataset.Hotel) *dataset.RatePlan {
		return &hotel.RatePlans[int(dataset.Unit(sample.Random, streamWritePlan)*float64(len(hotel.RatePlans)))]
	}
	index := a.writeTarget(sample.Random, func(index int) bool {
		hotel := &a.reference.Data.Hotels[index]
		return !a.writes.plan(hotel, pick(hotel)).busy
	})
	if index < 0 {
		return api.OperationPlan{}, fmt.Errorf("every rate plan has a write in flight")
	}
	hotel := &a.reference.Data.Hotels[index]
	plan := pick(hotel)
	h := a.writes.plan(hotel, plan)
	total := roundCents(plan.TotalRate * (0.8 + 0.45*dataset.Unit(sample.Random, streamWriteValue)))
	value := hotelsupport.Prices{
		Bookable: total, Total: total, Inclusive: roundCents(total * plan.TotalRateInclusive / plan.TotalRate),
	}
	h.begin(value, built)
	return postPlan(operation, rateRequest(hotel.ID, plan.Code, value),
		&operationState{kind: operation.Name, built: built, messages: []string{rateUpdated},
			finish: a.finisher(func(acked time.Time, ok bool) { h.finish(acked, ok) })}), nil
}

func rateRequest(hotelID, code string, prices hotelsupport.Prices) api.HTTPRequestSpec {
	format := func(value float64) string { return strconv.FormatFloat(value, 'f', -1, 64) }
	return api.HTTPRequestSpec{Method: http.MethodPost, Path: "/admin/rate", Query: map[string]string{
		"hotelId": hotelID, "code": code, "bookableRate": format(prices.Bookable),
		"totalRate": format(prices.Total), "totalRateInclusive": format(prices.Inclusive),
	}}
}

func capacityRequest(hotelID string, rooms int) api.HTTPRequestSpec {
	return api.HTTPRequestSpec{Method: http.MethodPost, Path: "/admin/capacity", Query: map[string]string{
		"hotelId": hotelID, "rooms": strconv.Itoa(rooms),
	}}
}

func profileRequest(hotelID string, profile hotelsupport.Profile) api.HTTPRequestSpec {
	return api.HTTPRequestSpec{Method: http.MethodPost, Path: "/admin/profile", Query: map[string]string{
		"hotelId": hotelID, "name": profile.Name, "phone": profile.Phone,
	}}
}

func roundCents(value float64) float64 { return math.Round(value*100) / 100 }

func (a *Application) capacityWritePlan(operation api.Operation, sample api.Sample, data *fixture) (api.OperationPlan, error) {
	built := time.Now()
	a.writes.mu.Lock()
	defer a.writes.mu.Unlock()
	index := a.writeTarget(sample.Random, func(index int) bool {
		return capacityManaged(index) && !a.writes.rooms(&a.reference.Data.Hotels[index]).busy
	})
	if index < 0 {
		return api.OperationPlan{}, fmt.Errorf("every capacity-managed hotel has a write in flight")
	}
	hotel := &a.reference.Data.Hotels[index]
	h := a.writes.rooms(hotel)
	low, high := max(1, hotel.Capacity/2), 2*hotel.Capacity
	rooms := low + int(dataset.Unit(sample.Random, streamWriteValue)*float64(high-low+1))
	h.begin(rooms, built)
	username, password := a.user(sample.Random, streamReserveUser)
	night := data.dateBase
	probe := api.HTTPRequestSpec{Method: http.MethodGet, Path: "/reservation", Query: map[string]string{
		"inDate": night.Format(time.DateOnly), "outDate": night.AddDate(0, 0, 1).Format(time.DateOnly),
		"hotelId": hotel.ID, "customerName": fmt.Sprintf("vibesys-capacity-%016x", sample.Random),
		"username": username, "password": password, "number": strconv.Itoa(rooms + 1),
	}}
	return api.OperationPlan{
		Invocations: []api.Invocation{
			{Target: operation.Target, Operation: operation.Name, Payload: capacityRequest(hotel.ID, rooms)},
			{Target: operation.Target, Operation: operation.Name, Payload: probe},
		},
		State: &operationState{kind: operation.Name, built: built, messages: []string{capacityUpdated, reservationFailure},
			finish: a.finisher(func(acked time.Time, ok bool) { h.finish(acked, ok) })},
	}, nil
}

// NewHotelBody is the JSON body of POST /admin/hotel.
type newHotelBody struct {
	HotelID     string          `json:"hotelId"`
	Name        string          `json:"name"`
	PhoneNumber string          `json:"phoneNumber"`
	Description string          `json:"description"`
	Address     newHotelAddress `json:"address"`
	Lat         float64         `json:"lat"`
	Lon         float64         `json:"lon"`
	Rating      float64         `json:"rating"`
	Price       float64         `json:"price"`
	Rooms       int             `json:"rooms"`
	RatePlans   []newRatePlan   `json:"ratePlans"`
}

type newHotelAddress struct {
	StreetNumber string `json:"streetNumber"`
	StreetName   string `json:"streetName"`
	City         string `json:"city"`
	State        string `json:"state"`
	Country      string `json:"country"`
	PostalCode   string `json:"postalCode"`
}

type newRatePlan struct {
	Code     string      `json:"code"`
	InDate   string      `json:"inDate"`
	OutDate  string      `json:"outDate"`
	RoomType newRoomType `json:"roomType"`
}

type newRoomType struct {
	Code               string  `json:"code"`
	RoomDescription    string  `json:"roomDescription"`
	BookableRate       float64 `json:"bookableRate"`
	TotalRate          float64 `json:"totalRate"`
	TotalRateInclusive float64 `json:"totalRateInclusive"`
}

// AddHotelRequest is the POST /admin/hotel request that adds hotel.
func AddHotelRequest(hotel *dataset.Hotel) api.HTTPRequestSpec {
	body := newHotelBody{
		HotelID: hotel.ID, Name: hotel.Name, PhoneNumber: hotel.Phone, Description: hotel.Description,
		Address: newHotelAddress{
			StreetNumber: hotel.Address.StreetNumber, StreetName: hotel.Address.StreetName,
			City: hotel.Address.City, State: hotel.Address.State, Country: hotel.Address.Country,
			PostalCode: hotel.Address.PostalCode,
		},
		Lat: hotel.Lat, Lon: hotel.Lon, Rating: hotel.Rating, Price: hotel.Price, Rooms: hotel.Capacity,
	}
	for _, plan := range hotel.RatePlans {
		body.RatePlans = append(body.RatePlans, newRatePlan{
			Code: plan.Code, InDate: plan.InDate, OutDate: plan.OutDate,
			RoomType: newRoomType{
				Code: plan.RoomCode, RoomDescription: plan.RoomDescription, BookableRate: plan.BookableRate,
				TotalRate: plan.TotalRate, TotalRateInclusive: plan.TotalRateInclusive,
			},
		})
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return api.HTTPRequestSpec{
		Method: http.MethodPost, Path: "/admin/hotel",
		Headers: map[string]string{"Content-Type": "application/json"}, Body: string(encoded),
	}
}

func (a *Application) addHotelPlan(operation api.Operation, sample api.Sample, data *fixture) (api.OperationPlan, error) {
	built := time.Now()
	a.writes.mu.Lock()
	defer a.writes.mu.Unlock()
	id := dataset.AddedHotelID(data.tag, len(data.added)+1)
	lat, lon := dataset.SandboxPoint(data.sandbox, dataset.Mix(sample.Random, streamAddPoint))
	hotel := dataset.AddedHotel(id, dataset.Mix(sample.Random, streamAddHotel), lat, lon)
	// Keep the "rate" and "price" recommendations of the whole namespace.
	hotel.Rating = math.Min(hotel.Rating, roundCents(a.reference.MaxRating()-0.01))
	hotel.Price = math.Max(hotel.Price, roundCents(a.reference.MinPrice()+0.01))
	item := &addition{hotel: &hotel, built: built}
	data.added = append(data.added, item)
	return postPlan(operation, AddHotelRequest(&hotel),
		&operationState{kind: operation.Name, built: built, messages: []string{hotelAdded},
			finish: a.finisher(func(acked time.Time, ok bool) {
				if !item.done {
					item.acked, item.ok, item.done = acked, ok, true
				}
			})}), nil
}

// finisher wraps an outcome recorder in the model's lock.
func (a *Application) finisher(record func(acked time.Time, ok bool)) func(time.Time, bool) {
	return func(acked time.Time, ok bool) {
		a.writes.mu.Lock()
		defer a.writes.mu.Unlock()
		record(acked, ok)
	}
}

// ratesPlan reads the rate plans of a hotel drawn by popularity.
func (a *Application) ratesPlan(operation api.Operation, sample api.Sample) api.OperationPlan {
	city, rank := a.popularHotel(sample.Random, 0)
	hotel := &a.reference.Data.Hotels[a.reference.Data.Cities[city].Hotels[rank]]
	return postPlan(operation, api.HTTPRequestSpec{
		Method: http.MethodGet, Path: "/rates", Query: map[string]string{"hotelId": hotel.ID},
	}, &operationState{kind: operation.Name, built: time.Now(), hotel: hotel})
}

// sandboxLocation returns a query location in the trial's sandbox near an
// addition visible to a read built now, if the sandbox holds one.
func (a *Application) sandboxLocation(random uint64, data *fixture) (lat, lon string, ok bool) {
	if dataset.Unit(random, streamSandbox) >= sandboxShare {
		return "", "", false
	}
	required, _ := a.writes.additionsAt(data, time.Now(), time.Now())
	if len(required) == 0 {
		return "", "", false
	}
	anchor := required[int(dataset.Unit(random, streamSandboxPick)*float64(len(required)))]
	_, _, kmPerDegreeLon := dataset.Sandbox(data.sandbox)
	dy := (2*dataset.Unit(random, streamSandboxLat) - 1) * sandboxJitterKm
	dx := (2*dataset.Unit(random, streamSandboxLon) - 1) * sandboxJitterKm
	return formatCoordinate(anchor.Lat + dy/111.32), formatCoordinate(anchor.Lon + dx/kmPerDegreeLon), true
}

// Reset restores every item this load generator changed to its seeded
// value, through the admin routes, and records the restoring writes.
func (a *Application) Reset(ctx context.Context, runtime api.Runtime, _ api.TrialContext) error {
	if runtime == nil || a.writes == nil {
		return nil
	}
	type restore struct {
		request api.HTTPRequestSpec
		expect  string
		finish  func(time.Time, bool)
	}
	var restores []restore
	a.writes.mu.Lock()
	now := time.Now()
	for id, h := range a.writes.profiles {
		if h.latest() != h.base {
			h := h
			h.begin(h.base, now)
			restores = append(restores, restore{profileRequest(id, h.base), profileUpdated, func(t time.Time, ok bool) { h.finish(t, ok) }})
		}
	}
	for key, h := range a.writes.prices {
		if h.latest() != h.base {
			h := h
			h.begin(h.base, now)
			restores = append(restores, restore{rateRequest(key.HotelID, key.Code, h.base), rateUpdated, func(t time.Time, ok bool) { h.finish(t, ok) }})
		}
	}
	for id, h := range a.writes.capacity {
		if h.latest() != h.base {
			h := h
			h.begin(h.base, now)
			restores = append(restores, restore{capacityRequest(id, h.base), capacityUpdated, func(t time.Time, ok bool) { h.finish(t, ok) }})
		}
	}
	a.writes.mu.Unlock()

	jobs := make(chan restore)
	errs := make(chan error, len(restores))
	var wg sync.WaitGroup
	for worker := 0; worker < restoreWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				requestContext, cancel := context.WithTimeout(ctx, a.config.Timeout)
				result := runtime.Invoke(requestContext, api.Invocation{
					Target: hotelsupport.GatewayTarget, Operation: "restore", Payload: job.request,
				})
				cancel()
				validation := validateMessage(result, job.expect)
				a.finisher(job.finish)(time.Now(), validation.Success)
				if !validation.Success {
					errs <- fmt.Errorf("restore %s %v: %s", job.request.Path, job.request.Query, validation.ErrorMessage)
				}
			}
		}()
	}
	for _, job := range restores {
		jobs <- job
	}
	close(jobs)
	wg.Wait()
	close(errs)
	return <-errs
}
