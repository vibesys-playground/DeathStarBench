package hotel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"time"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/accuracy/httpcheck"
	"vibesys/microservice-evaluator/api"
)

// Checks of the write contract (OBJECTIVE.md): after a write is acknowledged
// (HTTP 200), every read that starts more than StalenessBound later reflects
// it, through every replica; before that a read shows the old or the new
// value. Capacity changes apply to reservations at once and never go below
// the rooms reserved on a night. The checks run after every other check,
// because they change the seeded data the others expect.
//
// A visibility check reads right after the acknowledgement (old or new), then
// waits until StalenessBound plus boundMargin has passed and reads
// repeatedReads times: internal calls round-robin over the instances of each
// service, so the reads reach every replica of a two-replica deployment
// with probability 1 - 2^-7 per hop, and a cache or index that one replica
// never refreshes fails the check.
const (
	stalenessBound = time.Second
	boundMargin    = 50 * time.Millisecond
	repeatedReads  = 8

	rateUpdated     = "Rate updated."
	profileUpdated  = "Profile updated."
	capacityUpdated = "Capacity updated."
	hotelAdded      = "Hotel added."
)

// verifyWrites runs the write checks in order; each passes its property.
func (a *Application) verifyWrites(
	ctx context.Context, c client, check api.AccuracyContext, random *rand.Rand,
	recorder api.AccuracyRecorder, lifecycle bool,
) error {
	w := a.newWriteChecks(c, check.Seed, random)
	used := map[string]bool{}
	steps := []struct {
		property string
		run      func() error
	}{
		{"write_validation", func() error { return w.verifyWriteValidation(ctx) }},
		{"write_rate_visibility", func() error { return w.verifyRateWrites(ctx, used) }},
		{"write_profile_visibility", func() error { return w.verifyProfileWrites(ctx, used) }},
		{"write_capacity", func() error { return w.verifyCapacityWrites(ctx, used) }},
		{"write_hotel_addition", func() error { return w.verifyHotelAdditions(ctx) }},
	}
	if lifecycle {
		steps = append(steps, struct {
			property string
			run      func() error
		}{"durable_writes", func() error { return w.verifyDurableWrites(ctx, check.Crash, check.Start) }})
	}
	for _, step := range steps {
		if err := checkContext(ctx); err != nil {
			return err
		}
		before := w.checks
		err := step.run()
		recorder.AddChecks(w.checks - before)
		if err != nil {
			return fmt.Errorf("%s: %w", step.property, err)
		}
		if err := recorder.Pass(step.property); err != nil {
			return err
		}
	}
	return nil
}

// writeChecks holds the model of one Check run's writes.
type writeChecks struct {
	a      *Application
	c      client
	random *rand.Rand
	seed   int64
	state  *hotelsupport.State
	// reserved is rooms by hotel and night that these checks reserved.
	reserved map[string]int
	checks   int
	// Fixtures kept for durable_writes.
	ratePlans []hotelsupport.PlanKey
	profiles  []string
	capacity  struct {
		increased, decreased string
		full                 [2]string // a night the decreased hotel has full
	}
	added []*dataset.Hotel
}

func (a *Application) newWriteChecks(c client, seed int64, random *rand.Rand) *writeChecks {
	return &writeChecks{
		a: a, c: c, random: random, seed: seed,
		state: hotelsupport.NewState(hotelsupport.DefaultReference()), reserved: map[string]int{},
	}
}

// post sends a write and returns its HTTP status and, for 200, its message.
func (w *writeChecks) post(ctx context.Context, spec api.HTTPRequestSpec) (int, string, error) {
	w.checks++
	result := w.c.do(ctx, spec)
	if !result.TransportSuccess {
		return 0, "", fmt.Errorf("%s %s: %s", spec.Method, spec.Path, result.ErrorMessage)
	}
	response, ok := result.Payload.(api.HTTPResponse)
	if !ok {
		return 0, "", fmt.Errorf("%s %s: no HTTP response", spec.Method, spec.Path)
	}
	if response.StatusCode != http.StatusOK {
		return response.StatusCode, "", nil
	}
	message, ok := decodeMessage(response.Body)
	if !ok {
		return 0, "", fmt.Errorf("%s %s: response %q is not a message", spec.Method, spec.Path, response.Body)
	}
	return http.StatusOK, message, nil
}

// write sends a write that must be acknowledged with message and returns the
// acknowledgement time.
func (w *writeChecks) write(ctx context.Context, spec api.HTTPRequestSpec, message string) (time.Time, error) {
	status, got, err := w.post(ctx, spec)
	if err != nil {
		return time.Time{}, err
	}
	if status != http.StatusOK || got != message {
		return time.Time{}, fmt.Errorf("%s %s %v: HTTP %d %q, want 200 %q", spec.Method, spec.Path, spec.Query, status, got, message)
	}
	return time.Now(), nil
}

// expectStatus sends a request that must fail with status.
func (w *writeChecks) expectStatus(ctx context.Context, spec api.HTTPRequestSpec, status int) error {
	got, _, err := w.post(ctx, spec)
	if err != nil {
		return err
	}
	if got != status {
		return fmt.Errorf("%s %s %v: HTTP %d, want %d", spec.Method, spec.Path, spec.Query, got, status)
	}
	return nil
}

func waitPastBound(ctx context.Context, acked time.Time) error {
	delay := time.Until(acked.Add(stalenessBound + boundMargin))
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func adminRequest(path string, query map[string]string) api.HTTPRequestSpec {
	return api.HTTPRequestSpec{Method: http.MethodPost, Path: path, Query: query}
}

func rateWrite(key hotelsupport.PlanKey, prices hotelsupport.Prices) api.HTTPRequestSpec {
	format := func(value float64) string { return strconv.FormatFloat(value, 'f', -1, 64) }
	return adminRequest("/admin/rate", map[string]string{
		"hotelId": key.HotelID, "code": key.Code, "bookableRate": format(prices.Bookable),
		"totalRate": format(prices.Total), "totalRateInclusive": format(prices.Inclusive),
	})
}

func capacityWrite(hotelID string, rooms int) api.HTTPRequestSpec {
	return adminRequest("/admin/capacity", map[string]string{"hotelId": hotelID, "rooms": strconv.Itoa(rooms)})
}

// rates reads GET /rates and returns the prices by plan code, checking the
// plans' other fields against the model.
func (w *writeChecks) rates(ctx context.Context, hotelID string) (map[string]hotelsupport.Prices, error) {
	w.checks++
	result := w.c.request(ctx, "/rates", map[string]string{"hotelId": hotelID})
	response, err := httpcheck.Response(result, http.StatusOK)
	if err != nil {
		return nil, fmt.Errorf("GET /rates: %w", err)
	}
	var body struct {
		HotelID   string `json:"hotelId"`
		RatePlans []struct {
			Code     string `json:"code"`
			InDate   string `json:"inDate"`
			OutDate  string `json:"outDate"`
			RoomType struct {
				Code               string  `json:"code"`
				RoomDescription    string  `json:"roomDescription"`
				BookableRate       float64 `json:"bookableRate"`
				TotalRate          float64 `json:"totalRate"`
				TotalRateInclusive float64 `json:"totalRateInclusive"`
			} `json:"roomType"`
		} `json:"ratePlans"`
	}
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		return nil, fmt.Errorf("GET /rates?hotelId=%s: %w", hotelID, err)
	}
	if body.HotelID != hotelID {
		return nil, fmt.Errorf("GET /rates?hotelId=%s answered hotel %q", hotelID, body.HotelID)
	}
	want := w.state.Plans(hotelID)
	if len(body.RatePlans) != len(want) {
		return nil, fmt.Errorf("hotel %s has %d rate plans, want %d", hotelID, len(body.RatePlans), len(want))
	}
	prices := map[string]hotelsupport.Prices{}
	for _, plan := range body.RatePlans {
		var seeded *dataset.RatePlan
		for index := range want {
			if want[index].Code == plan.Code {
				seeded = &want[index]
			}
		}
		if seeded == nil || prices[plan.Code] != (hotelsupport.Prices{}) {
			return nil, fmt.Errorf("hotel %s lists unexpected or repeated rate plan %q", hotelID, plan.Code)
		}
		if plan.InDate != seeded.InDate || plan.OutDate != seeded.OutDate ||
			plan.RoomType.Code != seeded.RoomCode || plan.RoomType.RoomDescription != seeded.RoomDescription {
			return nil, fmt.Errorf("hotel %s plan %s: %+v does not match the catalog", hotelID, plan.Code, plan)
		}
		prices[plan.Code] = hotelsupport.Prices{
			Bookable: plan.RoomType.BookableRate, Total: plan.RoomType.TotalRate, Inclusive: plan.RoomType.TotalRateInclusive,
		}
	}
	return prices, nil
}

// pickHotels draws n distinct searchable seeded hotels that no earlier write
// check used.
func (w *writeChecks) pickHotels(n int, used map[string]bool) []string {
	ids := sortedRateBackedIDs()
	var picked []string
	for len(picked) < n {
		id := ids[w.random.Intn(len(ids))]
		if !used[id] {
			used[id] = true
			picked = append(picked, id)
		}
	}
	return picked
}

// verifyRateWrites: a plan's new prices are served by GET /rates after the
// bound, a second write replaces the first, and reads in between show an
// acknowledged or the previous value.
func (w *writeChecks) verifyRateWrites(ctx context.Context, used map[string]bool) error {
	type pending struct {
		key hotelsupport.PlanKey
		new hotelsupport.Prices
	}
	var writes []pending
	var acked time.Time
	for round, id := range w.pickHotels(3, used) {
		plans := w.state.Plans(id)
		plan := plans[w.random.Intn(len(plans))]
		key := hotelsupport.PlanKey{HotelID: id, Code: plan.Code}
		// Read first, so a cache holds the old value.
		if prices, err := w.rates(ctx, id); err != nil {
			return err
		} else if prices[plan.Code] != pricesOf(plan) {
			return fmt.Errorf("hotel %s plan %s before any write: %+v, want %+v", id, plan.Code, prices[plan.Code], pricesOf(plan))
		}
		updates := 1 + round%2 // the second hotel's plan is written twice
		old := pricesOf(plan)
		// Every value acknowledged less than StalenessBound ago may show,
		// and so may the value before them.
		allowed := []hotelsupport.Prices{old}
		for update := 0; update < updates; update++ {
			total := math.Round(old.Total*(1.05+0.1*w.random.Float64())*100) / 100
			next := hotelsupport.Prices{Bookable: total, Total: total, Inclusive: math.Round(total*1.1*100) / 100}
			var err error
			if acked, err = w.write(ctx, rateWrite(key, next), rateUpdated); err != nil {
				return err
			}
			allowed = append(allowed, next)
			if prices, err := w.rates(ctx, id); err != nil {
				return err
			} else if got := prices[plan.Code]; !slices.Contains(allowed, got) {
				return fmt.Errorf("hotel %s plan %s right after a write: %+v, not one of %+v", id, plan.Code, got, allowed)
			}
			if err := w.state.SetPrices(id, plan.Code, next); err != nil {
				return err
			}
			old = next
		}
		writes = append(writes, pending{key: key, new: old})
		w.ratePlans = append(w.ratePlans, key)
	}
	if err := waitPastBound(ctx, acked); err != nil {
		return err
	}
	for _, item := range writes {
		for read := 0; read < repeatedReads; read++ {
			prices, err := w.rates(ctx, item.key.HotelID)
			if err != nil {
				return err
			}
			if got := prices[item.key.Code]; got != item.new {
				return fmt.Errorf("hotel %s plan %s read %d, %v after the acknowledgement: %+v, want %+v",
					item.key.HotelID, item.key.Code, read+1, stalenessBound, got, item.new)
			}
		}
	}
	return nil
}

func pricesOf(plan dataset.RatePlan) hotelsupport.Prices {
	return hotelsupport.Prices{Bookable: plan.BookableRate, Total: plan.TotalRate, Inclusive: plan.TotalRateInclusive}
}

// searchAt is a two-night search at a hotel's location, on nights of the
// write checks' namespace that no check reserves.
func (w *writeChecks) searchAt(hotel *dataset.Hotel) map[string]string {
	start := reservationDate(w.seed).AddDate(0, 0, writesOffset+4+w.random.Intn(3))
	return map[string]string{
		"inDate": start.Format(time.DateOnly), "outDate": start.AddDate(0, 0, 2).Format(time.DateOnly),
		"lat": strconv.FormatFloat(hotel.Lat, 'f', -1, 64), "lon": strconv.FormatFloat(hotel.Lon, 'f', -1, 64),
	}
}

func distanceQuery(hotel *dataset.Hotel) map[string]string {
	return map[string]string{
		"require": "dis",
		"lat":     strconv.FormatFloat(hotel.Lat, 'f', -1, 64), "lon": strconv.FormatFloat(hotel.Lon, 'f', -1, 64),
	}
}

// expectedSearch is the model's search answer: geo's candidates over the
// current hotels with a room free (nobody reserves on searchAt's nights, so a
// hotel is full there only at capacity 0).
func (w *writeChecks) expectedSearch(query map[string]string) ([]string, error) {
	ids, exact, err := w.state.SearchCandidates(query["lat"], query["lon"])
	if err != nil {
		return nil, err
	}
	if !exact {
		return nil, fmt.Errorf("search location %s,%s ranks two hotels equally", query["lat"], query["lon"])
	}
	available := ids[:0:0]
	for _, id := range ids {
		if rooms, _ := w.state.Capacity(id); rooms > 0 {
			available = append(available, id)
		}
	}
	return available, nil
}

// readGeoJSON reads a GeoJSON route and checks every feature against the
// model's current profiles.
func (w *writeChecks) readGeoJSON(ctx context.Context, path string, query map[string]string) (map[string]feature, error) {
	w.checks++
	features, err := w.c.geoJSON(ctx, path, query)
	if err != nil {
		return nil, err
	}
	return features, nil
}

// profileMismatch returns the first feature whose profile is not allowed.
func (w *writeChecks) profileMismatch(features map[string]feature, allowed func(id string) []hotelsupport.Profile) error {
	ids := make([]string, 0, len(features))
	for id := range features {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		item := features[id]
		hotel, ok := w.state.Hotel(id)
		if !ok {
			return fmt.Errorf("unknown hotel %q", id)
		}
		matched := false
		for _, profile := range allowed(id) {
			matched = matched || (item.name == profile.Name && item.phone == profile.Phone)
		}
		if !matched {
			return fmt.Errorf("hotel %s shows name %q phone %q, allowed %v", id, item.name, item.phone, allowed(id))
		}
		wantLon, wantLat := hotelsupport.EncodedCoordinates(hotel)
		if item.lon != wantLon || item.lat != wantLat {
			return fmt.Errorf("hotel %s coordinates [%v %v], want [%v %v]", id, item.lon, item.lat, wantLon, wantLat)
		}
	}
	return nil
}

func (w *writeChecks) current(id string) []hotelsupport.Profile {
	profile, _ := w.state.Profile(id)
	return []hotelsupport.Profile{profile}
}

// verifyProfileWrites: new phones (and one new name) appear in search and
// in the distance recommendation after the bound.
func (w *writeChecks) verifyProfileWrites(ctx context.Context, used map[string]bool) error {
	ids := w.pickHotels(3, used)
	previous := map[string]hotelsupport.Profile{}
	var acked time.Time
	for index, id := range ids {
		hotel, _ := w.state.Hotel(id)
		// Read first, so caches hold the old profile.
		if _, err := w.readGeoJSON(ctx, "/hotels", w.searchAt(hotel)); err != nil {
			return err
		}
		if _, err := w.readGeoJSON(ctx, "/recommendations", distanceQuery(hotel)); err != nil {
			return err
		}
		previous[id], _ = w.state.Profile(id)
		query := map[string]string{"hotelId": id, "phone": dataset.NewPhone(hotel, w.random.Uint64())}
		if index == 0 {
			query["name"] = dataset.NewName(hotel, w.random.Uint64())
		}
		var err error
		if acked, err = w.write(ctx, adminRequest("/admin/profile", query), profileUpdated); err != nil {
			return err
		}
		if err := w.state.SetProfile(id, query["name"], query["phone"]); err != nil {
			return err
		}
		w.profiles = append(w.profiles, id)
		features, err := w.readGeoJSON(ctx, "/recommendations", distanceQuery(hotel))
		if err != nil {
			return err
		}
		if err := w.profileMismatch(features, func(other string) []hotelsupport.Profile {
			// Writes of this check acknowledged less than StalenessBound
			// ago may or may not show.
			if before, written := previous[other]; written {
				now, _ := w.state.Profile(other)
				return []hotelsupport.Profile{before, now}
			}
			return w.current(other)
		}); err != nil {
			return fmt.Errorf("distance recommendation right after a profile write: %w", err)
		}
	}
	if err := waitPastBound(ctx, acked); err != nil {
		return err
	}
	for _, id := range ids {
		hotel, _ := w.state.Hotel(id)
		for read := 0; read < repeatedReads; read++ {
			query := w.searchAt(hotel)
			want, err := w.expectedSearch(query)
			if err != nil {
				return err
			}
			features, err := w.readGeoJSON(ctx, "/hotels", query)
			if err != nil {
				return err
			}
			if err := exactIDs(features, want...); err != nil {
				return fmt.Errorf("search at hotel %s after a profile write: %w", id, err)
			}
			if err := w.profileMismatch(features, w.current); err != nil {
				return fmt.Errorf("search read %d, %v after a profile write: %w", read+1, stalenessBound, err)
			}
			features, err = w.readGeoJSON(ctx, "/recommendations", distanceQuery(hotel))
			if err != nil {
				return err
			}
			if err := w.profileMismatch(features, w.current); err != nil {
				return fmt.Errorf("distance recommendation read %d, %v after a profile write: %w", read+1, stalenessBound, err)
			}
		}
	}
	return nil
}

func (w *writeChecks) reserve(ctx context.Context, hotelID string, night [2]string, rooms int, label string) (bool, error) {
	username, password := hotelsupport.User(1)
	w.checks++
	message, err := w.c.message(ctx, "/reservation", reservationActionQuery(hotelID, night, rooms, label, username, password))
	if err != nil {
		return false, err
	}
	switch message {
	case reservationSuccess:
		w.reserved[hotelID+"/"+night[0]] += rooms
		return true, nil
	case reservationFailure:
		return false, nil
	default:
		return false, fmt.Errorf("reservation answered %q", message)
	}
}

// expectReserve reserves and requires the outcome the model predicts.
func (w *writeChecks) expectReserve(ctx context.Context, hotelID string, night [2]string, rooms int, label string) error {
	capacity, _ := w.state.Capacity(hotelID)
	want := w.reserved[hotelID+"/"+night[0]]+rooms <= capacity
	got, err := w.reserve(ctx, hotelID, night, rooms, label)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("reservation of %d rooms of hotel %s on %s (capacity %d, %d reserved): accepted=%v, want %v",
			rooms, hotelID, night[0], capacity, w.reserved[hotelID+"/"+night[0]], got, want)
	}
	return nil
}

// probeRooms reserves one room repeatedReads times (or times times when
// positive), each with the model's outcome, so the probes reach every
// replica of the reservation service.
func (w *writeChecks) probeRooms(ctx context.Context, hotelID string, night [2]string, times int, label string) error {
	if times <= 0 {
		times = repeatedReads
	}
	for probe := 0; probe < times; probe++ {
		if err := w.expectReserve(ctx, hotelID, night, 1, fmt.Sprintf("%s-%d", label, probe)); err != nil {
			return err
		}
	}
	return nil
}

func (w *writeChecks) maxReserved(hotelID string) int {
	most := 0
	for key, rooms := range w.reserved {
		if len(key) > len(hotelID) && key[:len(hotelID)+1] == hotelID+"/" && rooms > most {
			most = rooms
		}
	}
	return most
}

// setCapacity writes a capacity and requires the model's acceptance.
func (w *writeChecks) setCapacity(ctx context.Context, hotelID string, rooms int) (time.Time, error) {
	if rooms < w.maxReserved(hotelID) {
		return time.Time{}, w.expectStatus(ctx, capacityWrite(hotelID, rooms), http.StatusConflict)
	}
	acked, err := w.write(ctx, capacityWrite(hotelID, rooms), capacityUpdated)
	if err != nil {
		return acked, err
	}
	return acked, w.state.SetCapacity(hotelID, rooms)
}

// verifyCapacityWrites: a capacity below a night's reserved rooms is
// rejected and changes nothing; an accepted decrease or increase applies to
// the next reservation at once, and to search within the bound.
func (w *writeChecks) verifyCapacityWrites(ctx context.Context, used map[string]bool) error {
	ids := w.pickHotels(1, used)
	night := namespacedNight(w.seed, writesOffset)
	other := namespacedNight(w.seed, writesOffset+1)
	step := func(err error, what string) error {
		if err != nil {
			return fmt.Errorf("capacity writes, %s: %w", what, err)
		}
		return nil
	}

	// Hotel A: half full; decreasing below that is rejected and leaves the
	// capacity; decreasing to the reserved rooms fills it at once; an
	// increase frees rooms at once.
	a := ids[0]
	capacity, _ := w.state.Capacity(a)
	half := max(1, capacity/2)
	if err := w.expectReserve(ctx, a, night, half, "capacity-half"); err != nil {
		return step(err, "half")
	}
	if _, err := w.setCapacity(ctx, a, half-1); err != nil {
		return step(err, "below the reserved rooms")
	}
	if err := w.expectReserve(ctx, a, other, capacity, "capacity-unchanged"); err != nil {
		return step(err, "a rejected change kept the capacity")
	}
	if _, err := w.setCapacity(ctx, a, capacity-1); err != nil { // other night holds capacity: rejected
		return step(err, "below another night's reserved rooms")
	}
	if _, err := w.setCapacity(ctx, a, capacity); err != nil {
		return step(err, "equal to the reserved rooms")
	}
	if _, err := w.setCapacity(ctx, a, capacity+3); err != nil {
		return step(err, "increase")
	}
	if err := w.probeRooms(ctx, a, other, 3, "capacity-increased"); err != nil {
		return step(err, "an increase applies at once")
	}
	if err := w.probeRooms(ctx, a, other, 0, "capacity-increased-over"); err != nil {
		return step(err, "past the increased capacity")
	}
	w.capacity.increased = a

	// Seeded hotels may hold reservations of the earlier checks on other
	// nights, so the decreases that must be accepted use fresh hotels.
	// Hotel B: a decrease to the reserved rooms applies at once.
	b, err := w.addFresh(ctx)
	if err != nil {
		return step(err, "adding a hotel")
	}
	if err := w.expectReserve(ctx, b.ID, night, b.Capacity-1, "decrease-setup"); err != nil {
		return step(err, "setup")
	}
	if _, err := w.setCapacity(ctx, b.ID, b.Capacity-1); err != nil {
		return step(err, "decrease to the reserved rooms")
	}
	if err := w.probeRooms(ctx, b.ID, night, 0, "decrease-over"); err != nil {
		return step(err, "a decrease applies at once")
	}
	if err := w.expectReserve(ctx, b.ID, other, b.Capacity-1, "decrease-other-night"); err != nil {
		return step(err, "a decrease leaves other nights the new capacity")
	}
	if err := w.probeRooms(ctx, b.ID, other, 0, "decrease-other-night-over"); err != nil {
		return step(err, "a decrease leaves other nights the new capacity")
	}
	w.capacity.decreased, w.capacity.full = b.ID, night

	// Hotel C: capacity 0 closes it to reservations at once and hides it
	// from search within the bound; restoring it shows it again.
	c, err := w.addFresh(ctx)
	if err != nil {
		return step(err, "adding a hotel")
	}
	acked, err := w.setCapacity(ctx, c.ID, 0)
	if err != nil {
		return step(err, "capacity 0")
	}
	if err := w.probeRooms(ctx, c.ID, other, 0, "closed-hotel"); err != nil {
		return step(err, "a closed hotel")
	}
	if err := w.searchesMatchModel(ctx, c, acked); err != nil {
		return step(err, "search after capacity 0")
	}
	if acked, err = w.setCapacity(ctx, c.ID, c.Capacity); err != nil {
		return step(err, "reopen")
	}
	if err := w.expectReserve(ctx, c.ID, other, 1, "reopened-hotel"); err != nil {
		return step(err, "a reopened hotel")
	}
	if err := w.searchesMatchModel(ctx, c, acked); err != nil {
		return step(err, "search after reopening")
	}
	return nil
}

// addFresh adds a hotel in a new sandbox cell, so it has no reservations
// and no neighbours.
func (w *writeChecks) addFresh(ctx context.Context) (*dataset.Hotel, error) {
	hotel := w.newHotel(len(w.added) + 1)
	if _, err := w.write(ctx, addHotelRequest(&hotel), hotelAdded); err != nil {
		return nil, err
	}
	if err := w.state.Add(hotel); err != nil {
		return nil, err
	}
	w.added = append(w.added, &hotel)
	return &hotel, nil
}

// searchesMatchModel waits past the bound after acked and requires
// repeatedReads searches at the hotel's location to answer the model.
func (w *writeChecks) searchesMatchModel(ctx context.Context, hotel *dataset.Hotel, acked time.Time) error {
	if err := waitPastBound(ctx, acked); err != nil {
		return err
	}
	for read := 0; read < repeatedReads; read++ {
		query := w.searchAt(hotel)
		want, err := w.expectedSearch(query)
		if err != nil {
			return err
		}
		features, err := w.readGeoJSON(ctx, "/hotels", query)
		if err != nil {
			return err
		}
		if err := exactIDs(features, want...); err != nil {
			return fmt.Errorf("search read %d at hotel %s: %w", read+1, hotel.ID, err)
		}
		if err := w.profileMismatch(features, w.current); err != nil {
			return err
		}
	}
	return nil
}

// newHotel generates a hotel for the oracle's sandbox.
func (w *writeChecks) newHotel(n int) dataset.Hotel {
	cell := uint64(w.random.Int63())
	lat, lon := dataset.SandboxPoint(cell, w.random.Uint64())
	id := dataset.AddedHotelID(uint64(w.random.Int63())>>16, n)
	return dataset.AddedHotel(id, w.random.Uint64(), lat, lon)
}

// verifyHotelAdditions: an added hotel (best rated and cheapest of all) is
// served by search, the three recommendations, /rates and reservations after
// the bound; a second hotel next to it joins the search; a taken ID and an
// invalid body are rejected.
func (w *writeChecks) verifyHotelAdditions(ctx context.Context) error {
	reference := w.state.Reference()
	first := w.newHotel(len(w.added) + 1)
	first.Rating = math.Round((reference.MaxRating()+0.01)*100) / 100
	first.Price = math.Round((reference.MinPrice()-0.01)*100) / 100
	search := w.searchAt(&first)
	if want, err := w.expectedSearch(search); err != nil {
		return err
	} else if features, err := w.readGeoJSON(ctx, "/hotels", search); err != nil {
		return err
	} else if err := exactIDs(features, want...); err != nil {
		return fmt.Errorf("search where a hotel will be added: %w", err)
	}
	acked, err := w.write(ctx, addHotelRequest(&first), hotelAdded)
	if err != nil {
		return err
	}
	if err := w.state.Add(first); err != nil {
		return err
	}
	w.added = append(w.added, &first)
	if err := w.expectStatus(ctx, addHotelRequest(&first), http.StatusConflict); err != nil {
		return fmt.Errorf("adding a taken ID: %w", err)
	}
	invalid := first
	invalid.ID = "bad_id"
	if err := w.expectStatus(ctx, addHotelRequest(&invalid), http.StatusBadRequest); err != nil {
		return fmt.Errorf("adding an invalid ID: %w", err)
	}
	if err := waitPastBound(ctx, acked); err != nil {
		return err
	}
	if err := w.additionsServed(ctx, &first, true); err != nil {
		return err
	}
	night := namespacedNight(w.seed, writesOffset+2)
	if err := w.expectReserve(ctx, first.ID, night, first.Capacity, "added-fill"); err != nil {
		return fmt.Errorf("added hotel: %w", err)
	}
	if err := w.probeRooms(ctx, first.ID, night, 0, "added-over"); err != nil {
		return fmt.Errorf("added hotel: %w", err)
	}

	second := w.newHotel(len(w.added) + 1)
	second.Lat, second.Lon = first.Lat+0.0021, first.Lon-0.0013
	if acked, err = w.write(ctx, addHotelRequest(&second), hotelAdded); err != nil {
		return err
	}
	if err := w.state.Add(second); err != nil {
		return err
	}
	w.added = append(w.added, &second)
	if err := waitPastBound(ctx, acked); err != nil {
		return err
	}
	return w.additionsServed(ctx, &second, false)
}

// additionsServed requires repeatedReads answers of every read path that
// can return the hotel to match the model.
func (w *writeChecks) additionsServed(ctx context.Context, hotel *dataset.Hotel, recommendations bool) error {
	for read := 0; read < repeatedReads; read++ {
		query := w.searchAt(hotel)
		want, err := w.expectedSearch(query)
		if err != nil {
			return err
		}
		features, err := w.readGeoJSON(ctx, "/hotels", query)
		if err != nil {
			return err
		}
		if err := exactIDs(features, want...); err != nil {
			return fmt.Errorf("search read %d at added hotel %s: %w", read+1, hotel.ID, err)
		}
		if err := w.profileMismatch(features, w.current); err != nil {
			return fmt.Errorf("search at added hotel %s: %w", hotel.ID, err)
		}
		requires := map[string][]string{"dis": w.state.NearestHotels(hotel.Lat, hotel.Lon)}
		if recommendations {
			requires["rate"], requires["price"] = w.state.BestRated(), w.state.Cheapest()
		}
		for _, require := range []string{"dis", "rate", "price"} {
			ids, ok := requires[require]
			if !ok {
				continue
			}
			query := distanceQuery(hotel)
			query["require"] = require
			features, err := w.readGeoJSON(ctx, "/recommendations", query)
			if err != nil {
				return err
			}
			if err := exactIDs(features, ids...); err != nil {
				return fmt.Errorf("%s recommendation read %d after adding hotel %s: %w", require, read+1, hotel.ID, err)
			}
			if err := w.profileMismatch(features, w.current); err != nil {
				return err
			}
		}
		prices, err := w.rates(ctx, hotel.ID)
		if err != nil {
			return fmt.Errorf("rates of added hotel %s: %w", hotel.ID, err)
		}
		for _, plan := range hotel.RatePlans {
			if prices[plan.Code] != pricesOf(plan) {
				return fmt.Errorf("added hotel %s plan %s: %+v, want %+v", hotel.ID, plan.Code, prices[plan.Code], pricesOf(plan))
			}
		}
	}
	return nil
}

// addHotelRequest is POST /admin/hotel for a generated hotel.
func addHotelRequest(hotel *dataset.Hotel) api.HTTPRequestSpec {
	type roomType struct {
		Code               string  `json:"code"`
		RoomDescription    string  `json:"roomDescription"`
		BookableRate       float64 `json:"bookableRate"`
		TotalRate          float64 `json:"totalRate"`
		TotalRateInclusive float64 `json:"totalRateInclusive"`
	}
	type ratePlan struct {
		Code     string   `json:"code"`
		InDate   string   `json:"inDate"`
		OutDate  string   `json:"outDate"`
		RoomType roomType `json:"roomType"`
	}
	body := map[string]any{
		"hotelId": hotel.ID, "name": hotel.Name, "phoneNumber": hotel.Phone, "description": hotel.Description,
		"address": map[string]string{
			"streetNumber": hotel.Address.StreetNumber, "streetName": hotel.Address.StreetName,
			"city": hotel.Address.City, "state": hotel.Address.State, "country": hotel.Address.Country,
			"postalCode": hotel.Address.PostalCode,
		},
		"lat": hotel.Lat, "lon": hotel.Lon, "rating": hotel.Rating, "price": hotel.Price, "rooms": hotel.Capacity,
	}
	plans := []ratePlan{}
	for _, plan := range hotel.RatePlans {
		plans = append(plans, ratePlan{Code: plan.Code, InDate: plan.InDate, OutDate: plan.OutDate, RoomType: roomType{
			Code: plan.RoomCode, RoomDescription: plan.RoomDescription, BookableRate: plan.BookableRate,
			TotalRate: plan.TotalRate, TotalRateInclusive: plan.TotalRateInclusive,
		}})
	}
	body["ratePlans"] = plans
	encoded, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return api.HTTPRequestSpec{
		Method: http.MethodPost, Path: "/admin/hotel",
		Headers: map[string]string{"Content-Type": "application/json"}, Body: string(encoded),
	}
}

// verifyWriteValidation: malformed or unknown writes are rejected with the
// stated statuses and change nothing.
func (w *writeChecks) verifyWriteValidation(ctx context.Context) error {
	reference := w.state.Reference()
	hotel := &reference.Data.Hotels[w.random.Intn(len(reference.Data.Hotels))]
	key := hotelsupport.PlanKey{HotelID: hotel.ID, Code: hotel.RatePlans[0].Code}
	prices := pricesOf(hotel.RatePlans[0])
	unknown := strconv.Itoa(1_000_000 + w.random.Intn(1_000_000))
	cases := []struct {
		spec   api.HTTPRequestSpec
		status int
	}{
		{api.HTTPRequestSpec{Method: http.MethodGet, Path: "/admin/rate", Query: rateWrite(key, prices).Query}, http.StatusMethodNotAllowed},
		{adminRequest("/admin/rate", without(rateWrite(key, prices).Query, "totalRate")), http.StatusBadRequest},
		{adminRequest("/admin/rate", with(rateWrite(key, prices).Query, "totalRate", "-1")), http.StatusBadRequest},
		{rateWrite(hotelsupport.PlanKey{HotelID: hotel.ID, Code: "NOPLAN"}, prices), http.StatusNotFound},
		{rateWrite(hotelsupport.PlanKey{HotelID: unknown, Code: "RACK"}, prices), http.StatusNotFound},
		{adminRequest("/admin/profile", map[string]string{"hotelId": hotel.ID}), http.StatusBadRequest},
		{adminRequest("/admin/profile", map[string]string{"hotelId": unknown, "phone": "+1"}), http.StatusNotFound},
		{adminRequest("/admin/capacity", map[string]string{"hotelId": hotel.ID, "rooms": "-1"}), http.StatusBadRequest},
		{adminRequest("/admin/capacity", map[string]string{"hotelId": hotel.ID}), http.StatusBadRequest},
		{capacityWrite(unknown, 3), http.StatusNotFound},
		{api.HTTPRequestSpec{Method: http.MethodPost, Path: "/admin/hotel", Body: "{"}, http.StatusBadRequest},
	}
	w.random.Shuffle(len(cases), func(i, j int) { cases[i], cases[j] = cases[j], cases[i] })
	for _, item := range cases {
		if err := w.expectStatus(ctx, item.spec, item.status); err != nil {
			return err
		}
	}
	w.checks++
	if _, err := httpcheck.Response(w.c.request(ctx, "/rates", map[string]string{}), http.StatusBadRequest); err != nil {
		return fmt.Errorf("GET /rates without hotelId: %w", err)
	}
	// Nothing changed.
	if got, err := w.rates(ctx, hotel.ID); err != nil {
		return err
	} else if got[key.Code] != prices {
		return fmt.Errorf("a rejected write changed hotel %s plan %s to %+v", hotel.ID, key.Code, got[key.Code])
	}
	return nil
}

// verifyDurableWrites restarts the stateless services and the reservation
// cache after the writes and requires every acknowledged write to remain:
// no restarted cache or index serves an old value, and capacities hold.
func (w *writeChecks) verifyDurableWrites(ctx context.Context, crash, start func(context.Context) error) error {
	if err := crash(ctx); err != nil {
		return fmt.Errorf("crash: %w", err)
	}
	if err := start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	for read := 0; read < repeatedReads; read++ {
		for _, key := range w.ratePlans {
			prices, err := w.rates(ctx, key.HotelID)
			if err != nil {
				return err
			}
			for _, plan := range w.state.Plans(key.HotelID) {
				if plan.Code == key.Code && prices[key.Code] != pricesOf(plan) {
					return fmt.Errorf("after a restart hotel %s plan %s is %+v, want %+v", key.HotelID, key.Code, prices[key.Code], pricesOf(plan))
				}
			}
		}
		for _, require := range []string{"rate", "price"} {
			features, err := w.readGeoJSON(ctx, "/recommendations", map[string]string{"require": require, "lat": "0", "lon": "0"})
			if err != nil {
				return err
			}
			want := w.state.BestRated()
			if require == "price" {
				want = w.state.Cheapest()
			}
			if err := exactIDs(features, want...); err != nil {
				return fmt.Errorf("%s recommendation after a restart: %w", require, err)
			}
		}
	}
	for _, id := range append(append([]string(nil), w.profiles...), addedIDs(w.added)...) {
		hotel, _ := w.state.Hotel(id)
		if err := w.searchesMatchModel(ctx, hotel, time.Time{}); err != nil {
			return fmt.Errorf("after a restart: %w", err)
		}
	}
	// An increased capacity still admits its new rooms on a fresh night, and
	// a decreased one still refuses the old rooms.
	if increased := w.capacity.increased; increased != "" {
		rooms, _ := w.state.Capacity(increased)
		night := namespacedNight(w.seed, writesOffset+3)
		if err := w.expectReserve(ctx, increased, night, rooms, "durable-increase"); err != nil {
			return fmt.Errorf("increased capacity after a restart: %w", err)
		}
		if err := w.probeRooms(ctx, increased, night, 0, "durable-increase-over"); err != nil {
			return fmt.Errorf("increased capacity after a restart: %w", err)
		}
	}
	if decreased := w.capacity.decreased; decreased != "" {
		if err := w.probeRooms(ctx, decreased, w.capacity.full, 0, "durable-decrease-over"); err != nil {
			return fmt.Errorf("decreased capacity after a restart: %w", err)
		}
	}
	return nil
}

func addedIDs(hotels []*dataset.Hotel) []string {
	ids := make([]string, 0, len(hotels))
	for _, hotel := range hotels {
		ids = append(ids, hotel.ID)
	}
	return ids
}
