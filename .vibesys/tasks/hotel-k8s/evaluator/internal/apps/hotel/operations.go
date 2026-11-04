package hotel

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/api"
)

// Request distributions. Every draw of a sample is a function of
// sample.Random (dataset.Unit with a distinct stream per draw), so a run is
// replayable from its seed. Reservation slots additionally depend on the
// order in which plans are built, as the fixture's ordinal does.
//
//   - Locations (search and recommendations): a city by Zipf rank (exponent
//     1, the dataset's popularity order), then a point around its centre with
//     normal offsets of the city's hotel spread, written with 5 decimals
//     (about 1 m).
//   - Stays: a night count from stayNights (1-7, skewed short) and an arrival
//     uniform over the rate window such that the stay ends inside it.
//   - Logins: a user by Zipf rank (exponent 0.75) over the user list.
//   - Reservations: a hotel by popularity, a city by Zipf rank then a hotel
//     by Zipf rank (exponent 0.75) within the city, outside the quarter of
//     capacity-managed hotels (capacityManaged), on a trial-owned future
//     night; a user as for logins.
//   - Rate reads and admin writes: a hotel by popularity as for
//     reservations (writes.go). Write plans skip items with a write in
//     flight and sandbox reads depend on the additions acknowledged so far,
//     so those draws also depend on timing.
const (
	coordinateDecimals = 5
	// searchAttempts bounds redraws of a location where the geo service's own
	// ranking ties (its answer is then not determined).
	searchAttempts = 8
	// reservationAttempts bounds popularity draws of a hotel not yet used on
	// the current night, before falling back to the city's next hotels.
	reservationAttempts = 16
)

// stayNights[n] is the relative weight of a stay of n+1 nights.
var stayNights = []int{30, 24, 16, 11, 8, 6, 5}

// Mix stream numbers. Each draw uses its own streams; a redraw shifts them by
// attempt*streamStride.
const (
	streamCity        = 1
	streamLatitude    = 2 // 4 streams
	streamLongitude   = 6 // 4 streams
	streamNights      = 10
	streamArrival     = 11
	streamLocale      = 12
	streamUser        = 20
	streamReserveUser = 21
	streamHotelCity   = 30
	streamHotelRank   = 31
	streamStride      = 64
)

func (a *Application) BuildOperation(
	operation api.Operation,
	sample api.Sample,
	prepared any,
) (api.OperationPlan, error) {
	data, ok := prepared.(*fixture)
	if !ok || data == nil {
		return api.OperationPlan{}, fmt.Errorf("Hotel fixture is not prepared")
	}
	switch operation.Name {
	case operationSearch:
		if lat, lon, ok := a.sandboxLocation(sample.Random, data); ok {
			inDate, outDate := stay(sample.Random)
			return sandboxPlan(operation, api.HTTPRequestSpec{Method: http.MethodGet, Path: "/hotels", Query: map[string]string{
				"inDate": inDate, "outDate": outDate, "lat": lat, "lon": lon,
			}}, data), nil
		}
		request, expectedIDs, err := a.searchRequest(sample.Random)
		if err != nil {
			return api.OperationPlan{}, err
		}
		return oneRequestPlan(operation, request, expectedIDs), nil
	case operationRecommendDistance, operationRecommendRate, operationRecommendPrice:
		if operation.Name == operationRecommendDistance {
			if lat, lon, ok := a.sandboxLocation(sample.Random, data); ok {
				return sandboxPlan(operation, api.HTTPRequestSpec{Method: http.MethodGet, Path: "/recommendations", Query: map[string]string{
					"require": "dis", "lat": lat, "lon": lon,
				}}, data), nil
			}
		}
		request, expectedIDs := a.recommendationRequest(operation.Name, sample.Random)
		return oneRequestPlan(operation, request, expectedIDs), nil
	case operationRates:
		return a.ratesPlan(operation, sample), nil
	case operationUpdateRate:
		return a.rateWritePlan(operation, sample)
	case operationUpdateProfile:
		return a.profileWritePlan(operation, sample)
	case operationSetCapacity:
		return a.capacityWritePlan(operation, sample, data)
	case operationAddHotel:
		return a.addHotelPlan(operation, sample, data)
	case operationLoginValid, operationLoginInvalid:
		return a.loginPlan(operation, sample), nil
	case operationReserveCapacity:
		return a.reservationPlan(operation, sample, data)
	default:
		return api.OperationPlan{}, fmt.Errorf("unknown Hotel operation %q", operation.Name)
	}
}

func oneRequestPlan(
	operation api.Operation,
	request api.HTTPRequestSpec,
	expectedIDs []string,
) api.OperationPlan {
	return api.OperationPlan{
		Invocations: []api.Invocation{{
			Target: operation.Target, Operation: operation.Name, Payload: request,
		}},
		State: &operationState{
			kind: operation.Name, expectedIDs: append([]string(nil), expectedIDs...), built: time.Now(),
		},
	}
}

// sandboxPlan is a search or distance recommendation in the trial's
// sandbox, validated against the trial's additions.
func sandboxPlan(operation api.Operation, request api.HTTPRequestSpec, data *fixture) api.OperationPlan {
	return api.OperationPlan{
		Invocations: []api.Invocation{{Target: operation.Target, Operation: operation.Name, Payload: request}},
		State:       &operationState{kind: operation.Name, built: time.Now(), sandbox: data},
	}
}

// location draws a point near a city centre and formats it for the wire.
func (a *Application) location(random uint64, offset uint64) (lat, lon string) {
	city := &a.reference.Data.Cities[a.cities.Rank(dataset.Unit(random, offset+streamCity))]
	dy := city.SpreadKm * dataset.Normal(random, offset+streamLatitude)
	dx := city.SpreadKm * dataset.Normal(random, offset+streamLongitude)
	return formatCoordinate(city.Lat + dy/111.32), formatCoordinate(city.Lon + dx/city.KmPerDegreeLon)
}

// stay draws the arrival and departure dates of a stay in the rate window.
func stay(random uint64) (inDate, outDate string) {
	nights := 1 + pick(stayNights, dataset.Unit(random, streamNights))
	arrivals := dataset.RateWindowDays - nights + 1
	arrival := int(dataset.Unit(random, streamArrival) * float64(arrivals))
	start := dataset.RateWindowStart.AddDate(0, 0, arrival)
	return start.Format(time.DateOnly), start.AddDate(0, 0, nights).Format(time.DateOnly)
}

func (a *Application) searchRequest(random uint64) (api.HTTPRequestSpec, []string, error) {
	inDate, outDate := stay(random)
	for attempt := uint64(0); attempt < searchAttempts; attempt++ {
		lat, lon := a.location(random, attempt*streamStride)
		// No canonical reservation falls in the rate window, so every hotel
		// is available and search answers geo's nearest rate-backed hotels.
		expected, exact, err := a.reference.SearchCandidates(lat, lon)
		if err != nil {
			return api.HTTPRequestSpec{}, nil, err
		}
		if !exact {
			continue
		}
		query := map[string]string{"inDate": inDate, "outDate": outDate, "lat": lat, "lon": lon}
		if dataset.Unit(random, streamLocale) < 0.5 {
			query["locale"] = "en"
		}
		return api.HTTPRequestSpec{Method: http.MethodGet, Path: "/hotels", Query: query}, expected, nil
	}
	return api.HTTPRequestSpec{}, nil, fmt.Errorf("no unambiguous search location for sample %#x", random)
}

func (a *Application) recommendationRequest(operation string, random uint64) (api.HTTPRequestSpec, []string) {
	require := map[string]string{
		operationRecommendDistance: "dis",
		operationRecommendRate:     "rate",
		operationRecommendPrice:    "price",
	}[operation]
	lat, lon := a.location(random, 0)
	query := map[string]string{"lat": lat, "lon": lon, "require": require}
	if dataset.Unit(random, streamLocale) < 0.5 {
		query["locale"] = "en"
	}
	var expected []string
	switch operation {
	case operationRecommendDistance:
		// From the values the frontend parses, not from pre-format floats.
		expected = a.reference.NearestHotels(mustParseCoordinate(lat), mustParseCoordinate(lon))
	case operationRecommendRate:
		expected = a.reference.BestRated()
	default:
		expected = a.reference.Cheapest()
	}
	return api.HTTPRequestSpec{Method: http.MethodGet, Path: "/recommendations", Query: query}, expected
}

func formatCoordinate(value float64) string {
	return strconv.FormatFloat(value, 'f', coordinateDecimals, 64)
}

func mustParseCoordinate(value string) float64 {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		panic(err)
	}
	return parsed
}

// pick maps u in [0, 1) to an index with probability proportional to weights.
func pick(weights []int, u float64) int {
	total := 0
	for _, weight := range weights {
		total += weight
	}
	draw := int(u * float64(total))
	for index, weight := range weights {
		if draw < weight {
			return index
		}
		draw -= weight
	}
	return len(weights) - 1
}

func (a *Application) user(random uint64, stream uint64) (string, string) {
	return a.reference.User(a.users.Rank(dataset.Unit(random, stream)))
}

func (a *Application) loginPlan(operation api.Operation, sample api.Sample) api.OperationPlan {
	username, password := a.user(sample.Random, streamUser)
	if operation.Name == operationLoginInvalid {
		password += "-invalid"
	}
	return oneRequestPlan(operation, api.HTTPRequestSpec{
		Method: http.MethodGet,
		Path:   "/user",
		Query:  map[string]string{"username": username, "password": password},
	}, nil)
}

// popularHotel draws a hotel index by popularity: a city by Zipf rank, then a
// hotel by Zipf rank within it.
func (a *Application) popularHotel(random uint64, attempt uint64) (city, rank int) {
	city = a.cities.Rank(dataset.Unit(random, attempt*streamStride+streamHotelCity))
	rank = a.cityHotels[city].Rank(dataset.Unit(random, attempt*streamStride+streamHotelRank))
	return city, rank
}

func (a *Application) reservationPlan(
	operation api.Operation,
	sample api.Sample,
	data *fixture,
) (api.OperationPlan, error) {
	if sample.Counter < 0 {
		return api.OperationPlan{}, fmt.Errorf("Hotel sample counter must be non-negative")
	}
	a.reservationMu.Lock()
	release := func() { a.reservationMu.Unlock() }

	// Sample.Counter restarts between warmup and measurement. The fixture-owned
	// ordinal persists across both phases. reservationsPerNight consecutive
	// ordinals share a night of the trial's block, each on a different hotel,
	// so every reservation finds its hotel's full capacity free.
	ordinal := data.nextReservation.Add(1) - 1
	if ordinal >= reservationSlots {
		release()
		return api.OperationPlan{}, fmt.Errorf(
			"Hotel trial exceeded its %d unique hotel/night reservation slots", reservationSlots,
		)
	}
	night := ordinal / reservationsPerNight
	if data.used == nil || data.night != night {
		data.night, data.used = night, make(map[int]struct{}, reservationsPerNight)
	}
	hotelIndex := -1
	for attempt := uint64(0); attempt < reservationAttempts && hotelIndex < 0; attempt++ {
		city, rank := a.popularHotel(sample.Random, attempt)
		if candidate := a.reference.Data.Cities[city].Hotels[rank]; !data.isUsed(candidate) && !capacityManaged(candidate) {
			hotelIndex = candidate
		}
	}
	if hotelIndex < 0 {
		// The popular hotels of this night are taken: the drawn city's next
		// hotels in popularity order (a night holds fewer reservations than
		// any city has hotels).
		city, rank := a.popularHotel(sample.Random, 0)
		hotels := a.reference.Data.Cities[city].Hotels
		for offset := 1; offset <= len(hotels) && hotelIndex < 0; offset++ {
			if candidate := hotels[(rank+offset)%len(hotels)]; !data.isUsed(candidate) && !capacityManaged(candidate) {
				hotelIndex = candidate
			}
		}
	}
	data.used[hotelIndex] = struct{}{}
	hotel := &a.reference.Data.Hotels[hotelIndex]
	username, password := a.user(sample.Random, streamReserveUser)
	inDate := data.dateBase.AddDate(0, 0, int(night))
	outDate := inDate.AddDate(0, 0, 1)
	baseQuery := map[string]string{
		"inDate": inDate.Format(time.DateOnly), "outDate": outDate.Format(time.DateOnly),
		"hotelId":      hotel.ID,
		"customerName": fmt.Sprintf("vibesys-%016x-%d", sample.Random, sample.Counter),
		"username":     username, "password": password,
	}
	request := func(number int) api.HTTPRequestSpec {
		query := make(map[string]string, len(baseQuery)+1)
		for key, value := range baseQuery {
			query[key] = value
		}
		query["number"] = strconv.Itoa(number)
		return api.HTTPRequestSpec{Method: http.MethodGet, Path: "/reservation", Query: query}
	}
	return api.OperationPlan{
		Invocations: []api.Invocation{
			{Target: operation.Target, Operation: operation.Name, Payload: request(hotel.Capacity)},
			{Target: operation.Target, Operation: operation.Name, Payload: request(1)},
		},
		State: &operationState{kind: operation.Name, release: release, built: time.Now()},
	}, nil
}

func (a *Application) FinishOperation(plan api.OperationPlan) {
	state, ok := plan.State.(*operationState)
	if !ok || state == nil {
		return
	}
	if state.finish != nil {
		// A write that was not validated may still have been applied.
		state.finish(time.Now(), false)
		state.finish = nil
	}
	if state.release != nil {
		state.release()
		state.release = nil
	}
}
