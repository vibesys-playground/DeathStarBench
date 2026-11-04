package hotel

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/accuracy"
	"vibesys/microservice-evaluator/api"
)

const (
	loginSuccess       = "Login successfully!"
	loginFailure       = "Failed. Please check your username and password. "
	reservationSuccess = "Reserve successfully!"
	reservationFailure = "Failed. Already reserved. "
)

func (a *Application) Check(
	ctx context.Context,
	runtime api.Runtime,
	check api.AccuracyContext,
	recorder api.AccuracyRecorder,
) error {
	if err := validateAccuracyCaseCount(check.Cases); err != nil {
		return err
	}
	c := client{runtime: runtime, timeout: a.timeout}
	random := rand.New(rand.NewSource(check.Seed ^ 0x51f15e))

	checks, err := a.verifySearch(ctx, c, random, max(check.Cases, 4))
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("strict_geojson_schema", "search_semantics"); err != nil {
		return err
	}

	checks, err = a.verifyRecommendations(ctx, c, random)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("seeded_profile_semantics", "recommendation_semantics"); err != nil {
		return err
	}

	checks, err = a.verifyAuthentication(ctx, c, random, max(check.Cases, 4))
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("authentication_semantics"); err != nil {
		return err
	}

	checks, err = a.verifyNegativeCases(ctx, c, random)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}

	checks, err = a.verifySequentialDifferential(ctx, c, check.Seed, check.Cases)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("sequential_differential"); err != nil {
		return err
	}

	checks, err = a.verifySearchAvailability(ctx, c, check.Seed, random)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("search_availability"); err != nil {
		return err
	}

	checks, err = a.verifyOptionalReservationNumber(ctx, c, check.Seed, random)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("reservation_optional_number"); err != nil {
		return err
	}

	checks, err = a.verifyReservations(ctx, c, check.Seed, check.Cases, random)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("reservation_capacity", "read_your_write", "reservation_isolation"); err != nil {
		return err
	}

	checks, err = a.verifyDegenerateDateRanges(ctx, c, check.Seed, random)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("degenerate_date_ranges"); err != nil {
		return err
	}

	checks, err = a.verifyMultiNightAtomicity(ctx, c, check.Seed, check.Cases, random)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("multi_night_atomicity"); err != nil {
		return err
	}

	checks, err = a.verifyConcurrentIsolation(ctx, c, check.Seed, random)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("concurrent_isolation"); err != nil {
		return err
	}

	if a.strict.LinearizableCapacity {
		checks, err = a.verifyLinearizableCapacity(ctx, c, check.Seed, check.Cases, random)
		recorder.AddChecks(checks)
		if err != nil {
			return err
		}
		if err := recorder.Pass("linearizable_capacity"); err != nil {
			return err
		}
	}
	if a.strict.EndpointLiveness {
		checks, err = a.verifyEndpointLiveness(ctx, c, check.Seed, random)
		recorder.AddChecks(checks)
		if err != nil {
			return err
		}
		if err := recorder.Pass("endpoint_liveness"); err != nil {
			return err
		}
	}
	lifecycle := check.Crash != nil && check.Start != nil
	if lifecycle {
		if err := a.verifyLifecycle(ctx, c, check, random, recorder); err != nil {
			return err
		}
	}
	// The write checks change seeded data the checks above expect, so they
	// run last.
	return a.verifyWrites(ctx, c, check, random, recorder, lifecycle)
}

func (a *Application) verifyLifecycle(
	ctx context.Context, c client, check api.AccuracyContext, random *rand.Rand, recorder api.AccuracyRecorder,
) error {
	checks, err := a.verifyCrashRecovery(ctx, c, check.Seed, random, check.Crash, check.Start)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	if err := recorder.Pass("crash_recovery"); err != nil {
		return err
	}

	checks, err = a.verifyDurableState(
		ctx, c, check.Seed, check.Cases, random, check.Crash, check.Start, a.strict.DurableAvailability,
	)
	recorder.AddChecks(checks)
	if err != nil {
		return err
	}
	durable := []string{"durable_capacity"}
	if a.strict.DurableAvailability {
		durable = append(durable, "durable_availability")
	}
	return recorder.Pass(durable...)
}

func validateAccuracyCaseCount(cases int) error {
	if cases < 1 || cases > maxAccuracyCases {
		return fmt.Errorf(
			"Hotel accuracy cases must be between 1 and %d, got %d; larger values overlap reserved date namespaces",
			maxAccuracyCases,
			cases,
		)
	}
	return nil
}

func (a *Application) verifySearch(ctx context.Context, c client, random *rand.Rand, cases int) (int, error) {
	checks := 0
	for caseIndex := 0; caseIndex < cases; caseIndex++ {
		if err := checkContext(ctx); err != nil {
			return checks, err
		}
		query, expected, err := a.fuzzedSearch(random)
		if err != nil {
			return checks, err
		}
		if caseIndex%2 == 0 {
			query["locale"] = "en"
		}
		first, err := c.geoJSON(ctx, "/hotels", query)
		checks++
		if err != nil {
			return checks, err
		}
		if err := a.validateSearchSet(first, expected); err != nil {
			return checks, fmt.Errorf("fuzzed search case %d query %v: %w", caseIndex, query, err)
		}
		second, err := c.geoJSON(ctx, "/hotels", query)
		checks++
		if err != nil {
			return checks, err
		}
		if err := a.validateSearchSet(second, expected); err != nil {
			return checks, fmt.Errorf("warm fuzzed search case %d query %v: %w", caseIndex, query, err)
		}
		if err := sameIDs(first, second); err != nil {
			return checks, fmt.Errorf("fuzzed search case %d cache stability: %w", caseIndex, err)
		}
	}
	return checks, nil
}

// fuzzedSearch draws a stay in the rate window (1-7 nights) and a location
// within about 100 m of a random hotel, and the search answer the model
// expects there. No check reserves inside the rate window, so every hotel is
// available and the answer is geo's nearest rate-backed hotels.
func (a *Application) fuzzedSearch(random *rand.Rand) (map[string]string, []string, error) {
	nights := 1 + random.Intn(7)
	start := dataset.RateWindowStart.AddDate(0, 0, random.Intn(dataset.RateWindowDays-nights+1))
	oracle, err := newHotelOracle(a.catalog)
	if err != nil {
		return nil, nil, err
	}
	for attempt := 0; attempt < 16; attempt++ {
		anchor := a.catalog[strconv.Itoa(1+random.Intn(len(a.catalog)))]
		jitter := func() float64 { return (random.Float64()*2 - 1) / 1000.0 }
		query := map[string]string{
			"inDate":  start.Format(time.DateOnly),
			"outDate": start.AddDate(0, 0, nights).Format(time.DateOnly),
			"lat":     strconv.FormatFloat(anchor.recommendLat+jitter(), 'f', 6, 64),
			"lon":     strconv.FormatFloat(anchor.recommendLon+jitter(), 'f', 6, 64),
		}
		observation, err := oracle.search(hotelState{Reserved: map[string]int{}}, query)
		if err != nil {
			continue // an ambiguous location; draw another
		}
		return query, observation.HotelIDs, nil
	}
	return nil, nil, fmt.Errorf("no unambiguous search location in 16 draws")
}

func (a *Application) validateSearchSet(features map[string]feature, expected []string) error {
	if err := validateProfiles(features, a.catalog, "canonical search"); err != nil {
		return err
	}
	if err := exactIDs(features, expected...); err != nil {
		return fmt.Errorf("nearby rate-backed result set: %w", err)
	}
	return nil
}

func (a *Application) verifyRecommendations(ctx context.Context, c client, random *rand.Rand) (int, error) {
	checks := 0
	reference := hotelsupport.DefaultReference()
	queries := []struct {
		require string
		ids     []string
	}{
		{require: "price", ids: reference.Cheapest()},
		{require: "rate", ids: reference.BestRated()},
	}
	for queryIndex, item := range queries {
		query := map[string]string{"require": item.require, "lat": "37.7749", "lon": "-122.4194"}
		if queryIndex%2 == 0 {
			query["locale"] = "en"
		}
		features, err := c.geoJSON(ctx, "/recommendations", query)
		checks++
		if err != nil {
			return checks, err
		}
		if err := validateProfiles(features, a.catalog, item.require+" recommendation"); err != nil {
			return checks, err
		}
		if err := exactIDs(features, item.ids...); err != nil {
			return checks, fmt.Errorf("%s recommendation: %w", item.require, err)
		}
	}

	// Query the exact coordinates of randomly chosen seeded hotels: distance
	// recommendation must return the hotel at that location (the model scans
	// every hotel, so it also states the answer if two hotels shared it).
	sampled := 80
	for caseIndex := 0; caseIndex < sampled; caseIndex++ {
		if err := checkContext(ctx); err != nil {
			return checks, err
		}
		expected := a.catalog[strconv.Itoa(1+random.Intn(len(a.catalog)))]
		query := map[string]string{
			"require": "dis",
			"lat":     strconv.FormatFloat(expected.recommendLat, 'f', -1, 64),
			"lon":     strconv.FormatFloat(expected.recommendLon, 'f', -1, 64),
		}
		if caseIndex%2 == 0 {
			query["locale"] = "en"
		}
		want := reference.NearestHotels(expected.recommendLat, expected.recommendLon)
		features, err := c.geoJSON(ctx, "/recommendations", query)
		checks++
		if err != nil {
			return checks, err
		}
		if err := validateProfiles(features, a.catalog, "distance recommendation"); err != nil {
			return checks, err
		}
		if err := exactIDs(features, want...); err != nil {
			return checks, fmt.Errorf("distance recommendation at hotel %s coordinates: %w", expected.id, err)
		}
	}

	// Probe non-catalog coordinates derived from the runtime seed, within a
	// few hundred metres of a random hotel; the model computes the answer.
	for caseIndex := 0; caseIndex < 2*max(4, sampled/20); caseIndex++ {
		anchor := a.catalog[strconv.Itoa(1+random.Intn(len(a.catalog)))]
		jitter := func() float64 { return (random.Float64()*2 - 1) / 300.0 }
		lat := anchor.recommendLat + jitter()
		lon := anchor.recommendLon + jitter()
		query := map[string]string{
			"require": "dis",
			"lat":     strconv.FormatFloat(lat, 'f', 8, 64),
			"lon":     strconv.FormatFloat(lon, 'f', 8, 64),
		}
		if caseIndex%2 == 0 {
			query["locale"] = "en"
		}
		parsedLat, _ := strconv.ParseFloat(query["lat"], 64)
		parsedLon, _ := strconv.ParseFloat(query["lon"], 64)
		want := reference.NearestHotels(parsedLat, parsedLon)
		features, err := c.geoJSON(ctx, "/recommendations", query)
		checks++
		if err != nil {
			return checks, err
		}
		if err := validateProfiles(features, a.catalog, "fuzzed distance recommendation"); err != nil {
			return checks, err
		}
		if err := exactIDs(features, want...); err != nil {
			return checks, fmt.Errorf("fuzzed distance recommendation near hotel %s: %w", anchor.id, err)
		}
	}
	return checks, nil
}

func (a *Application) verifyAuthentication(ctx context.Context, c client, random *rand.Rand, cases int) (int, error) {
	checks := 0
	indices := random.Perm(hotelsupport.UserCount())
	for caseIndex := 0; caseIndex < cases; caseIndex++ {
		index := indices[caseIndex%len(indices)]
		if err := checkContext(ctx); err != nil {
			return checks, err
		}
		username, password := hotelsupport.User(index)
		if err := c.exactMessage(ctx, "/user", credentials(username, password), loginSuccess); err != nil {
			return checks + 1, fmt.Errorf("valid seed user %d: %w", index, err)
		}
		checks++
		if err := c.exactMessage(ctx, "/user", credentials(username, password+"-wrong"), loginFailure); err != nil {
			return checks + 1, fmt.Errorf("wrong password for seed user %d: %w", index, err)
		}
		checks++
	}
	if err := c.exactMessage(ctx, "/user", credentials(fmt.Sprintf("nobody-%016x", random.Uint64()), "not-a-password"), loginFailure); err != nil {
		return checks + 1, fmt.Errorf("nonexistent user: %w", err)
	}
	return checks + 1, nil
}

func (a *Application) verifyNegativeCases(ctx context.Context, c client, random *rand.Rand) (int, error) {
	username, password := hotelsupport.User(0)
	checks := 0
	reservationBase := map[string]string{
		"inDate": "3000-01-01", "outDate": "3000-01-02", "hotelId": "1",
		"customerName": "negative-case", "username": username, "password": password, "number": "1",
	}
	negative := []struct {
		path  string
		query map[string]string
	}{
		{"/hotels", map[string]string{"lat": "37", "lon": "-122"}},
		{"/hotels", map[string]string{"inDate": "2015-04-09", "outDate": "2015-04-10"}},
		{"/recommendations", map[string]string{"require": "price"}},
		{"/recommendations", map[string]string{
			"lat": "37", "lon": "-122", "require": fmt.Sprintf("unknown-%016x", random.Uint64()),
		}},
		{"/user", map[string]string{"username": username}},
		{"/reservation", without(reservationBase, "inDate")},
		{"/reservation", with(reservationBase, "inDate", fmt.Sprintf("invalid-%016x", random.Uint64()))},
		{"/reservation", without(reservationBase, "hotelId")},
		{"/reservation", without(reservationBase, "customerName")},
		{"/reservation", without(reservationBase, "password")},
	}
	random.Shuffle(len(negative), func(left, right int) { negative[left], negative[right] = negative[right], negative[left] })
	for _, item := range negative {
		if err := c.badRequest(ctx, item.path, item.query); err != nil {
			return checks + 1, err
		}
		checks++
	}
	return checks, nil
}

func (a *Application) verifyOptionalReservationNumber(
	ctx context.Context,
	c client,
	seed int64,
	random *rand.Rand,
) (int, error) {
	username, password := hotelsupport.User(0)
	hotelID := strconv.Itoa(1 + random.Intn(len(a.catalog)))
	capacity, err := hotelsupport.Capacity(hotelID)
	if err != nil {
		return 0, err
	}
	start := reservationDate(seed).AddDate(0, 0, 180)
	query := credentials(username, password)
	query["hotelId"] = hotelID
	query["inDate"] = start.Format(time.DateOnly)
	query["outDate"] = start.AddDate(0, 0, 1).Format(time.DateOnly)
	query["customerName"] = "optional-number"
	if err := c.exactMessage(ctx, "/reservation", query, reservationSuccess); err != nil {
		return 1, fmt.Errorf("reservation without optional number: %w", err)
	}
	query["number"] = strconv.Itoa(capacity)
	query["customerName"] = "optional-number-capacity"
	if err := c.exactMessage(ctx, "/reservation", query, reservationSuccess); err != nil {
		return 2, fmt.Errorf("omitted number unexpectedly consumed capacity: %w", err)
	}
	query["number"] = "1"
	query["customerName"] = "optional-number-over-capacity"
	if err := c.exactMessage(ctx, "/reservation", query, reservationFailure); err != nil {
		return 3, fmt.Errorf("optional-number capacity read-back: %w", err)
	}
	return 3, nil
}

func (a *Application) verifySearchAvailability(
	ctx context.Context,
	c client,
	seed int64,
	random *rand.Rand,
) (int, error) {
	username, password := hotelsupport.User(0)
	rateIDs := sortedRateBackedIDs()
	hotelID := rateIDs[random.Intn(len(rateIDs))]
	capacity, err := hotelsupport.Capacity(hotelID)
	if err != nil {
		return 0, err
	}
	start := reservationDate(seed).AddDate(0, 0, 365)
	reservedDate := [2]string{start.Format(time.DateOnly), start.AddDate(0, 0, 1).Format(time.DateOnly)}
	query := credentials(username, password)
	query["hotelId"] = hotelID
	query["inDate"] = reservedDate[0]
	query["outDate"] = reservedDate[1]
	query["number"] = strconv.Itoa(capacity)
	query["customerName"] = "search-availability"
	if err := c.exactMessage(ctx, "/reservation", query, reservationSuccess); err != nil {
		return 1, fmt.Errorf("search availability exact-capacity setup: %w", err)
	}
	overCapacity := cloneQuery(query)
	overCapacity["number"] = "1"
	if err := c.exactMessage(ctx, "/reservation", overCapacity, reservationFailure); err != nil {
		return 2, fmt.Errorf("search availability setup visibility: %w", err)
	}

	anchor := a.catalog[hotelID]
	search := func(date [2]string) (map[string]feature, error) {
		return c.geoJSON(ctx, "/hotels", map[string]string{
			"inDate": date[0], "outDate": date[1],
			"lat":    strconv.FormatFloat(anchor.recommendLat, 'f', -1, 64),
			"lon":    strconv.FormatFloat(anchor.recommendLon, 'f', -1, 64),
			"locale": "en",
		})
	}
	reserved, err := search(reservedDate)
	if err != nil {
		return 3, err
	}
	if err := validateProfiles(reserved, a.catalog, "reserved-date search"); err != nil {
		return 3, err
	}
	oracle, err := newHotelOracle(a.catalog)
	if err != nil {
		return 3, err
	}
	filled := hotelState{Reserved: map[string]int{reservationStateKey(hotelID, reservedDate[0]): capacity}}
	searchAt := func(date [2]string) map[string]string {
		return map[string]string{
			"inDate": date[0], "outDate": date[1],
			"lat": strconv.FormatFloat(anchor.recommendLat, 'f', -1, 64),
			"lon": strconv.FormatFloat(anchor.recommendLon, 'f', -1, 64),
		}
	}
	wanted, err := oracle.search(filled, searchAt(reservedDate))
	if err != nil {
		return 3, err
	}
	if err := exactIDs(reserved, wanted.HotelIDs...); err != nil {
		return 3, fmt.Errorf("reserved-date search for filled hotel %s: %w", hotelID, err)
	}
	adjacentDate := [2]string{
		start.AddDate(0, 0, 1).Format(time.DateOnly),
		start.AddDate(0, 0, 2).Format(time.DateOnly),
	}
	adjacent, err := search(adjacentDate)
	if err != nil {
		return 4, err
	}
	if err := validateProfiles(adjacent, a.catalog, "adjacent-date search"); err != nil {
		return 4, err
	}
	wantedAdjacent, err := oracle.search(filled, searchAt(adjacentDate))
	if err != nil {
		return 4, err
	}
	if err := exactIDs(adjacent, wantedAdjacent.HotelIDs...); err != nil {
		return 4, fmt.Errorf("adjacent-date search after filling hotel %s: %w", hotelID, err)
	}
	found := false
	for _, id := range wantedAdjacent.HotelIDs {
		found = found || id == hotelID
	}
	if !found {
		return 4, fmt.Errorf("model: hotel %s is not in the search at its own location", hotelID)
	}
	return 4, nil
}

func (a *Application) verifyReservations(
	ctx context.Context,
	c client,
	seed int64,
	cases int,
	random *rand.Rand,
) (int, error) {
	username, password := hotelsupport.User(0)
	start := reservationDate(seed)
	checks := 0
	reserve := func(hotelID string, date [2]string, rooms int, customer, expected string) error {
		query := credentials(username, password)
		query["hotelId"] = hotelID
		query["inDate"] = date[0]
		query["outDate"] = date[1]
		query["number"] = strconv.Itoa(rooms)
		query["customerName"] = customer
		checks++
		return c.exactMessage(ctx, "/reservation", query, expected)
	}

	// The endpoint has no delete operation. Use shuffled hotel pairs and
	// hash-namespaced, disjoint future date slots so each case is independent
	// without claiming cleanup that the application cannot perform.
	hotelIDs := sortedRateBackedIDs()
	hotelOrder := random.Perm(len(hotelIDs))
	for caseIndex := 0; caseIndex < cases; caseIndex++ {
		if err := checkContext(ctx); err != nil {
			return checks, err
		}
		primary := hotelIDs[hotelOrder[(caseIndex*2)%len(hotelOrder)]]
		isolation := hotelIDs[hotelOrder[(caseIndex*2+1)%len(hotelOrder)]]
		caseStart := start.AddDate(0, 0, caseIndex*4)
		exactDate := [2]string{
			caseStart.Format("2006-01-02"),
			caseStart.AddDate(0, 0, 1).Format("2006-01-02"),
		}
		adjacentDate := [2]string{
			caseStart.AddDate(0, 0, 2).Format("2006-01-02"),
			caseStart.AddDate(0, 0, 3).Format("2006-01-02"),
		}
		capacity, err := hotelsupport.Capacity(primary)
		if err != nil {
			return checks, err
		}
		label := fmt.Sprintf("case-%d", caseIndex)
		if err := reserve(primary, exactDate, capacity, label+"-exact-capacity", reservationSuccess); err != nil {
			return checks, fmt.Errorf("reservation case %d exact capacity: %w", caseIndex, err)
		}
		if err := reserve(primary, exactDate, 1, label+"-read-your-write", reservationFailure); err != nil {
			return checks, fmt.Errorf("reservation case %d immediately visible: %w", caseIndex, err)
		}
		if err := reserve(primary, adjacentDate, capacity+1, label+"-atomic-rejection", reservationFailure); err != nil {
			return checks, fmt.Errorf("reservation case %d over capacity: %w", caseIndex, err)
		}
		if err := reserve(primary, adjacentDate, 1, label+"-post-rejection", reservationSuccess); err != nil {
			return checks, fmt.Errorf("reservation case %d rejected request mutated capacity: %w", caseIndex, err)
		}
		if err := reserve(isolation, adjacentDate, 1, label+"-hotel-isolation", reservationSuccess); err != nil {
			return checks, fmt.Errorf("reservation case %d same-date hotel isolation: %w", caseIndex, err)
		}
	}
	return checks, nil
}

func (a *Application) verifyCrashRecovery(
	ctx context.Context,
	c client,
	seed int64,
	random *rand.Rand,
	crash func(context.Context) error,
	startCandidate func(context.Context) error,
) (int, error) {
	username, password := hotelsupport.User(0)
	rateIDs := sortedRateBackedIDs()
	hotelID := rateIDs[random.Intn(len(rateIDs))]
	capacity, err := hotelsupport.Capacity(hotelID)
	if err != nil {
		return 0, err
	}
	start := reservationDate(seed).AddDate(0, 0, 730)
	dates := [2]string{start.Format(time.DateOnly), start.AddDate(0, 0, 1).Format(time.DateOnly)}
	program := accuracy.Program[differentialAction]{
		SchemaVersion: accuracy.ProgramSchemaVersion,
		ID:            "hotel-crash-recovery",
		Steps: []accuracy.Step[differentialAction]{
			callStep("crash-recovery-write", differentialAction{
				Kind: actionReserve,
				Query: reservationActionQuery(
					hotelID, dates, capacity, "crash-recovery", username, password,
				),
			}),
			crashStep(),
			startStep(),
			callStep("crash-recovery-capacity", differentialAction{
				Kind: actionReserve,
				Query: reservationActionQuery(
					hotelID, dates, 1, "crash-recovery-probe", username, password,
				),
			}),
		},
	}
	// Upstream restores reservation capacity from MongoDB after cache loss, but
	// its search miss path does not restore cached availability. Keep that
	// stronger guarantee opt-in, as in verifyDurableState.
	if a.strict.DurableAvailability {
		program.Steps = append(program.Steps, callStep("crash-recovery-search", differentialAction{
			Kind:  actionSearch,
			Query: searchQuery(dates, a.catalog[hotelID], true),
		}))
	}
	return a.verifyHotelProgram(ctx, c, program, crash, startCandidate)
}

func reservationDate(seed int64) time.Time {
	digest := sha256.Sum256([]byte(fmt.Sprintf("hotel-reservation-accuracy/%d", seed)))
	year := 3000 + int(binary.BigEndian.Uint16(digest[:2]))%5000
	day := 1 + int(binary.BigEndian.Uint16(digest[2:4]))%300
	return time.Date(year, time.January, day, 0, 0, 0, 0, time.UTC)
}

func credentials(username, password string) map[string]string {
	return map[string]string{"username": username, "password": password}
}

func without(source map[string]string, key string) map[string]string {
	result := cloneQuery(source)
	delete(result, key)
	return result
}

func with(source map[string]string, key, value string) map[string]string {
	result := cloneQuery(source)
	result[key] = value
	return result
}

func cloneQuery(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
