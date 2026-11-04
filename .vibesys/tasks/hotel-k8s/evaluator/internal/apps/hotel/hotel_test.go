package hotel

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/api"
)

func TestNewValidatesHotelContract(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*api.Workload)
		want   string
	}{
		{"missing gateway", func(workload *api.Workload) { workload.Targets = nil }, "target named"},
		{"wrong protocol", func(workload *api.Workload) { workload.Targets[0].Protocol = "grpc" }, "must use HTTP"},
		{"non-reused session", func(workload *api.Workload) { workload.Targets[0].SessionPolicy = "new" }, "session_policy reuse"},
		{"zero timeout", func(workload *api.Workload) { workload.Load.TimeoutSeconds = 0 }, "timeout must be positive"},
		{"unknown config", func(workload *api.Workload) { workload.ApplicationConfig["rooms"] = int64(2) }, "unknown Hotel application_config"},
		{"unknown operation", func(workload *api.Workload) { workload.Operations[0].Name = "search" }, "unknown Hotel operation"},
		{"wrong target", func(workload *api.Workload) { workload.Operations[0].Target = "search" }, "must target"},
		{"declarative request", func(workload *api.Workload) { workload.Operations[0].HTTP = &api.HTTPRequestSpec{} }, "must not declare"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workload := hotelWorkload(operationSearch)
			test.mutate(&workload)
			if _, err := New(workload); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("New() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestApplicationExposesSharedPreflight(t *testing.T) {
	applicationValue, err := New(hotelWorkload(operationLoginValid))
	if err != nil {
		t.Fatal(err)
	}
	preflight, ok := applicationValue.(api.PreflightApplication)
	if !ok {
		t.Fatal("Hotel benchmark does not implement api.PreflightApplication")
	}
	if len(preflight.ReadinessProbes()) != 3 || len(preflight.PreflightProbes()) != 2 {
		t.Fatalf("unexpected shared preflight cardinality: readiness=%d protocol=%d",
			len(preflight.ReadinessProbes()), len(preflight.PreflightProbes()))
	}
}

func newApplication(t testing.TB, operations ...string) *Application {
	t.Helper()
	applicationValue, err := New(hotelWorkload(operations...))
	if err != nil {
		t.Fatal(err)
	}
	return applicationValue.(*Application)
}

func build(t testing.TB, application *Application, name string, sample api.Sample, prepared any) api.OperationPlan {
	t.Helper()
	plan, err := application.BuildOperation(api.Operation{Name: name, Target: hotelsupport.GatewayTarget}, sample, prepared)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestBuildOperationCreatesCanonicalRequests(t *testing.T) {
	tests := []struct {
		operation string
		path      string
		require   string
		steps     int
	}{
		{operationSearch, "/hotels", "", 1},
		{operationRecommendDistance, "/recommendations", "dis", 1},
		{operationRecommendRate, "/recommendations", "rate", 1},
		{operationRecommendPrice, "/recommendations", "price", 1},
		{operationLoginValid, "/user", "", 1},
		{operationLoginInvalid, "/user", "", 1},
		{operationReserveCapacity, "/reservation", "", 2},
	}
	reference := hotelsupport.DefaultReference()
	for _, test := range tests {
		t.Run(test.operation, func(t *testing.T) {
			application := newApplication(t, test.operation)
			prepared, err := application.Prepare(context.Background(), nil, api.TrialContext{Index: 2, FixtureSeed: 99})
			if err != nil {
				t.Fatal(err)
			}
			for random := uint64(0); random < 200; random++ {
				plan := build(t, application, test.operation, api.Sample{Counter: int64(random), Random: random * 7919}, prepared)
				if len(plan.Invocations) != test.steps {
					t.Fatalf("invocations = %d, want %d", len(plan.Invocations), test.steps)
				}
				request := plan.Invocations[0].Payload.(api.HTTPRequestSpec)
				if request.Method != http.MethodGet || request.Path != test.path || len(request.Form) != 0 || request.Body != "" {
					t.Fatalf("unexpected canonical request: %+v", request)
				}
				state := plan.State.(*operationState)
				switch {
				case test.operation == operationSearch:
					assertCanonicalSearchQuery(t, request.Query)
					if len(state.expectedIDs) == 0 || len(state.expectedIDs) > 5 {
						t.Fatalf("search at %v expects %v", request.Query, state.expectedIDs)
					}
				case test.require != "":
					assertCanonicalRecommendationQuery(t, request.Query, test.require)
				case test.operation == operationLoginValid || test.operation == operationLoginInvalid:
					username, password := request.Query["username"], request.Query["password"]
					valid := reference.ValidLogin(username, password)
					if valid != (test.operation == operationLoginValid) {
						t.Fatalf("login query %v: valid = %v", request.Query, valid)
					}
				case test.operation == operationReserveCapacity:
					first := plan.Invocations[0].Payload.(api.HTTPRequestSpec)
					second := plan.Invocations[1].Payload.(api.HTTPRequestSpec)
					capacity, err := reference.Capacity(first.Query["hotelId"])
					if err != nil {
						t.Fatal(err)
					}
					if first.Query["number"] != strconv.Itoa(capacity) || second.Query["number"] != "1" {
						t.Fatalf("capacity sequence = %q then %q, capacity %d", first.Query["number"], second.Query["number"], capacity)
					}
					if first.Query["inDate"] != second.Query["inDate"] || first.Query["outDate"] != second.Query["outDate"] {
						t.Fatalf("capacity steps use different dates: %v then %v", first.Query, second.Query)
					}
					if !reference.ValidLogin(first.Query["username"], first.Query["password"]) {
						t.Fatalf("reservation uses an unknown account: %v", first.Query)
					}
				}
				application.FinishOperation(plan)
			}
		})
	}
}

func TestPlansAreDeterministicFunctionsOfTheSample(t *testing.T) {
	left := newApplication(t, operationSearch)
	right := newApplication(t, operationSearch)
	leftFixture, _ := left.Prepare(context.Background(), nil, api.TrialContext{Index: 1, FixtureSeed: 5})
	rightFixture, _ := right.Prepare(context.Background(), nil, api.TrialContext{Index: 1, FixtureSeed: 5})
	for _, name := range []string{
		operationSearch, operationRecommendDistance, operationRecommendRate, operationRecommendPrice,
		operationLoginValid, operationLoginInvalid, operationReserveCapacity,
	} {
		for random := uint64(1); random < 50; random++ {
			sample := api.Sample{Counter: int64(random), Random: random * 0x9e3779b97f4a7c15}
			first := build(t, left, name, sample, leftFixture)
			second := build(t, right, name, sample, rightFixture)
			left.FinishOperation(first)
			right.FinishOperation(second)
			if !reflect.DeepEqual(first.Invocations, second.Invocations) ||
				!reflect.DeepEqual(first.State.(*operationState).expectedIDs, second.State.(*operationState).expectedIDs) {
				t.Fatalf("%s plan for sample %v differs between runs", name, sample)
			}
		}
	}
	// A pinned sample keeps its request, so a run is replayable.
	plan := build(t, left, operationSearch, api.Sample{Random: 12345}, leftFixture)
	want := map[string]string{"inDate": "2027-05-19", "outDate": "2027-05-20", "lat": "51.45385", "lon": "-0.13766"}
	if got := plan.Invocations[0].Payload.(api.HTTPRequestSpec).Query; !reflect.DeepEqual(got, want) {
		t.Fatalf("search for sample 12345 = %v, want %v", got, want)
	}
}

// TestExpectedAnswersAgainstIndependentReferences checks the adapter's expected
// answers against brute force over the dataset: the distance recommendation
// exactly, search by its contract (at most five hotels, all within 10 km, all
// with rate plans) and by agreement with an exact nearest-five search except
// where go-geoindex's approximation differs (pinned by appsupport's golden).
func TestExpectedAnswersAgainstIndependentReferences(t *testing.T) {
	application := newApplication(t, operationSearch)
	prepared, _ := application.Prepare(context.Background(), nil, api.TrialContext{})
	data := dataset.Default()
	const samples = 600
	agree := 0
	for random := uint64(0); random < samples; random++ {
		plan := build(t, application, operationSearch, api.Sample{Random: random}, prepared)
		query := plan.Invocations[0].Payload.(api.HTTPRequestSpec).Query
		lat, _ := hotelsupport.ParseSearchCoordinate(query["lat"])
		lon, _ := hotelsupport.ParseSearchCoordinate(query["lon"])
		expected := append([]string(nil), plan.State.(*operationState).expectedIDs...)
		sort.Strings(expected)
		for _, id := range expected {
			hotel, _ := hotelsupport.DefaultReference().Hotel(id)
			if distance := haversineKm(lat, lon, hotel.Lat, hotel.Lon); distance > 10 || len(hotel.RatePlans) == 0 {
				t.Fatalf("search %v expects hotel %s at %.2f km", query, id, distance)
			}
		}
		if reflect.DeepEqual(expected, exactNearest(data, lat, lon, 5, 10)) {
			agree++
		}
	}
	if agree < samples*95/100 {
		t.Fatalf("search expectations agree with an exact nearest-five search in %d of %d samples", agree, samples)
	}
	t.Logf("search expectations equal the exact nearest five in %d of %d samples", agree, samples)

	for random := uint64(0); random < samples; random++ {
		plan := build(t, application, operationRecommendDistance, api.Sample{Random: random}, prepared)
		query := plan.Invocations[0].Payload.(api.HTTPRequestSpec).Query
		lat, _ := strconv.ParseFloat(query["lat"], 64)
		lon, _ := strconv.ParseFloat(query["lon"], 64)
		expected := plan.State.(*operationState).expectedIDs
		want := exactNearest(data, lat, lon, 1, math.Inf(1))
		if len(expected) != 1 || expected[0] != want[0] {
			t.Fatalf("distance recommendation at %v expects %v, brute force %v", query, expected, want)
		}
	}
	best, cheapest := []string{}, []string{}
	maxRating, minPrice := 0.0, math.MaxFloat64
	for _, hotel := range data.Hotels {
		maxRating, minPrice = math.Max(maxRating, hotel.Rating), math.Min(minPrice, hotel.Price)
	}
	for _, hotel := range data.Hotels {
		if hotel.Rating == maxRating {
			best = append(best, hotel.ID)
		}
		if hotel.Price == minPrice {
			cheapest = append(cheapest, hotel.ID)
		}
	}
	for name, want := range map[string][]string{operationRecommendRate: best, operationRecommendPrice: cheapest} {
		plan := build(t, application, name, api.Sample{Random: 3}, prepared)
		if got := plan.State.(*operationState).expectedIDs; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s expects %v, brute force %v", name, got, want)
		}
	}
}

func exactNearest(data *dataset.Dataset, lat, lon float64, k int, maxKm float64) []string {
	type candidate struct {
		id       string
		distance float64
	}
	candidates := make([]candidate, 0, len(data.Hotels))
	for _, hotel := range data.Hotels {
		if distance := haversineKm(lat, lon, hotel.Lat, hotel.Lon); distance <= maxKm {
			candidates = append(candidates, candidate{hotel.ID, distance})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].distance < candidates[j].distance })
	ids := []string{}
	for index := 0; index < len(candidates) && index < k; index++ {
		ids = append(ids, candidates[index].id)
	}
	sort.Strings(ids)
	return ids
}

func haversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	rad := math.Pi / 180
	s1 := math.Sin((lat2 - lat1) * rad / 2)
	s2 := math.Sin((lon2 - lon1) * rad / 2)
	a := s1*s1 + math.Cos(lat1*rad)*math.Cos(lat2*rad)*s2*s2
	return 2 * 6371 * math.Asin(math.Min(1, math.Sqrt(a)))
}

// TestRequestDistributions draws 100,000 samples per operation and reports
// how often an exact-request cache (keyed on the full query) would hit.
func TestRequestDistributions(t *testing.T) {
	if testing.Short() {
		t.Skip("draws 100,000 samples per operation")
	}
	application := newApplication(t, operationSearch)
	prepared, _ := application.Prepare(context.Background(), nil, api.TrialContext{})
	const samples = 100000
	weights := map[string]float64{
		operationSearch: 600, operationRecommendDistance: 130, operationRecommendRate: 130,
		operationRecommendPrice: 130, operationLoginValid: 3, operationLoginInvalid: 2,
	}
	mixHits, mixWeight := 0.0, 0.0
	for _, name := range []string{
		operationSearch, operationRecommendDistance, operationRecommendRate, operationRecommendPrice,
		operationLoginValid, operationLoginInvalid,
	} {
		requests := make(map[string]int, samples)
		locations := make(map[string]int, samples)
		nights := make(map[int]int)
		cities := make(map[string]int)
		maxRepeat := 0
		for random := uint64(0); random < samples; random++ {
			plan := build(t, application, name, api.Sample{Random: random*0x2545f4914f6cdd1d + 17}, prepared)
			query := plan.Invocations[0].Payload.(api.HTTPRequestSpec).Query
			key := canonicalKey(query)
			requests[key]++
			if query["lat"] != "" {
				location := query["lat"] + "," + query["lon"]
				locations[location]++
				maxRepeat = max(maxRepeat, locations[location])
				cities[nearestCity(query)]++
			}
			if query["inDate"] != "" {
				in, _ := time.Parse(time.DateOnly, query["inDate"])
				out, _ := time.Parse(time.DateOnly, query["outDate"])
				nights[int(out.Sub(in).Hours()/24)]++
			}
		}
		hitRate := float64(samples-len(requests)) / samples
		mixHits += hitRate * weights[name]
		mixWeight += weights[name]
		t.Logf("%-18s exact-request cache hit rate %.4f (%d distinct of %d); most repeated location %d",
			name, hitRate, len(requests), samples, maxRepeat)
		if name == operationSearch {
			t.Logf("%-18s stay nights %v; requests per city %v", name, nights, cities)
			for n := 1; n <= 7; n++ {
				if nights[n] == 0 || n > 1 && nights[n] > nights[n-1] {
					t.Fatalf("stay lengths %v are not 1-7 nights skewed short", nights)
				}
			}
			if cities["London"] < cities["Paris"] || cities["Paris"] < cities["Rio de Janeiro"] {
				t.Fatalf("search cities do not follow popularity: %v", cities)
			}
		}
		if query := name; query != operationLoginValid && query != operationLoginInvalid {
			if maxRepeat > samples/1000 || hitRate > 0.01 {
				t.Fatalf("%s: a location repeats %d times, hit rate %.4f", name, maxRepeat, hitRate)
			}
		}
	}
	t.Logf("canonical mix (without reservations) exact-request cache hit rate %.4f", mixHits/mixWeight)
}

func canonicalKey(query map[string]string) string {
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	for _, key := range keys {
		builder.WriteString(key + "=" + query[key] + "&")
	}
	return builder.String()
}

func nearestCity(query map[string]string) string {
	lat, _ := strconv.ParseFloat(query["lat"], 64)
	lon, _ := strconv.ParseFloat(query["lon"], 64)
	best, name := math.Inf(1), ""
	for _, city := range dataset.Default().Cities {
		if distance := haversineKm(lat, lon, city.Lat, city.Lon); distance < best {
			best, name = distance, city.Name
		}
	}
	return name
}

func TestReservationsFollowPopularityAndNeverReuseASlot(t *testing.T) {
	application := newApplication(t, operationReserveCapacity)
	prepared, _ := application.Prepare(context.Background(), nil, api.TrialContext{Index: 0, FixtureSeed: 7})
	const samples = 20000
	slots := make(map[string]bool, samples)
	byCity := make(map[int]int)
	topRank := 0
	reference := hotelsupport.DefaultReference()
	for random := uint64(0); random < samples; random++ {
		plan := build(t, application, operationReserveCapacity, api.Sample{Counter: int64(random), Random: random * 0x9e3779b97f4a7c15}, prepared)
		query := plan.Invocations[0].Payload.(api.HTTPRequestSpec).Query
		application.FinishOperation(plan)
		slot := query["hotelId"] + "/" + query["inDate"]
		if slots[slot] {
			t.Fatalf("reservation slot %s used twice", slot)
		}
		slots[slot] = true
		hotel, _ := reference.Hotel(query["hotelId"])
		byCity[hotel.City]++
		if hotel.Popularity <= 10 {
			topRank++
		}
	}
	if byCity[0] < byCity[1] || byCity[1] < byCity[19] {
		t.Fatalf("reservations per city %v do not follow popularity", byCity)
	}
	t.Logf("%d reservations: %d on a city's 10 most popular hotels; per city %v", samples, topRank, byCity)
	if topRank < samples/20 {
		t.Fatalf("only %d of %d reservations on popular hotels", topRank, samples)
	}
}

func assertCanonicalSearchQuery(t *testing.T, query map[string]string) {
	t.Helper()
	if err := validateCanonicalSearchQuery(query); err != nil {
		t.Fatal(err)
	}
}

func validateCanonicalSearchQuery(query map[string]string) error {
	locale, hasLocale := query["locale"]
	if len(query) != 4 && len(query) != 5 || hasLocale && locale != "en" || len(query) == 5 && !hasLocale {
		return fmt.Errorf("search query has non-canonical fields: %v", query)
	}
	inDate, err := time.Parse(time.DateOnly, query["inDate"])
	if err != nil {
		return fmt.Errorf("invalid search arrival %q: %w", query["inDate"], err)
	}
	outDate, err := time.Parse(time.DateOnly, query["outDate"])
	if err != nil {
		return fmt.Errorf("invalid search departure %q: %w", query["outDate"], err)
	}
	nights := int(outDate.Sub(inDate).Hours() / 24)
	if inDate.Before(dataset.RateWindowStart) || outDate.After(dataset.RateWindowEnd()) || nights < 1 || nights > 7 {
		return fmt.Errorf("search stay %s-%s lies outside the rate window or 1-7 nights", query["inDate"], query["outDate"])
	}
	return validateCanonicalLocation(query)
}

func assertCanonicalRecommendationQuery(t *testing.T, query map[string]string, require string) {
	t.Helper()
	if err := validateCanonicalRecommendationQuery(query, require); err != nil {
		t.Fatal(err)
	}
}

func validateCanonicalRecommendationQuery(query map[string]string, require string) error {
	locale, hasLocale := query["locale"]
	if len(query) != 3 && len(query) != 4 || hasLocale && locale != "en" || len(query) == 4 && !hasLocale || query["require"] != require {
		return fmt.Errorf("recommendation query has non-canonical fields: %v", query)
	}
	return validateCanonicalLocation(query)
}

// validateCanonicalLocation accepts 5-decimal coordinates within the bounded
// normal spread (3.5 sigma per axis) of some city centre.
func validateCanonicalLocation(query map[string]string) error {
	lat, latErr := strconv.ParseFloat(query["lat"], 64)
	lon, lonErr := strconv.ParseFloat(query["lon"], 64)
	if latErr != nil || lonErr != nil || formatCoordinate(lat) != query["lat"] || formatCoordinate(lon) != query["lon"] {
		return fmt.Errorf("location is not canonical: lat=%q lon=%q", query["lat"], query["lon"])
	}
	for _, city := range dataset.Default().Cities {
		if haversineKm(lat, lon, city.Lat, city.Lon) <= 2*math.Sqrt(3)*city.SpreadKm*math.Sqrt2+0.01 {
			return nil
		}
	}
	if lat > -59 && lat < -19 && lon > -166 && lon < -84 {
		return nil // the additions' sandboxes
	}
	return fmt.Errorf("location %s,%s is not near any city centre", query["lat"], query["lon"])
}

func TestGeoJSONValidationIsStrictAndOrderInsensitive(t *testing.T) {
	valid := []map[string]any{feature("3"), feature("1"), feature("2")}
	if validation := validateGeoJSON(httpJSONResult(geoJSON(valid)), []string{"1", "2", "3"}); !validation.Success {
		t.Fatalf("valid shuffled GeoJSON failed: %+v", validation)
	}

	tests := []struct {
		name     string
		features []map[string]any
		want     string
	}{
		{"omitted ID", []map[string]any{feature("1"), feature("2")}, "response_value"},
		{"duplicate ID", []map[string]any{feature("1"), feature("1"), feature("2")}, "response_value"},
		{"wrong exact set", []map[string]any{feature("1"), feature("2"), feature("9")}, "response_value"},
		{"unknown catalog ID", []map[string]any{feature("1"), feature("2"), rawFeature("25001")}, "response_schema"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validation := validateGeoJSON(httpJSONResult(geoJSON(test.features)), []string{"1", "2", "3"})
			if validation.Success || validation.ErrorCategory != test.want {
				t.Fatalf("adversarial GeoJSON unexpectedly passed: %+v", validation)
			}
		})
	}

	t.Run("extra root field", func(t *testing.T) {
		body := geoJSON(valid)
		body["status"] = "ok"
		validation := validateGeoJSON(httpJSONResult(body), []string{"1", "2", "3"})
		if validation.Success || validation.ErrorCategory != "response_schema" {
			t.Fatalf("extra root field unexpectedly passed: %+v", validation)
		}
	})
	t.Run("wrong catalog value", func(t *testing.T) {
		corrupt := feature("1")
		corrupt["properties"].(map[string]any)["phone_number"] = "(000) 000-0000"
		validation := validateGeoJSON(httpJSONResult(geoJSON([]map[string]any{corrupt})), []string{"1"})
		if validation.Success || validation.ErrorCategory != "response_schema" {
			t.Fatalf("wrong catalog property unexpectedly passed: %+v", validation)
		}
	})
	t.Run("wrong coordinate", func(t *testing.T) {
		corrupt := feature("1")
		corrupt["geometry"].(map[string]any)["coordinates"] = []float32{-122.0, 37.0}
		validation := validateGeoJSON(httpJSONResult(geoJSON([]map[string]any{corrupt})), []string{"1"})
		if validation.Success || validation.ErrorCategory != "response_schema" {
			t.Fatalf("wrong coordinates unexpectedly passed: %+v", validation)
		}
	})
	t.Run("float64 coordinate", func(t *testing.T) {
		// The profile service stores float32: an answer computed from the
		// float64 geo coordinates is not the application's.
		hotel, _ := hotelsupport.DefaultReference().Hotel("1")
		corrupt := feature("1")
		corrupt["geometry"].(map[string]any)["coordinates"] = []float64{hotel.Lon + 1e-7, hotel.Lat}
		validation := validateGeoJSON(httpJSONResult(geoJSON([]map[string]any{corrupt})), []string{"1"})
		if validation.Success {
			t.Fatalf("non-float32 coordinates unexpectedly passed: %+v", validation)
		}
	})
	t.Run("extra feature field", func(t *testing.T) {
		corrupt := feature("1")
		corrupt["distance"] = 0
		validation := validateGeoJSON(httpJSONResult(geoJSON([]map[string]any{corrupt})), []string{"1"})
		if validation.Success || validation.ErrorCategory != "response_schema" {
			t.Fatalf("extra feature field unexpectedly passed: %+v", validation)
		}
	})
	t.Run("search requires the exact nearby set", func(t *testing.T) {
		application := newApplication(t, operationSearch)
		prepared, _ := application.Prepare(context.Background(), nil, api.TrialContext{})
		plan := build(t, application, operationSearch, api.Sample{Random: 99}, prepared)
		ids := plan.State.(*operationState).expectedIDs
		features := make([]map[string]any, 0, len(ids))
		for index := len(ids) - 1; index >= 0; index-- {
			features = append(features, feature(ids[index]))
		}
		if validation := validateGeoJSON(httpJSONResult(geoJSON(features)), ids); !validation.Success {
			t.Fatalf("reference search response failed: %+v", validation)
		}
		for mutationIndex := range features {
			mutated := append([]map[string]any(nil), features...)
			mutated = append(mutated[:mutationIndex], mutated[mutationIndex+1:]...)
			if validation := validateGeoJSON(httpJSONResult(geoJSON(mutated)), ids); validation.Success {
				t.Fatalf("search response without hotel %q unexpectedly passed", ids[len(ids)-1-mutationIndex])
			}
		}
	})
}

func TestMessageValidationRequiresExactSchemaAndText(t *testing.T) {
	if validation := validateMessage(httpJSONResult(map[string]any{"message": loginSuccess}), loginSuccess); !validation.Success {
		t.Fatalf("valid message failed: %+v", validation)
	}
	for _, body := range []any{
		map[string]any{"message": loginSuccess + " "},
		map[string]any{"message": loginSuccess, "ok": true},
		map[string]any{"message": true},
		[]any{loginSuccess},
	} {
		if validation := validateMessage(httpJSONResult(body), loginSuccess); validation.Success {
			t.Fatalf("non-exact message body unexpectedly passed: %v", body)
		}
	}
}

func TestReservationPlansSerializeAndUseUniqueHotelDateSlots(t *testing.T) {
	application := newApplication(t, operationReserveCapacity)
	data := &fixture{dateBase: time.Date(3050, time.January, 1, 12, 0, 0, 0, time.UTC)}
	operation := api.Operation{Name: operationReserveCapacity, Target: hotelsupport.GatewayTarget}
	first, err := application.BuildOperation(operation, api.Sample{Counter: 1, Random: 1}, data)
	if err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		plan api.OperationPlan
		err  error
	}
	ready := make(chan outcome, 1)
	go func() {
		// Counter and random word deliberately repeat, as at the warmup/measurement
		// boundary. The fixture ordinal must still allocate a new hotel/night slot.
		plan, buildErr := application.BuildOperation(operation, api.Sample{Counter: 1, Random: 1}, data)
		ready <- outcome{plan: plan, err: buildErr}
	}()
	select {
	case result := <-ready:
		if result.err == nil {
			application.FinishOperation(result.plan)
		}
		t.Fatal("conflicting reservation plan was not serialized")
	case <-time.After(30 * time.Millisecond):
	}
	firstQuery := first.Invocations[0].Payload.(api.HTTPRequestSpec).Query
	application.FinishOperation(first)
	application.FinishOperation(first)
	select {
	case result := <-ready:
		if result.err != nil {
			t.Fatal(result.err)
		}
		secondQuery := result.plan.Invocations[0].Payload.(api.HTTPRequestSpec).Query
		application.FinishOperation(result.plan)
		if firstQuery["inDate"] == secondQuery["inDate"] && firstQuery["hotelId"] == secondQuery["hotelId"] {
			t.Fatalf("distinct ordinals reused reservation slot hotel=%s date=%s", firstQuery["hotelId"], firstQuery["inDate"])
		}
	case <-time.After(time.Second):
		t.Fatal("reservation plan did not acquire lock after FinishOperation")
	}

	data.nextReservation.Store(reservationSlots)
	if _, err := application.BuildOperation(operation, api.Sample{Counter: 2, Random: 2}, data); err == nil {
		t.Fatal("a trial past its reservation slots was accepted")
	}
}

func TestFixtureDateNamespaceIsDeterministicAndSeedDifferentiated(t *testing.T) {
	application := &Application{}
	prepare := func(index int, seed int64) time.Time {
		value, err := application.Prepare(context.Background(), nil, api.TrialContext{Index: index, FixtureSeed: seed})
		if err != nil {
			t.Fatal(err)
		}
		return value.(*fixture).dateBase
	}
	first := prepare(2, 99)
	if first.Year() < 3000 || first.Year() >= 9800 {
		t.Fatalf("namespace date %s lies outside [3000, 9800)", first.Format(time.DateOnly))
	}
	if first != prepare(2, 99) {
		t.Fatal("same trial context produced a different date namespace")
	}
	if first == prepare(2, 100) {
		t.Fatalf("distinct fixture seeds collided at %s", first.Format(time.DateOnly))
	}
	days := daysBeforeYear(first.Year()) - daysBeforeYear(3000) + first.YearDay() - 1
	if days%reservationBlockDays != 0 {
		t.Fatalf("namespace date %s is not aligned to a %d-day block", first.Format(time.DateOnly), reservationBlockDays)
	}
	// Trials of one run take consecutive blocks: they never share a night.
	if next := prepare(3, 99); next != first.AddDate(0, 0, reservationBlockDays) && next.Year() != 3000 {
		t.Fatalf("trial 3 block starts %s, trial 2 %s", next.Format(time.DateOnly), first.Format(time.DateOnly))
	}
	last := first.AddDate(0, 0, reservationBlockDays-1)
	if !last.After(dataset.RateWindowEnd()) {
		t.Fatal("reservation nights overlap the rate window")
	}
}

func daysBeforeYear(year int) int {
	previous := year - 1
	return previous*365 + previous/4 - previous/100 + previous/400
}

func TestAllOperationsAgainstHTTPFake(t *testing.T) {
	operations := []string{
		operationSearch, operationRecommendDistance, operationRecommendRate, operationRecommendPrice,
		operationLoginValid, operationLoginInvalid, operationReserveCapacity,
	}
	application := newApplication(t, operations...)
	prepared, err := application.Prepare(context.Background(), nil, api.TrialContext{Index: 3, FixtureSeed: 42})
	if err != nil {
		t.Fatal(err)
	}
	target := newFakeHotel(t)
	runtime := &handlerRuntime{handler: target}
	for index, name := range operations {
		t.Run(name, func(t *testing.T) {
			for repeat := 0; repeat < 20; repeat++ {
				operation := api.Operation{Name: name, Target: hotelsupport.GatewayTarget}
				plan := build(t, application, name, api.Sample{Counter: int64(index*100 + repeat), Random: uint64(9182 + index*1000 + repeat)}, prepared)
				results := make([]api.ProtocolResult, len(plan.Invocations))
				for resultIndex, invocation := range plan.Invocations {
					results[resultIndex] = runtime.Invoke(context.Background(), invocation)
				}
				validation := application.ValidateOperation(operation, plan, results)
				application.FinishOperation(plan)
				if !validation.Success {
					t.Fatalf("validation failed: %+v", validation)
				}
			}
		})
	}
	if got, want := len(runtime.requests), 20*(len(operations)+1); got != want {
		t.Fatalf("HTTP requests = %d, want %d (reservation has two steps)", got, want)
	}
}

// fakeHotel answers from the dataset and its writes as the application
// would, with writes visible at once.
type fakeHotel struct {
	t            *testing.T
	mu           sync.Mutex
	reservations map[string]int
	state        *hotelsupport.State
	// staleProfiles serves every profile as a cache that is never
	// invalidated would: as first read.
	staleProfiles bool
	writes        []api.HTTPRequestSpec
}

func newFakeHotel(t *testing.T) http.Handler {
	handler, _ := newWritableFake(t)
	return handler
}

func newWritableFake(t *testing.T) (http.Handler, *fakeHotel) {
	fake := &fakeHotel{t: t, reservations: make(map[string]int), state: hotelsupport.NewState(hotelsupport.DefaultReference())}
	mux := http.NewServeMux()
	mux.HandleFunc("/hotels", fake.hotels)
	mux.HandleFunc("/recommendations", fake.recommendations)
	mux.HandleFunc("/user", fake.user)
	mux.HandleFunc("/reservation", fake.reservation)
	mux.HandleFunc("/rates", fake.rates)
	mux.HandleFunc("/admin/", fake.admin)
	return mux, fake
}

func (f *fakeHotel) hotels(writer http.ResponseWriter, request *http.Request) {
	query := singleQuery(request.URL.Query())
	if err := validateCanonicalSearchQuery(query); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ids, _, err := f.state.SearchCandidates(query["lat"], query["lon"])
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	f.writeFeatures(writer, ids)
}

func (f *fakeHotel) recommendations(writer http.ResponseWriter, request *http.Request) {
	query := singleQuery(request.URL.Query())
	if err := validateCanonicalRecommendationQuery(query, query["require"]); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	switch query["require"] {
	case "dis":
		lat, _ := strconv.ParseFloat(query["lat"], 64)
		lon, _ := strconv.ParseFloat(query["lon"], 64)
		ids = f.state.NearestHotels(lat, lon)
	case "rate":
		ids = f.state.BestRated()
	case "price":
		ids = f.state.Cheapest()
	default:
		http.Error(writer, "bad require", http.StatusBadRequest)
		return
	}
	f.writeFeatures(writer, ids)
}

func (f *fakeHotel) writeFeatures(writer http.ResponseWriter, ids []string) {
	ids = append([]string(nil), ids...)
	sort.Sort(sort.Reverse(sort.StringSlice(ids)))
	features := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		hotel, _ := f.state.Hotel(id)
		profile, _ := f.state.Profile(id)
		if f.staleProfiles {
			profile = hotelsupport.Profile{Name: hotel.Name, Phone: hotel.Phone}
		}
		features = append(features, map[string]any{
			"type": "Feature", "id": hotel.ID,
			"properties": map[string]any{"name": profile.Name, "phone_number": profile.Phone},
			"geometry":   map[string]any{"type": "Point", "coordinates": []float32{float32(hotel.Lon), float32(hotel.Lat)}},
		})
	}
	writeJSON(writer, geoJSON(features))
}

func (f *fakeHotel) rates(writer http.ResponseWriter, request *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := request.URL.Query().Get("hotelId")
	plans := []map[string]any{}
	for _, plan := range f.state.Plans(id) {
		plans = append(plans, map[string]any{
			"code": plan.Code, "inDate": plan.InDate, "outDate": plan.OutDate,
			"roomType": map[string]any{
				"code": plan.RoomCode, "roomDescription": plan.RoomDescription, "bookableRate": plan.BookableRate,
				"totalRate": plan.TotalRate, "totalRateInclusive": plan.TotalRateInclusive,
			},
		})
	}
	writeJSON(writer, map[string]any{"hotelId": id, "ratePlans": plans})
}

func (f *fakeHotel) admin(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "POST", http.StatusMethodNotAllowed)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	query := singleQuery(request.URL.Query())
	f.writes = append(f.writes, api.HTTPRequestSpec{Path: request.URL.Path, Query: query})
	number := func(key string) float64 {
		value, err := strconv.ParseFloat(query[key], 64)
		if err != nil {
			f.t.Errorf("%s %s=%q", request.URL.Path, key, query[key])
		}
		return value
	}
	var err error
	message := ""
	switch request.URL.Path {
	case "/admin/rate":
		err = f.state.SetPrices(query["hotelId"], query["code"], hotelsupport.Prices{
			Bookable: number("bookableRate"), Total: number("totalRate"), Inclusive: number("totalRateInclusive"),
		})
		message = rateUpdated
	case "/admin/profile":
		err = f.state.SetProfile(query["hotelId"], query["name"], query["phone"])
		message = profileUpdated
	case "/admin/capacity":
		err = f.state.SetCapacity(query["hotelId"], int(number("rooms")))
		message = capacityUpdated
	case "/admin/hotel":
		var body newHotelBody
		if decodeErr := json.NewDecoder(request.Body).Decode(&body); decodeErr != nil {
			http.Error(writer, decodeErr.Error(), http.StatusBadRequest)
			return
		}
		hotel := dataset.Hotel{ID: body.HotelID, Name: body.Name, Phone: body.PhoneNumber, Lat: body.Lat, Lon: body.Lon,
			Rating: body.Rating, Price: body.Price, Capacity: body.Rooms}
		for _, plan := range body.RatePlans {
			hotel.RatePlans = append(hotel.RatePlans, dataset.RatePlan{Code: plan.Code, InDate: plan.InDate, OutDate: plan.OutDate,
				RoomCode: plan.RoomType.Code, RoomDescription: plan.RoomType.RoomDescription,
				BookableRate: plan.RoomType.BookableRate, TotalRate: plan.RoomType.TotalRate, TotalRateInclusive: plan.RoomType.TotalRateInclusive})
		}
		err = f.state.Add(hotel)
		message = hotelAdded
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(writer, map[string]any{"message": message})
}

func (f *fakeHotel) user(writer http.ResponseWriter, request *http.Request) {
	message := loginFailure
	if hotelsupport.DefaultReference().ValidLogin(request.URL.Query().Get("username"), request.URL.Query().Get("password")) {
		message = loginSuccess
	}
	writeJSON(writer, map[string]any{"message": message})
}

func (f *fakeHotel) reservation(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	reference := hotelsupport.DefaultReference()
	if !reference.ValidLogin(query.Get("username"), query.Get("password")) {
		writeJSON(writer, map[string]any{"message": loginFailure})
		return
	}
	number, numberErr := strconv.Atoi(query.Get("number"))
	f.mu.Lock()
	capacity, known := f.state.Capacity(query.Get("hotelId"))
	f.mu.Unlock()
	var capacityErr error
	if !known {
		capacityErr = fmt.Errorf("unknown hotel")
	}
	if numberErr != nil || capacityErr != nil || query.Get("inDate") == "" || query.Get("outDate") == "" || query.Get("customerName") == "" {
		http.Error(writer, "invalid reservation query", http.StatusBadRequest)
		return
	}
	key := fmt.Sprintf("%s/%s/%s", query.Get("hotelId"), query.Get("inDate"), query.Get("outDate"))
	f.mu.Lock()
	defer f.mu.Unlock()
	message := reservationFailure
	if f.reservations[key]+number <= capacity {
		f.reservations[key] += number
		message = reservationSuccess
	}
	writeJSON(writer, map[string]any{"message": message})
}

type handlerRuntime struct {
	handler  http.Handler
	mu       sync.Mutex
	requests []api.HTTPRequestSpec
}

func (r *handlerRuntime) Invoke(_ context.Context, invocation api.Invocation) api.ProtocolResult {
	request, ok := invocation.Payload.(api.HTTPRequestSpec)
	if !ok {
		return api.ProtocolResult{ErrorCategory: "invalid_request", ErrorMessage: fmt.Sprintf("got %T", invocation.Payload)}
	}
	if invocation.Target != hotelsupport.GatewayTarget {
		return api.ProtocolResult{ErrorCategory: "invalid_target", ErrorMessage: invocation.Target}
	}
	r.mu.Lock()
	r.requests = append(r.requests, request)
	r.mu.Unlock()
	requestURL := "http://hotel.test" + request.Path
	if len(request.Query) != 0 {
		query := make(url.Values, len(request.Query))
		for key, value := range request.Query {
			query.Set(key, value)
		}
		requestURL += "?" + query.Encode()
	}
	httpRequest := httptest.NewRequest(request.Method, requestURL, strings.NewReader(request.Body))
	for key, value := range request.Headers {
		httpRequest.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	r.handler.ServeHTTP(recorder, httpRequest)
	return api.ProtocolResult{
		TransportSuccess: true,
		NativeStatus:     recorder.Result().Status,
		Payload: api.HTTPResponse{
			StatusCode: recorder.Code,
			Body:       append([]byte(nil), recorder.Body.Bytes()...),
		},
	}
}

func hotelWorkload(operations ...string) api.Workload {
	values := make([]api.Operation, len(operations))
	for index, name := range operations {
		values[index] = api.Operation{Name: name, Target: hotelsupport.GatewayTarget}
	}
	return api.Workload{
		Application: "hotel",
		Load: api.Load{
			TimeoutSeconds: 1, Repetitions: 1, Seed: 7,
		},
		Targets: []api.Target{{
			Name: hotelsupport.GatewayTarget, Protocol: "http", Address: "http://hotel.test", SessionPolicy: "reuse",
		}},
		Operations:        values,
		ApplicationConfig: map[string]any{},
	}
}

// feature is the frontend's GeoJSON feature for a dataset hotel: the
// profile's name, phone and float32 coordinates.
func feature(id string) map[string]any {
	hotel, ok := hotelsupport.DefaultReference().Hotel(id)
	if !ok {
		panic("unknown hotel " + id)
	}
	return map[string]any{
		"type": "Feature",
		"id":   hotel.ID,
		"properties": map[string]any{
			"name": hotel.Name, "phone_number": hotel.Phone,
		},
		"geometry": map[string]any{
			"type": "Point", "coordinates": []float32{float32(hotel.Lon), float32(hotel.Lat)},
		},
	}
}

func rawFeature(id string) map[string]any {
	return map[string]any{
		"type": "Feature", "id": id,
		"properties": map[string]any{"name": "unknown", "phone_number": "unknown"},
		"geometry":   map[string]any{"type": "Point", "coordinates": []float32{0, 0}},
	}
}

func geoJSON(features []map[string]any) map[string]any {
	return map[string]any{"type": "FeatureCollection", "features": features}
}

func httpJSONResult(value any) api.ProtocolResult {
	body, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return api.ProtocolResult{
		TransportSuccess: true,
		Payload:          api.HTTPResponse{StatusCode: http.StatusOK, Body: body},
	}
}

func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		panic(err)
	}
}

func singleQuery(values url.Values) map[string]string {
	result := make(map[string]string, len(values))
	for key, entries := range values {
		if len(entries) == 1 {
			result[key] = entries[0]
		}
	}
	return result
}
