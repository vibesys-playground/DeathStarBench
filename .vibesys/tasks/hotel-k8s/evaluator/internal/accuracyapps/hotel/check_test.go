package hotel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/microservice-evaluator/api"
)

// fakeHotel serves the four frontend routes with the pinned application's
// semantics over the task's dataset, keeping reservations in memory.
type fakeHotel struct {
	mu       sync.Mutex
	reserved map[string]int // hotel/night -> rooms
	// mutate, when set, corrupts one route's answer to prove the check fails.
	mutate func(path string, ids []string) []string
}

func (f *fakeHotel) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	reference := hotelsupport.DefaultReference()
	missing := func(keys ...string) bool {
		for _, key := range keys {
			if query.Get(key) == "" {
				http.Error(writer, "missing "+key, http.StatusBadRequest)
				return true
			}
		}
		return false
	}
	switch request.URL.Path {
	case "/hotels":
		if missing("inDate", "outDate", "lat", "lon") {
			return
		}
		ids, _, err := reference.SearchCandidates(query.Get("lat"), query.Get("lon"))
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		f.mu.Lock()
		available := []string{}
		for _, id := range ids {
			capacity, _ := reference.Capacity(id)
			free := true
			for _, night := range nights(query.Get("inDate"), query.Get("outDate")) {
				free = free && f.reserved[id+"/"+night]+1 <= capacity
			}
			if free {
				available = append(available, id)
			}
		}
		f.mu.Unlock()
		f.features(writer, "/hotels", available)
	case "/recommendations":
		if missing("lat", "lon") {
			return
		}
		lat, _ := strconv.ParseFloat(query.Get("lat"), 64)
		lon, _ := strconv.ParseFloat(query.Get("lon"), 64)
		switch query.Get("require") {
		case "dis":
			f.features(writer, "/recommendations", reference.NearestHotels(lat, lon))
		case "rate":
			f.features(writer, "/recommendations", reference.BestRated())
		case "price":
			f.features(writer, "/recommendations", reference.Cheapest())
		default:
			http.Error(writer, "Please specify require params", http.StatusBadRequest)
		}
	case "/user":
		if missing("username", "password") {
			return
		}
		message := loginFailure
		if reference.ValidLogin(query.Get("username"), query.Get("password")) {
			message = loginSuccess
		}
		writeMessage(writer, message)
	case "/reservation":
		if missing("inDate", "outDate") {
			return
		}
		if !dateShape(query.Get("inDate")) || !dateShape(query.Get("outDate")) {
			http.Error(writer, "bad date", http.StatusBadRequest)
			return
		}
		if missing("hotelId", "customerName", "username", "password") {
			return
		}
		rooms, _ := strconv.Atoi(query.Get("number"))
		hotelID := query.Get("hotelId")
		capacity, err := reference.Capacity(hotelID)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		stay := nights(query.Get("inDate"), query.Get("outDate"))
		for _, night := range stay {
			if f.reserved[hotelID+"/"+night]+rooms > capacity {
				writeMessage(writer, reservationFailure)
				return
			}
		}
		for _, night := range stay {
			f.reserved[hotelID+"/"+night] += rooms
		}
		// The pinned frontend reserves even when the login fails.
		if !reference.ValidLogin(query.Get("username"), query.Get("password")) {
			writeMessage(writer, loginFailure)
			return
		}
		writeMessage(writer, reservationSuccess)
	default:
		http.NotFound(writer, request)
	}
}

func (f *fakeHotel) features(writer http.ResponseWriter, path string, ids []string) {
	if f.mutate != nil {
		ids = f.mutate(path, ids)
	}
	reference := hotelsupport.DefaultReference()
	features := []any{}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		hotel, _ := reference.Hotel(id)
		features = append(features, map[string]any{
			"type": "Feature", "id": hotel.ID,
			"properties": map[string]string{"name": hotel.Name, "phone_number": hotel.Phone},
			"geometry": map[string]any{
				"type": "Point", "coordinates": []float32{float32(hotel.Lon), float32(hotel.Lat)},
			},
		})
	}
	json.NewEncoder(writer).Encode(map[string]any{"type": "FeatureCollection", "features": features})
}

func writeMessage(writer http.ResponseWriter, message string) {
	json.NewEncoder(writer).Encode(map[string]string{"message": message})
}

func dateShape(date string) bool {
	_, err := time.Parse(time.DateOnly, date)
	return err == nil
}

func nights(in, out string) []string {
	start, err1 := time.Parse(time.DateOnly, in)
	end, err2 := time.Parse(time.DateOnly, out)
	if err1 != nil || err2 != nil {
		return nil
	}
	var result []string
	for day := start; day.Before(end); day = day.AddDate(0, 0, 1) {
		result = append(result, day.Format(time.DateOnly))
	}
	return result
}

type handlerRuntime struct{ handler http.Handler }

func (r handlerRuntime) Invoke(_ context.Context, invocation api.Invocation) api.ProtocolResult {
	request := invocation.Payload.(api.HTTPRequestSpec)
	values := url.Values{}
	for key, value := range request.Query {
		values.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	r.handler.ServeHTTP(recorder, httptest.NewRequest(request.Method, "http://hotel.test"+request.Path+"?"+values.Encode(), nil))
	return api.ProtocolResult{
		TransportSuccess: true,
		NativeStatus:     recorder.Result().Status,
		Payload:          api.HTTPResponse{StatusCode: recorder.Code, Body: recorder.Body.Bytes()},
	}
}

type recorder struct {
	checks int
	passed []string
}

func (r *recorder) AddChecks(n int) { r.checks += n }

func (r *recorder) Pass(properties ...string) error {
	r.passed = append(r.passed, properties...)
	return nil
}

func workload() api.Workload {
	return api.Workload{
		Load: api.Load{TimeoutSeconds: 5, Repetitions: 1, Seed: 48},
		Targets: []api.Target{{
			Name: hotelsupport.GatewayTarget, Protocol: "http", SessionPolicy: "reuse",
		}},
		Operations: []api.Operation{{Name: "search_hotels", Target: hotelsupport.GatewayTarget}},
	}
}

func runCheck(t *testing.T, fake *fakeHotel, seed int64) (*recorder, error) {
	t.Helper()
	application, err := New(workload())
	if err != nil {
		t.Fatal(err)
	}
	result := &recorder{}
	noop := func(context.Context) error { return nil }
	err = application.Check(context.Background(), handlerRuntime{fake}, api.AccuracyContext{
		Seed: seed, Cases: 4, Crash: noop, Start: noop,
	}, result)
	return result, err
}

func TestCheckPassesAReferenceCandidate(t *testing.T) {
	for _, seed := range []int64{1, 48, 20261104} {
		result, err := runCheck(t, &fakeHotel{reserved: map[string]int{}}, seed)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		want := []string{
			"strict_geojson_schema", "search_semantics", "seeded_profile_semantics", "recommendation_semantics",
			"authentication_semantics", "sequential_differential", "search_availability",
			"reservation_optional_number", "reservation_capacity", "read_your_write", "reservation_isolation",
			"degenerate_date_ranges", "multi_night_atomicity", "concurrent_isolation", "crash_recovery",
			"durable_capacity",
		}
		sort.Strings(want)
		got := append([]string(nil), result.passed...)
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("seed %d passed %v, want %v", seed, got, want)
		}
		t.Logf("seed %d: %d checks", seed, result.checks)
	}
}

func TestCheckRejectsWrongAnswers(t *testing.T) {
	tests := map[string]func(path string, ids []string) []string{
		// An exact nearest-neighbour geo (or any other) that changes one search answer.
		"search drops a hotel": func(path string, ids []string) []string {
			if path == "/hotels" && len(ids) > 1 {
				return ids[1:]
			}
			return ids
		},
		"search adds a hotel": func(path string, ids []string) []string {
			if path == "/hotels" {
				return append(ids, "25000")
			}
			return ids
		},
		"recommendation answers another hotel": func(path string, ids []string) []string {
			if path == "/recommendations" {
				return []string{"1"}
			}
			return ids
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := runCheck(t, &fakeHotel{reserved: map[string]int{}, mutate: mutate}, 7); err == nil {
				t.Fatal("check passed a wrong candidate")
			}
		})
	}
}

func TestCatalogAndSearchableHotelsComeFromTheDataset(t *testing.T) {
	catalog, err := seedProfiles()
	if err != nil {
		t.Fatal(err)
	}
	data := hotelsupport.DefaultReference().Data
	if len(catalog) != len(data.Hotels) {
		t.Fatalf("catalog has %d profiles", len(catalog))
	}
	searchable := sortedRateBackedIDs()
	if len(searchable) < len(data.Hotels)*99/100 {
		t.Fatalf("only %d of %d hotels are found by a search at their own location", len(searchable), len(data.Hotels))
	}
	t.Logf("%d of %d hotels are searchable at their own location", len(searchable), len(data.Hotels))
}
