package hotel

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
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

// fakeHotel serves the frontend routes with the base application's
// semantics over the task's dataset and its writes, keeping reservations in
// memory. Requests alternate between two replicas; a fault makes replica 1
// misbehave the way a candidate's cache or index might.
type fakeHotel struct {
	mu       sync.Mutex
	reserved map[string]int // hotel/night -> rooms
	state    *hotelsupport.State
	faults   faults
	next     int
	// mutate, when set, corrupts one route's answer to prove the check fails.
	mutate func(path string, ids []string) []string
}

// faults are candidate bugs the write checks must catch.
type faults struct {
	// Replica 1 keeps what it cached before any write, forever: profiles,
	// rate plans, capacities, or a geo and recommendation index without the
	// added hotels.
	staleProfiles, staleRates, staleCapacity, staleGeo bool
	// volatile loses every write in a restart.
	volatile bool
	// oversell accepts a capacity below the rooms already reserved.
	oversell bool
}

func newFake() *fakeHotel {
	return &fakeHotel{reserved: map[string]int{}, state: hotelsupport.NewState(hotelsupport.DefaultReference())}
}

func (f *fakeHotel) crash() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.faults.volatile {
		f.state = hotelsupport.NewState(hotelsupport.DefaultReference())
	}
}

// capacity is the capacity a replica uses.
func (f *fakeHotel) capacity(replica int, id string) (int, bool) {
	if f.faults.staleCapacity && replica == 1 {
		hotel, ok := f.state.Hotel(id)
		if !ok {
			return 0, false
		}
		return hotel.Capacity, true
	}
	return f.state.Capacity(id)
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
	f.mu.Lock()
	defer f.mu.Unlock()
	replica := f.next % 2
	f.next++
	staleGeo := f.faults.staleGeo && replica == 1
	if strings.HasPrefix(request.URL.Path, "/admin/") {
		f.admin(writer, request)
		return
	}
	switch request.URL.Path {
	case "/hotels":
		if missing("inDate", "outDate", "lat", "lon") {
			return
		}
		search := f.state.SearchCandidates
		if staleGeo {
			search = reference.SearchCandidates
		}
		ids, _, err := search(query.Get("lat"), query.Get("lon"))
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		available := []string{}
		for _, id := range ids {
			capacity, _ := f.capacity(replica, id)
			free := true
			for _, night := range nights(query.Get("inDate"), query.Get("outDate")) {
				free = free && f.reserved[id+"/"+night]+1 <= capacity
			}
			if free {
				available = append(available, id)
			}
		}
		f.features(writer, replica, "/hotels", available)
	case "/recommendations":
		if missing("lat", "lon") {
			return
		}
		lat, _ := strconv.ParseFloat(query.Get("lat"), 64)
		lon, _ := strconv.ParseFloat(query.Get("lon"), 64)
		var ids []string
		switch query.Get("require") {
		case "dis":
			ids = f.state.NearestHotels(lat, lon)
			if staleGeo {
				ids = reference.NearestHotels(lat, lon)
			}
		case "rate":
			ids = f.state.BestRated()
			if staleGeo {
				ids = reference.BestRated()
			}
		case "price":
			ids = f.state.Cheapest()
			if staleGeo {
				ids = reference.Cheapest()
			}
		default:
			http.Error(writer, "Please specify require params", http.StatusBadRequest)
			return
		}
		f.features(writer, replica, "/recommendations", ids)
	case "/rates":
		if missing("hotelId") {
			return
		}
		id := query.Get("hotelId")
		plans := f.state.Plans(id)
		if hotel, ok := f.state.Hotel(id); ok && f.faults.staleRates && replica == 1 {
			plans = hotel.RatePlans
		}
		encoded := []any{}
		for _, plan := range plans {
			encoded = append(encoded, map[string]any{
				"code": plan.Code, "inDate": plan.InDate, "outDate": plan.OutDate,
				"roomType": map[string]any{
					"code": plan.RoomCode, "roomDescription": plan.RoomDescription, "bookableRate": plan.BookableRate,
					"totalRate": plan.TotalRate, "totalRateInclusive": plan.TotalRateInclusive,
				},
			})
		}
		json.NewEncoder(writer).Encode(map[string]any{"hotelId": id, "ratePlans": encoded})
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
		capacity, ok := f.capacity(replica, hotelID)
		if !ok {
			http.Error(writer, "unknown hotel", http.StatusInternalServerError)
			return
		}
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

var fakeHotelID = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)

// admin serves the owner routes as the base application does.
func (f *fakeHotel) admin(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "POST only", http.StatusMethodNotAllowed)
		return
	}
	value := request.FormValue
	amount := func(key string) (float64, bool) {
		number, err := strconv.ParseFloat(value(key), 64)
		return number, err == nil && number >= 0 && !math.IsInf(number, 0)
	}
	fail := func(status int) { http.Error(writer, http.StatusText(status), status) }
	switch request.URL.Path {
	case "/admin/rate":
		bookable, ok1 := amount("bookableRate")
		total, ok2 := amount("totalRate")
		inclusive, ok3 := amount("totalRateInclusive")
		if value("hotelId") == "" || value("code") == "" || !ok1 || !ok2 || !ok3 {
			fail(http.StatusBadRequest)
			return
		}
		if err := f.state.SetPrices(value("hotelId"), value("code"), hotelsupport.Prices{
			Bookable: bookable, Total: total, Inclusive: inclusive,
		}); err != nil {
			fail(http.StatusNotFound)
			return
		}
		writeMessage(writer, rateUpdated)
	case "/admin/profile":
		if value("hotelId") == "" || (value("name") == "" && value("phone") == "") {
			fail(http.StatusBadRequest)
			return
		}
		if err := f.state.SetProfile(value("hotelId"), value("name"), value("phone")); err != nil {
			fail(http.StatusNotFound)
			return
		}
		writeMessage(writer, profileUpdated)
	case "/admin/capacity":
		id := value("hotelId")
		rooms, err := strconv.Atoi(value("rooms"))
		if id == "" || err != nil || rooms < 0 {
			fail(http.StatusBadRequest)
			return
		}
		if _, ok := f.state.Hotel(id); !ok {
			fail(http.StatusNotFound)
			return
		}
		for key, reserved := range f.reserved {
			if strings.HasPrefix(key, id+"/") && reserved > rooms && !f.faults.oversell {
				fail(http.StatusConflict)
				return
			}
		}
		f.state.SetCapacity(id, rooms)
		writeMessage(writer, capacityUpdated)
	case "/admin/hotel":
		var body struct {
			HotelID, Name, PhoneNumber, Description string
			Address                                 map[string]string
			Lat, Lon, Rating, Price                 float64
			Rooms                                   int
			RatePlans                               []struct {
				Code, InDate, OutDate string
				RoomType              struct {
					Code, RoomDescription                       string
					BookableRate, TotalRate, TotalRateInclusive float64
				}
			}
		}
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil || !fakeHotelID.MatchString(body.HotelID) || body.Name == "" {
			fail(http.StatusBadRequest)
			return
		}
		if _, taken := f.state.Hotel(body.HotelID); taken {
			fail(http.StatusConflict)
			return
		}
		hotel := dataset.Hotel{
			ID: body.HotelID, City: -1, Name: body.Name, Phone: body.PhoneNumber, Description: body.Description,
			Lat: body.Lat, Lon: body.Lon, Rating: body.Rating, Price: body.Price, Capacity: body.Rooms,
		}
		for _, plan := range body.RatePlans {
			hotel.RatePlans = append(hotel.RatePlans, dataset.RatePlan{
				Code: plan.Code, InDate: plan.InDate, OutDate: plan.OutDate,
				RoomCode: plan.RoomType.Code, RoomDescription: plan.RoomType.RoomDescription,
				BookableRate: plan.RoomType.BookableRate, TotalRate: plan.RoomType.TotalRate,
				TotalRateInclusive: plan.RoomType.TotalRateInclusive,
			})
		}
		f.state.Add(hotel)
		writeMessage(writer, hotelAdded)
	default:
		http.NotFound(writer, request)
	}
}

func (f *fakeHotel) features(writer http.ResponseWriter, replica int, path string, ids []string) {
	if f.mutate != nil {
		ids = f.mutate(path, ids)
	}
	features := []any{}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		hotel, _ := f.state.Hotel(id)
		profile, _ := f.state.Profile(id)
		if f.faults.staleProfiles && replica == 1 {
			profile = hotelsupport.Profile{Name: hotel.Name, Phone: hotel.Phone}
		}
		features = append(features, map[string]any{
			"type": "Feature", "id": hotel.ID,
			"properties": map[string]string{"name": profile.Name, "phone_number": profile.Phone},
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
	httpRequest := httptest.NewRequest(
		request.Method, "http://hotel.test"+request.Path+"?"+values.Encode(), strings.NewReader(request.Body),
	)
	for key, value := range request.Headers {
		httpRequest.Header.Set(key, value)
	}
	r.handler.ServeHTTP(recorder, httpRequest)
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
	crash := func(context.Context) error { fake.crash(); return nil }
	start := func(context.Context) error { return nil }
	err = application.Check(context.Background(), handlerRuntime{fake}, api.AccuracyContext{
		Seed: seed, Cases: 4, Crash: crash, Start: start,
	}, result)
	return result, err
}

func TestCheckPassesAReferenceCandidate(t *testing.T) {
	for _, seed := range []int64{1, 48, 20261104} {
		result, err := runCheck(t, newFake(), seed)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		want := []string{
			"strict_geojson_schema", "search_semantics", "seeded_profile_semantics", "recommendation_semantics",
			"authentication_semantics", "sequential_differential", "search_availability",
			"reservation_optional_number", "reservation_capacity", "read_your_write", "reservation_isolation",
			"degenerate_date_ranges", "multi_night_atomicity", "concurrent_isolation", "crash_recovery",
			"durable_capacity", "write_validation", "write_rate_visibility", "write_profile_visibility",
			"write_capacity", "write_hotel_addition", "durable_writes",
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
			fake := newFake()
			fake.mutate = mutate
			if _, err := runCheck(t, fake, 7); err == nil {
				t.Fatal("check passed a wrong candidate")
			}
		})
	}
}

// TestCheckRejectsStaleReplicas: a replica whose cache or index never sees a
// write, or a restart that loses writes, fails the write checks.
func TestCheckRejectsStaleReplicas(t *testing.T) {
	tests := map[string]struct {
		faults faults
		// properties are the checks that may catch the fault first.
		properties []string
	}{
		"profile cache never invalidated":   {faults{staleProfiles: true}, []string{"write_profile_visibility"}},
		"rate cache never invalidated":      {faults{staleRates: true}, []string{"write_rate_visibility"}},
		"capacity cached for reservations":  {faults{staleCapacity: true}, []string{"write_capacity"}},
		"geo and recommendation never load": {faults{staleGeo: true}, []string{"write_capacity", "write_hotel_addition"}},
		"writes lost in a restart":          {faults{volatile: true}, []string{"durable_writes"}},
		"capacity below the reserved rooms": {faults{oversell: true}, []string{"write_capacity"}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			for _, seed := range []int64{7, 48} {
				fake := newFake()
				fake.faults = test.faults
				result, err := runCheck(t, fake, seed)
				if err == nil {
					t.Fatalf("seed %d: check passed a stale candidate", seed)
				}
				property, _, _ := strings.Cut(err.Error(), ": ")
				if !slices.Contains(test.properties, property) || slices.Contains(result.passed, property) {
					t.Fatalf("seed %d: failed %v, want one of %v", seed, err, test.properties)
				}
				t.Logf("seed %d: %v", seed, err)
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
