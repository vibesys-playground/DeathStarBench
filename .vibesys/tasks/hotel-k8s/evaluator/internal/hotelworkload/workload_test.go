package hotelworkload

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/workload"

	"vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// TestMain needs the task's dataset file, which dataset.py writes:
//
//	python dataset.py /tmp/dataset.json; HOTEL_DATASET=/tmp/dataset.json go test ./...
//
// Without it the package's tests are skipped.
func TestMain(m *testing.M) {
	if _, err := dataset.Load(); err != nil {
		fmt.Printf("skipping %s: %v\n", "internal/hotelworkload", err)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func newModel(t *testing.T) *Model {
	t.Helper()
	m, err := NewModel(dataset.Default(), workload.NewRand(7, "model"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func operations() map[string]workload.Operation[*Model] {
	ops := map[string]workload.Operation[*Model]{}
	for _, op := range Workload().Operations {
		ops[op.Name] = op
	}
	return ops
}

func build(t *testing.T, m *Model, name string, n int) (workload.Request, json.RawMessage) {
	t.Helper()
	request := operations()[name].Build(m, workload.NewRand(7, "phase", 0).Child(n))
	info, err := json.Marshal(request.Info)
	if err != nil {
		t.Fatal(err)
	}
	if request.Info == nil {
		info = []byte("{}")
	}
	return request, info
}

func pending(info json.RawMessage) workload.Call { return workload.Call{Info: info, Pending: true} }

var t0 = time.Unix(100, 0)

func at(secs float64) time.Time { return t0.Add(time.Duration(secs * float64(time.Second))) }

// done is a call that ran over [start, end] seconds after t0 and answered bodies (JSON values).
func done(t *testing.T, info any, start, end float64, bodies ...any) workload.Call {
	t.Helper()
	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	call := workload.Call{Info: raw, Start: at(start), End: at(end), OK: true}
	for _, b := range bodies {
		encoded, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		call.Responses = append(call.Responses, workload.Response{Status: 200, Body: encoded})
	}
	return call
}

func geojson(m *Model, ids []string, profiles map[string]hotel.Profile) map[string]any {
	features := []any{}
	for _, id := range ids {
		h, _ := m.state.Hotel(id)
		profile, ok := profiles[id]
		if !ok {
			profile = hotel.Profile{Name: h.Name, Phone: h.Phone}
		}
		lon, lat := hotel.EncodedCoordinates(h)
		features = append(features, map[string]any{
			"type": "Feature", "id": id,
			"properties": map[string]any{"name": profile.Name, "phone_number": profile.Phone},
			"geometry":   map[string]any{"type": "Point", "coordinates": []float64{lon, lat}},
		})
	}
	return map[string]any{"type": "FeatureCollection", "features": features}
}

func TestTheMixIsTheOldWorkloads(t *testing.T) {
	want := map[string]float64{
		"search_hotels": 6000, "recommend_distance": 1300, "recommend_rate": 1300, "recommend_price": 1300,
		"login_valid": 30, "login_invalid": 20, "reserve_capacity": 50, "hotel_rates": 400,
		"update_rate": 42, "update_profile": 42, "set_capacity": 12, "add_hotel": 8,
	}
	ops := operations()
	if len(ops) != len(want) {
		t.Fatalf("%d operations, want %d", len(ops), len(want))
	}
	for name, weight := range want {
		if ops[name].Weight != weight {
			t.Errorf("%s weighs %v, want %v", name, ops[name].Weight, weight)
		}
		if ops[name].Check == nil {
			t.Errorf("%s is never checked", name)
		}
	}
}

func TestEveryOperationBuildsAndAppliesItsWrites(t *testing.T) {
	m := newModel(t)
	for name, op := range operations() {
		for n := range 30 {
			request, info := build(t, m, name, n)
			if len(request.Steps) == 0 {
				t.Fatalf("%s: no steps", name)
			}
			if op.Apply != nil {
				if err := op.Apply(m, pending(info)); err != nil {
					t.Fatalf("%s %d: %v", name, n, err)
				}
			}
		}
	}
}

func TestReservationsFillDistinctHotelsPerNight(t *testing.T) {
	m := newModel(t)
	op := operations()["reserve_capacity"]
	seen := map[[2]int]bool{}
	for n := range 3 * reservationsPerNight {
		slot := m.nextSlot
		_, raw := build(t, m, "reserve_capacity", n)
		var info reservationInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			t.Fatal(err)
		}
		if info.Slot != slot || CapacityManaged(info.Hotel) {
			t.Fatalf("reservation %d: %+v (slot %d)", n, info, slot)
		}
		key := [2]int{slot / reservationsPerNight, info.Hotel}
		if seen[key] {
			t.Fatalf("hotel %d twice on night %d", info.Hotel, key[0])
		}
		seen[key] = true
		if err := op.Apply(m, pending(raw)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCapacityWritesGoToManagedHotels(t *testing.T) {
	m := newModel(t)
	for n := range 50 {
		_, raw := build(t, m, "set_capacity", n)
		var info capacityInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			t.Fatal(err)
		}
		h, _ := m.ref.Hotel(info.Hotel)
		index, _ := strconv.Atoi(h.ID)
		if !CapacityManaged(index-1) || info.Rooms < max(1, h.Capacity/2) || info.Rooms > 2*h.Capacity {
			t.Fatalf("capacity write %+v of hotel with %d rooms", info, h.Capacity)
		}
	}
}

func TestSearchCheckAcceptsTheReferenceAndRejectsOthers(t *testing.T) {
	m := newModel(t)
	_, raw := build(t, m, "search_hotels", 3)
	var info expectedInfo
	if err := json.Unmarshal(raw, &info); err != nil || len(info.Expected) == 0 {
		t.Fatalf("search info %s: %v", raw, err)
	}
	if err := m.checkSearch(done(t, info, 0, 0.1, geojson(m, info.Expected, nil))); err != nil {
		t.Fatal(err)
	}
	wrong := append(append([]string(nil), info.Expected[:len(info.Expected)-1]...), "1")
	if err := m.checkSearch(done(t, info, 0, 0.1, geojson(m, wrong, nil))); err == nil {
		t.Fatal("a wrong answer passed")
	}
}

func TestProfileWritesAreVisibleWithinTheBound(t *testing.T) {
	m := newModel(t)
	h := &m.data.Hotels[0]
	write := done(t, profileInfo{Hotel: h.ID, Name: h.Name, Phone: "new"}, 10, 10.2, map[string]string{"message": "Profile updated."})
	if err := m.applyProfile(write); err != nil {
		t.Fatal(err)
	}
	ids, _, err := m.ref.SearchCandidates(coordinate(h.Lat), coordinate(h.Lon))
	if err != nil {
		t.Fatal(err)
	}
	info := expectedInfo{ids}
	old, updated := hotel.Profile{Name: h.Name, Phone: h.Phone}, hotel.Profile{Name: h.Name, Phone: "new"}
	cases := []struct {
		start   float64
		profile hotel.Profile
		ok      bool
	}{{10.5, old, true}, {11.5, updated, true}, {11.5, old, false}, {10.5, updated, true}}
	for _, c := range cases {
		err := m.checkSearch(done(t, info, c.start, c.start+0.1, geojson(m, ids, map[string]hotel.Profile{h.ID: c.profile})))
		if (err == nil) != c.ok {
			t.Errorf("read at %v showing %v: %v", c.start, c.profile, err)
		}
	}
}

func ratesBody(h *dataset.Hotel) map[string]any {
	var plans []any
	for i := len(h.RatePlans) - 1; i >= 0; i-- {
		p := h.RatePlans[i]
		plans = append(plans, map[string]any{"code": p.Code, "inDate": p.InDate, "outDate": p.OutDate,
			"roomType": map[string]any{"bookableRate": p.BookableRate, "code": p.RoomCode, "roomDescription": p.RoomDescription,
				"totalRate": p.TotalRate, "totalRateInclusive": p.TotalRateInclusive}})
	}
	return map[string]any{"hotelId": h.ID, "ratePlans": plans}
}

func TestRatesCheck(t *testing.T) {
	m := newModel(t)
	h := &m.data.Hotels[5]
	body := ratesBody(h)
	if err := m.checkRates(done(t, hotelInfo{h.ID}, 0, 0.1, body)); err != nil {
		t.Fatal(err)
	}
	room := body["ratePlans"].([]any)[0].(map[string]any)["roomType"].(map[string]any)
	room["totalRate"] = room["totalRate"].(float64) + 1
	if err := m.checkRates(done(t, hotelInfo{h.ID}, 0, 0.1, body)); err == nil {
		t.Fatal("a wrong price passed")
	}
}

func TestRateWritesAreVisibleWithinTheBound(t *testing.T) {
	m := newModel(t)
	h := &m.data.Hotels[5]
	plan := h.RatePlans[0]
	prices := [3]float64{plan.TotalRate + 1, plan.TotalRate + 1, plan.TotalRateInclusive + 1}
	if err := m.applyRate(done(t, rateInfo{Hotel: h.ID, Code: plan.Code, Prices: prices}, 10, 10.2)); err != nil {
		t.Fatal(err)
	}
	updated := ratesBody(h)
	for _, p := range updated["ratePlans"].([]any) {
		if p := p.(map[string]any); p["code"] == plan.Code {
			room := p["roomType"].(map[string]any)
			room["bookableRate"], room["totalRate"], room["totalRateInclusive"] = prices[0], prices[1], prices[2]
		}
	}
	for _, c := range []struct {
		start float64
		body  map[string]any
		ok    bool
	}{{10.5, ratesBody(h), true}, {10.5, updated, true}, {11.5, updated, true}, {11.5, ratesBody(h), false}} {
		if err := m.checkRates(done(t, hotelInfo{h.ID}, c.start, c.start+0.1, c.body)); (err == nil) != c.ok {
			t.Errorf("read at %v: %v", c.start, err)
		}
	}
}

func TestMessagesAreExact(t *testing.T) {
	check := operations()["login_valid"].Check
	for body, ok := range map[string]bool{
		`{"message":"Login successfully!"}`:       true,
		`{"message":"Login successfully!","x":1}`: false,
		`{"message":"Login successfully"}`:        false,
		`{"message":1}`:                           false,
		`not json`:                                false,
	} {
		call := workload.Call{Info: json.RawMessage(`{}`), OK: true, Responses: []workload.Response{{Status: 200, Body: []byte(body)}}}
		if err := check(nil, call); (err == nil) != ok {
			t.Errorf("%s: %v", body, err)
		}
	}
}

// An addition acknowledged in an earlier phase must be answered near it; one written during the
// read may or may not be.
func TestSandboxReadsSeeTheAdditionsTheWritesAllow(t *testing.T) {
	m := newModel(t)
	for n := range 2 {
		_, raw := build(t, m, "add_hotel", n)
		var info additionInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			t.Fatal(err)
		}
		start := 10.0 + 5*float64(n) // the first is acknowledged long before the reads, the second during them
		if err := m.applyAddition(done(t, info, start, start+0.1)); err != nil {
			t.Fatal(err)
		}
	}
	first, second := m.added[0], m.added[1]
	lat, lon := coordinate(first.Lat), coordinate(first.Lon)
	call := func(ids []string) workload.Call {
		return done(t, nearInfo{Near: [2]string{lat, lon}}, 15, 15.05, geojson(m, ids, nil))
	}
	exact := func(hotels ...*dataset.Hotel) []string {
		ids, _, err := m.ref.SearchCandidatesWith(lat, lon, hotel.NewAdditions(hotels...))
		if err != nil {
			t.Fatal(err)
		}
		return ids
	}
	if err := m.checkSearch(call(exact(first))); err != nil {
		t.Errorf("only the acknowledged addition: %v", err)
	}
	if err := m.checkSearch(call(exact(first, second))); err != nil {
		t.Errorf("both additions: %v", err)
	}
	if err := m.checkSearch(call([]string{})); err == nil {
		t.Error("an answer without the acknowledged addition passed")
	}
	near := locationInfo{Lat: lat, Lon: lon, Sandbox: true}
	latitude, _ := strconv.ParseFloat(lat, 64)
	longitude, _ := strconv.ParseFloat(lon, 64)
	nearest := m.ref.NearestHotelsWith(latitude, longitude, hotel.NewAdditions(first))
	check := checkRecommendation("dis")
	if err := check(m, done(t, near, 15, 15.05, geojson(m, nearest, nil))); err != nil {
		t.Errorf("distance recommendation of the nearest: %v", err)
	}
	if err := check(m, done(t, near, 15, 15.05, geojson(m, m.ref.NearestHotels(latitude, longitude), nil))); err == nil {
		t.Error("a distance recommendation without the acknowledged addition passed")
	}
}

// BenchmarkBuild is the cost of drawing one operation of the mix and building its request (the
// generator's stream adds its encoding).
func BenchmarkBuild(b *testing.B) {
	m, err := NewModel(dataset.Default(), workload.NewRand(7, "model"))
	if err != nil {
		b.Fatal(err)
	}
	w := Workload()
	weights := make([]float64, len(w.Operations))
	for i, op := range w.Operations {
		weights[i] = op.Weight
	}
	stream := workload.NewRand(7, "phase", 0)
	b.ResetTimer()
	for n := range b.N {
		op := &w.Operations[stream.Weighted(weights)]
		request := op.Build(m, stream.Child(n))
		if op.Apply != nil {
			info, _ := json.Marshal(request.Info)
			if err := op.Apply(m, pending(info)); err != nil {
				b.Fatal(err)
			}
		}
	}
}
