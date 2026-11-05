package hotel

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/api"
)

func TestHistoryAllowsTheValuesTheBoundAllows(t *testing.T) {
	at := func(ms int) time.Time { return time.Unix(1000, 0).Add(time.Duration(ms) * time.Millisecond) }
	h := &history[string]{base: "seed"}
	check := func(start, end int, want ...string) {
		t.Helper()
		if got := h.allowed(at(start), at(end)); !reflect.DeepEqual(got, want) {
			t.Fatalf("read [%d, %d] ms: allowed %v, want %v", start, end, got, want)
		}
	}
	check(0, 5, "seed")
	h.begin("a", at(100))
	check(50, 90, "seed")       // built after the read answered
	check(50, 150, "seed", "a") // in flight while the read ran
	h.finish(at(120), true)
	check(1100, 1110, "seed", "a") // acknowledged less than 1 s before the read
	check(1121, 1130, "a")         // more than 1 s: only the new value
	h.begin("b", at(2000))
	check(2500, 2600, "a", "b")
	h.finish(at(2010), false) // failed: may still have been applied
	check(5000, 5001, "a", "b")
	h.begin("c", at(6000))
	h.finish(at(6010), true)
	check(7011, 7020, "c")
	if h.latest() != "c" || h.busy {
		t.Fatalf("latest %q busy %v", h.latest(), h.busy)
	}
}

// run builds, executes against the fake, validates and finishes one plan.
func run(t *testing.T, application *Application, runtime *handlerRuntime, prepared any, name string, random uint64) api.ValidationResult {
	t.Helper()
	operation := api.Operation{Name: name, Target: hotelsupport.GatewayTarget}
	plan := build(t, application, name, api.Sample{Counter: int64(random >> 1), Random: random}, prepared)
	defer application.FinishOperation(plan)
	results := make([]api.ProtocolResult, len(plan.Invocations))
	for index, invocation := range plan.Invocations {
		results[index] = runtime.Invoke(context.Background(), invocation)
	}
	return application.ValidateOperation(operation, plan, results)
}

var allOperations = []string{
	operationSearch, operationRecommendDistance, operationRecommendRate, operationRecommendPrice,
	operationLoginValid, operationLoginInvalid, operationReserveCapacity, operationRates,
	operationUpdateRate, operationUpdateProfile, operationSetCapacity, operationAddHotel,
}

func TestMixedReadsAndWritesAgainstAFake(t *testing.T) {
	application := newApplication(t, allOperations...)
	prepared, _ := application.Prepare(context.Background(), nil, api.TrialContext{Index: 1, FixtureSeed: 11})
	handler, fake := newWritableFake(t)
	runtime := &handlerRuntime{handler: handler}
	for _, name := range []string{operationAddHotel, operationAddHotel, operationUpdateProfile, operationUpdateRate, operationSetCapacity} {
		for random := uint64(1); random <= 30; random++ {
			if validation := run(t, application, runtime, prepared, name, random*0x9e3779b97f4a7c15); !validation.Success {
				t.Fatalf("%s: %+v", name, validation)
			}
		}
	}
	// Reads see the written values (the fake applies writes at once, which
	// the window allows) and, a second later, nothing else.
	time.Sleep(StalenessBound + 50*time.Millisecond)
	sandboxReads := 0
	for random := uint64(1); random <= 3000; random++ {
		name := allOperations[int(random)%len(allOperations)]
		if name == operationAddHotel && random%7 != 0 {
			continue
		}
		before := len(runtime.requests)
		validation := run(t, application, runtime, prepared, name, random*0x2545f4914f6cdd1d)
		if !validation.Success {
			t.Fatalf("%s sample %d: %+v", name, random, validation)
		}
		request := runtime.requests[before]
		lat, _ := strconv.ParseFloat(request.Query["lat"], 64)
		lon, _ := strconv.ParseFloat(request.Query["lon"], 64)
		if (name == operationSearch || name == operationRecommendDistance) && lat < -19 && lon < -84 {
			sandboxReads++ // the sandbox region, far from every seeded city
		}
	}
	if sandboxReads == 0 {
		t.Fatal("no read queried the sandbox")
	}
	data := prepared.(*fixture)
	if len(data.added) < 60 || !data.added[0].ok {
		t.Fatalf("additions %d", len(data.added))
	}
	// Every profile and capacity write went to a hotel the model knows.
	if len(application.writes.profiles) == 0 || len(application.writes.prices) == 0 || len(application.writes.capacity) == 0 {
		t.Fatal("writes not recorded")
	}
	for id := range application.writes.capacity {
		hotel, _ := application.reference.Hotel(id)
		index, _ := strconv.Atoi(hotel.ID)
		if !capacityManaged(index - 1) {
			t.Fatalf("capacity write to hotel %s outside the managed quarter", id)
		}
	}

	// Reset restores the seeded values through the admin routes.
	if err := application.Reset(context.Background(), runtime, api.TrialContext{}); err != nil {
		t.Fatal(err)
	}
	for id := range application.writes.profiles {
		hotel, _ := application.reference.Hotel(id)
		if profile, _ := fake.state.Profile(id); profile.Name != hotel.Name || profile.Phone != hotel.Phone {
			t.Fatalf("profile %s not restored: %+v", id, profile)
		}
	}
	for id := range application.writes.capacity {
		hotel, _ := application.reference.Hotel(id)
		if rooms, _ := fake.state.Capacity(id); rooms != hotel.Capacity {
			t.Fatalf("capacity %s not restored: %d", id, rooms)
		}
	}
	for key := range application.writes.prices {
		hotel, _ := application.reference.Hotel(key.HotelID)
		for index, plan := range fake.state.Plans(key.HotelID) {
			if plan != hotel.RatePlans[index] {
				t.Fatalf("plan %+v not restored", key)
			}
		}
	}
	restores := len(fake.writes)
	if err := application.Reset(context.Background(), runtime, api.TrialContext{}); err != nil || len(fake.writes) != restores {
		t.Fatalf("a second Reset wrote again (%v)", err)
	}
}

// A candidate whose profile cache never invalidates passes reads within the
// bound and fails them once the bound has passed.
func TestStaleProfilesFailAfterTheBound(t *testing.T) {
	application := newApplication(t, operationUpdateProfile, operationSearch)
	prepared, _ := application.Prepare(context.Background(), nil, api.TrialContext{FixtureSeed: 3})
	handler, fake := newWritableFake(t)
	fake.staleProfiles = true
	runtime := &handlerRuntime{handler: handler}
	// Update the profiles of the hotels a few searches return.
	var searches []uint64
	for random := uint64(1); len(searches) < 20; random++ {
		plan := build(t, application, operationSearch, api.Sample{Random: random}, prepared)
		application.FinishOperation(plan)
		if plan.State.(*operationState).sandbox == nil && len(plan.State.(*operationState).expectedIDs) > 0 {
			searches = append(searches, random)
		}
	}
	for _, random := range searches {
		plan := build(t, application, operationSearch, api.Sample{Random: random}, prepared)
		application.FinishOperation(plan)
		for _, id := range plan.State.(*operationState).expectedIDs {
			application.writes.mu.Lock()
			hotel, _ := application.reference.Hotel(id)
			h := application.writes.profile(hotel)
			busy := h.busy
			application.writes.mu.Unlock()
			if busy {
				continue
			}
			result := runtime.Invoke(context.Background(), api.Invocation{Target: hotelsupport.GatewayTarget, Payload: api.HTTPRequestSpec{
				Method: "POST", Path: "/admin/profile", Query: map[string]string{"hotelId": id, "phone": "+0 " + id},
			}})
			application.writes.mu.Lock()
			h.begin(hotelsupport.Profile{Name: hotel.Name, Phone: "+0 " + id}, time.Now())
			h.finish(time.Now(), validateMessage(result, profileUpdated).Success)
			application.writes.mu.Unlock()
		}
	}
	searchAll := func() (failures int) {
		for _, random := range searches {
			if validation := run(t, application, runtime, prepared, operationSearch, random); !validation.Success {
				failures++
			}
		}
		return failures
	}
	if failures := searchAll(); failures != 0 {
		t.Fatalf("%d reads failed within the bound", failures)
	}
	time.Sleep(StalenessBound + 50*time.Millisecond)
	if failures := searchAll(); failures != len(searches) {
		t.Fatalf("only %d of %d stale reads failed after the bound", failures, len(searches))
	}
}

func TestWritePlans(t *testing.T) {
	application := newApplication(t, operationUpdateRate, operationSetCapacity, operationAddHotel, operationUpdateProfile)
	prepared, _ := application.Prepare(context.Background(), nil, api.TrialContext{FixtureSeed: 5})
	reference := hotelsupport.DefaultReference()
	seen := map[string]bool{}
	for random := uint64(1); random < 300; random++ {
		plan := build(t, application, operationSetCapacity, api.Sample{Random: random * 7919}, prepared)
		first := plan.Invocations[0].Payload.(api.HTTPRequestSpec)
		probe := plan.Invocations[1].Payload.(api.HTTPRequestSpec)
		hotel, _ := reference.Hotel(first.Query["hotelId"])
		rooms, _ := strconv.Atoi(first.Query["rooms"])
		if rooms < max(1, hotel.Capacity/2) || rooms > 2*hotel.Capacity || probe.Query["number"] != strconv.Itoa(rooms+1) {
			t.Fatalf("capacity plan %v then %v for capacity %d", first.Query, probe.Query, hotel.Capacity)
		}
		// Writes to one item are serialized: the hotel is not drawn again
		// while this plan is in flight.
		if seen[hotel.ID] {
			t.Fatalf("capacity of hotel %s written by two plans in flight", hotel.ID)
		}
		seen[hotel.ID] = true
	}
	for random := uint64(1); random < 100; random++ {
		plan := build(t, application, operationUpdateRate, api.Sample{Random: random * 31}, prepared)
		query := plan.Invocations[0].Payload.(api.HTTPRequestSpec).Query
		hotel, _ := reference.Hotel(query["hotelId"])
		total, _ := strconv.ParseFloat(query["totalRate"], 64)
		for _, seeded := range hotel.RatePlans {
			if seeded.Code == query["code"] && (total < 0.79*seeded.TotalRate || total > 1.26*seeded.TotalRate) {
				t.Fatalf("rate %v for seeded %v", query, seeded)
			}
		}
		application.FinishOperation(plan)
	}
	plan := build(t, application, operationAddHotel, api.Sample{Random: 77}, prepared)
	if body := plan.Invocations[0].Payload.(api.HTTPRequestSpec).Body; !strings.Contains(body, `"hotelId":"a`) {
		t.Fatalf("addition body %s", body)
	}
	added := prepared.(*fixture).added[0].hotel
	if added.Rating >= reference.MaxRating() || added.Price <= reference.MinPrice() {
		t.Fatalf("addition rating %v price %v would change the rate or price recommendations", added.Rating, added.Price)
	}
	centreLat, centreLon, _ := dataset.Sandbox(prepared.(*fixture).sandbox)
	if diff := added.Lat - centreLat; diff > 0.02 || diff < -0.02 || added.Lon-centreLon > 0.04 || added.Lon-centreLon < -0.04 {
		t.Fatalf("addition at %v,%v, sandbox centre %v,%v", added.Lat, added.Lon, centreLat, centreLon)
	}
	application.FinishOperation(plan)
	if item := prepared.(*fixture).added[0]; !item.done || item.ok {
		t.Fatalf("an unvalidated addition was recorded as %+v", item)
	}
}

func TestReservationsAvoidCapacityManagedHotels(t *testing.T) {
	application := newApplication(t, operationReserveCapacity)
	prepared, _ := application.Prepare(context.Background(), nil, api.TrialContext{FixtureSeed: 9})
	managed := 0
	for index := range application.reference.Data.Hotels {
		if capacityManaged(index) {
			managed++
		}
	}
	if managed < 5800 || managed > 6700 {
		t.Fatalf("%d capacity-managed hotels", managed)
	}
	for random := uint64(0); random < 3000; random++ {
		plan := build(t, application, operationReserveCapacity, api.Sample{Random: random * 0x9e3779b97f4a7c15}, prepared)
		id, _ := strconv.Atoi(plan.Invocations[0].Payload.(api.HTTPRequestSpec).Query["hotelId"])
		application.FinishOperation(plan)
		if capacityManaged(id - 1) {
			t.Fatalf("reservation on capacity-managed hotel %d", id)
		}
	}
}

// Concurrent workers, as the closed loop runs them, with writes among the
// reads: every answer validates (run with -race).
func TestConcurrentMixAgainstAFake(t *testing.T) {
	application := newApplication(t, allOperations...)
	prepared, _ := application.Prepare(context.Background(), nil, api.TrialContext{Index: 2, FixtureSeed: 12})
	handler, _ := newWritableFake(t)
	runtime := &handlerRuntime{handler: handler}
	deadline := time.Now().Add(1500 * time.Millisecond)
	errs := make(chan string, 16)
	for worker := 0; worker < 16; worker++ {
		go func(worker int) {
			random := uint64(worker+1) * 0x9e3779b97f4a7c15
			for time.Now().Before(deadline) {
				random = dataset.Mix(random, 1)
				name := allOperations[random%uint64(len(allOperations))]
				operation := api.Operation{Name: name, Target: hotelsupport.GatewayTarget}
				plan, err := application.BuildOperation(operation, api.Sample{Counter: int64(random >> 2), Random: random}, prepared)
				if err != nil {
					errs <- err.Error()
					return
				}
				results := make([]api.ProtocolResult, len(plan.Invocations))
				for index, invocation := range plan.Invocations {
					results[index] = runtime.Invoke(context.Background(), invocation)
				}
				validation := application.ValidateOperation(operation, plan, results)
				application.FinishOperation(plan)
				if !validation.Success {
					errs <- name + ": " + validation.ErrorMessage
					return
				}
			}
			errs <- ""
		}(worker)
	}
	for worker := 0; worker < 16; worker++ {
		if message := <-errs; message != "" {
			t.Fatal(message)
		}
	}
}
