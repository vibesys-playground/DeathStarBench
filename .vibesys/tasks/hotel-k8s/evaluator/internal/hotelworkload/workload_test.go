package hotelworkload

import (
	"os"
	"testing"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/workload"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// TestMain generates the tests' dataset: dataset.TestSpec (the file $HOTEL_DATASET names, else
// a small dataset of the production shape).
func TestMain(m *testing.M) {
	dataset.UseTestDataset()
	os.Exit(m.Run())
}

func newModel(t *testing.T, phase int) *Model {
	t.Helper()
	m, err := NewModel(dataset.Default(), workload.NewRand(7, "model"))
	if err != nil {
		t.Fatal(err)
	}
	if err := BeginPhase(m, phase); err != nil {
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

func build(m *Model, name string, n int) workload.Request {
	return operations()[name].Build(m, workload.NewRand(7, "phase", 0).Child(n))
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
	}
}

func TestEveryOperationBuilds(t *testing.T) {
	m := newModel(t, 0)
	for name := range operations() {
		for n := range 30 {
			if request := build(m, name, n); len(request.Steps) == 0 {
				t.Fatalf("%s: no steps", name)
			}
		}
	}
}

// Reservations take distinct (night, hotel) slots, never a capacity-managed hotel, and each phase
// books nights of its own block.
func TestReservationsFillDistinctHotelsPerNightAndPhase(t *testing.T) {
	seen := map[[2]int]bool{}
	for phase := range 3 {
		m := newModel(t, phase)
		for n := range 3 * reservationsPerNight {
			night, index := m.reservationSlot(workload.NewRand(7, "phase", 0).Child(n))
			if CapacityManaged(index) {
				t.Fatalf("reservation %d of capacity-managed hotel %d", n, index)
			}
			if night < phase*phaseNights || night >= (phase+1)*phaseNights {
				t.Fatalf("phase %d reserves night %d, outside its block", phase, night)
			}
			key := [2]int{night, index}
			if seen[key] {
				t.Fatalf("hotel %d twice on night %d", index, night)
			}
			seen[key] = true
		}
	}
}

// Capacity writes go to managed hotels, at most one per hotel and phase, so none overlaps
// another of its hotel (nothing serializes them).
func TestCapacityWritesGoToManagedHotels(t *testing.T) {
	m := newModel(t, 0)
	written := map[int]bool{}
	for n := range 500 {
		index, rooms := m.capacityWrite(workload.NewRand(7, "phase", 0).Child(n))
		capacity := m.data.Capacity(index)
		if !CapacityManaged(index) || rooms < max(1, capacity/2) || rooms > 2*capacity {
			t.Fatalf("capacity write of %d rooms to hotel %d with %d rooms", rooms, index, capacity)
		}
		if written[index] {
			t.Fatalf("write %d: hotel %d already written this phase", n, index)
		}
		written[index] = true
	}
	if err := BeginPhase(m, 1); err != nil {
		t.Fatal(err)
	}
	if len(m.capacitySet) != 0 {
		t.Fatal("a phase starts with no capacity written")
	}
}

// Additions have numbers of their phase's block, and later searches of the phase go near them.
func TestAdditionsArePerPhaseAndSearchedNear(t *testing.T) {
	for phase := range 2 {
		m := newModel(t, phase)
		build(m, "add_hotel", 0)
		if len(m.added) != 1 || m.nextAddition != 2+phase*additionsPerPhase {
			t.Fatalf("phase %d: %d additions, next number %d", phase, len(m.added), m.nextAddition)
		}
		near := 0
		for n := range 2000 {
			if _, _, ok := m.sandboxLocation(workload.NewRand(9, "near").Child(n)); ok {
				near++
			}
		}
		if near < 20 || near > 120 {
			t.Fatalf("phase %d: %d of 2000 searches near an addition, want about 3%%", phase, near)
		}
	}
}
