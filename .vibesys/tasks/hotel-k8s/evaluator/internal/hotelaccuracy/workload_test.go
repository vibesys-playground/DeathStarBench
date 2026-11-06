package hotelaccuracy

import (
	"os"
	"slices"
	"testing"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/workload"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/hotel-k8s-evaluator/internal/hotelspec"
)

func TestMain(m *testing.M) {
	dataset.UseTestDataset()
	os.Exit(m.Run())
}

// built is one phase's operations as the generator builds them: name and info, in stream order.
type built struct {
	name string
	info any
}

func phase(t *testing.T, p, n int) (*Model, []built) {
	t.Helper()
	w := Workload()
	m, err := w.Model(dataset.Default(), workload.NewRand(11, "model"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.BeginPhase(m, p); err != nil {
		t.Fatal(err)
	}
	weights := make([]float64, len(w.Operations))
	for i, op := range w.Operations {
		weights[i] = op.Weight
	}
	stream := workload.NewRand(11, "phase", uint64(p))
	var out []built
	for i := range n {
		op := w.Operations[stream.Weighted(weights)]
		request := op.Build(m, stream.Child(uint64(i)))
		if len(request.Steps) != 1 {
			t.Fatalf("%s: %d steps, the oracle reads one", op.Name, len(request.Steps))
		}
		out = append(out, built{op.Name, request.Info})
	}
	return m, out
}

func hotelOf(b built) string {
	switch in := b.info.(type) {
	case hotelspec.ReserveInfo:
		return in.Hotel
	case hotelspec.CapacityInfo:
		return in.Hotel
	}
	return ""
}

// The contract with the oracle: one-night reservations, and no hotel takes both capacity writes
// and reservations in one phase (so the two never overlap); each capacity hotel takes both, in
// alternate phases.
func TestCapacityWritesAndReservationsNeverShareAPhase(t *testing.T) {
	roles := map[string]map[string]bool{} // hotel -> roles over the phases
	for p := range 4 {
		_, ops := phase(t, p, 6000)
		reserved, written := map[string]bool{}, map[string]bool{}
		for _, b := range ops {
			switch b.name {
			case "reserve", "reserve_capacity_hotel":
				in := b.info.(hotelspec.ReserveInfo)
				if len(in.Nights) != 1 {
					t.Fatalf("phase %d: a reservation of %d nights", p, len(in.Nights))
				}
				reserved[in.Hotel] = true
			case "set_capacity":
				written[hotelOf(b)] = true
			}
		}
		for h := range written {
			if reserved[h] {
				t.Fatalf("phase %d: hotel %s takes capacity writes and reservations", p, h)
			}
			if roles[h] == nil {
				roles[h] = map[string]bool{}
			}
			roles[h]["write"] = true
		}
		for h := range reserved {
			if roles[h] == nil {
				roles[h] = map[string]bool{}
			}
			roles[h]["reserve"] = true
		}
		if len(written) != CapacityHotels/2 {
			t.Fatalf("phase %d writes the capacity of %d hotels, want %d", p, len(written), CapacityHotels/2)
		}
	}
	both := 0
	for _, r := range roles {
		if r["write"] && r["reserve"] {
			both++
		}
	}
	if both != CapacityHotels {
		t.Fatalf("%d hotels both written and reserved over the phases, want %d", both, CapacityHotels)
	}
}

// Reservations of a window go to two nights of the phase's block; from phase 1 some go to an
// earlier phase's nights.
func TestReservationNights(t *testing.T) {
	for p := range 3 {
		m, ops := phase(t, p, 3*windowOps)
		own, earlier := 0, 0
		for _, b := range ops {
			if b.name != "reserve" {
				continue
			}
			night := b.info.(hotelspec.ReserveInfo).Nights[0]
			if slices.Contains([]string{day(m.night(p, 0)), day(m.night(p, 1)), day(m.night(p, 2)), day(m.night(p, 3))}, night) {
				own++
			} else {
				earlier++
			}
		}
		if p == 0 && earlier != 0 || p > 0 && (earlier == 0 || earlier > own) {
			t.Fatalf("phase %d: %d reservations of its own nights, %d of earlier phases'", p, own, earlier)
		}
	}
}
