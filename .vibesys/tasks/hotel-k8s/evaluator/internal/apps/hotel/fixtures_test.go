package hotel

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/api"
)

// phaseFixtures prepares every trial a run may have, as the engine passes
// them (trial fixture seed = run fixture seed + index).
func phaseFixtures(t *testing.T, run int64) []*fixture {
	t.Helper()
	application := &Application{}
	fixtures := make([]*fixture, 0, TrialsPerRun)
	for index := 0; index < TrialsPerRun; index++ {
		value, err := application.Prepare(context.Background(), nil, api.TrialContext{
			Index: index, Seed: 1, FixtureSeed: run + int64(index),
		})
		if err != nil {
			t.Fatal(err)
		}
		fixtures = append(fixtures, value.(*fixture))
	}
	return fixtures
}

// Property: the closed phase (fixture seed F) and the open phase (F + 1, as
// run.py derives it), and generally any two runs whose seeds differ by 1 to
// RunSlots-1, own disjoint nights, sandbox cells and added-hotel IDs, for
// every trial either may run, whatever F is.
func TestPhaseFixturesAreDisjoint(t *testing.T) {
	if RunSlots*TrialsPerRun > dataset.SandboxCount() {
		t.Fatalf("%d trial slots, only %d sandbox cells", RunSlots*TrialsPerRun, dataset.SandboxCount())
	}
	random := rand.New(rand.NewSource(1))
	seeds := []int64{0, 47, 48, RunSlots - 1, RunSlots, 1<<62 - 2}
	for len(seeds) < 300 {
		seeds = append(seeds, random.Int63n(1<<62))
	}
	end := time.Date(9800, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, closed := range seeds {
		offsets := []int64{1, 1 + random.Int63n(RunSlots-2)}
		for _, offset := range offsets {
			nights := map[time.Time]int64{}
			cells := map[uint64]int64{}
			tags := map[uint64]int64{}
			for _, run := range []int64{closed, closed + offset} {
				for index, data := range phaseFixtures(t, run) {
					if data.dateBase.Before(time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)) ||
						!data.dateBase.AddDate(0, 0, reservationBlockDays).Before(end) {
						t.Fatalf("run %d trial %d: block %s outside [3000, 9800)", run, index, data.dateBase)
					}
					if data.sandbox >= dataset.SandboxCount() {
						t.Fatalf("run %d trial %d: sandbox cell %d", run, index, data.sandbox)
					}
					// Blocks are aligned, so distinct starts mean disjoint nights.
					if other, taken := nights[data.dateBase]; taken {
						t.Fatalf("runs %d and %d share the block at %s", other, run, data.dateBase.Format(time.DateOnly))
					}
					nights[data.dateBase] = run
					if other, taken := cells[data.sandbox]; taken {
						t.Fatalf("runs %d and %d share sandbox cell %d", other, run, data.sandbox)
					}
					cells[data.sandbox] = run
					if other, taken := tags[data.tag]; taken {
						t.Fatalf("runs %d and %d share hotel ID tag %x", other, run, data.tag)
					}
					tags[data.tag] = run
				}
			}
		}
	}
}

func TestTrialSlotBounds(t *testing.T) {
	if _, err := TrialSlot(api.TrialContext{Index: TrialsPerRun, FixtureSeed: 5}); err == nil {
		t.Fatal("accepted a trial beyond TrialsPerRun")
	}
	// A negative fixture seed still lands in range.
	slot, err := TrialSlot(api.TrialContext{Index: 3, FixtureSeed: -7})
	if err != nil || slot >= RunSlots*TrialsPerRun || slot%TrialsPerRun != 3 {
		t.Fatalf("slot %d, %v", slot, err)
	}
	// The development workloads' seeds (48 and 49) are disjoint too.
	closed, _ := TrialSlot(api.TrialContext{Index: 0, FixtureSeed: 48})
	open, _ := TrialSlot(api.TrialContext{Index: 0, FixtureSeed: 49})
	if closed/TrialsPerRun == open/TrialsPerRun {
		t.Fatalf("seeds 48 and 49 share run slot %d", closed/TrialsPerRun)
	}
}
