package snworkload

import (
	"strconv"
	"testing"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/workload"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
)

func newModel(t *testing.T) *Model {
	t.Helper()
	m, err := NewModel(dataset.Default(), workload.NewRand(7, "model"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func operation(name string) workload.Operation[*Model] {
	for _, op := range Workload().Operations {
		if op.Name == name {
			return op
		}
	}
	panic(name)
}

// TestRequestDistribution measures, over 100k operations of the mix, how often a timeline
// request repeats one already made (the hit rate of an unbounded cache keyed by the exact
// request); the earlier adapter's was 66.7% (user timelines 77%, home timelines 57%).
func TestRequestDistribution(t *testing.T) {
	m := newModel(t)
	ops := Workload().Operations
	weights := make([]float64, len(ops))
	for i, op := range ops {
		weights[i] = op.Weight
	}
	stream := workload.NewRand(1, "phase", 0)
	seen := map[string]bool{}
	hits, reads := 0, 0
	byKind := map[string][2]int{}
	pages := map[int]int{}
	for i := 0; i < 100000; i++ {
		op := ops[stream.Weighted(weights)]
		// The draws of the operation's Build: its word decides the user and the page.
		word := stream.Child(i).Uint64()
		op.Build(m, stream.Child(i))
		var key string
		switch op.Name {
		case opUserTimeline, opHomeTimeline:
			user := m.popularUser(word, streamUser)
			if op.Name == opHomeTimeline {
				user = m.activeUser(word, streamUser)
			}
			key = op.Name + strconv.Itoa(int(user)) + "/" + strconv.Itoa(Page(word))
			pages[Page(word)/pageSize]++
		case opCompose:
			key = opUserTimeline + strconv.Itoa(int(m.activeUser(word, streamUser))) + "/0"
		}
		if key == "" {
			continue
		}
		reads++
		counts := byKind[op.Name]
		counts[0]++
		if seen[key] {
			hits++
			counts[1]++
		}
		byKind[op.Name] = counts
		seen[key] = true
	}
	rate := float64(hits) / float64(reads)
	t.Logf("exact-request cache hit rate: %.3f of %d timeline requests; pages %v", rate, reads, pages)
	for name, counts := range byKind {
		t.Logf("  %s: %d requests, hit rate %.3f", name, counts[0], float64(counts[1])/float64(counts[0]))
	}
	if rate < 0.6 || rate > 0.75 {
		t.Fatalf("hit rate %.3f, the earlier adapter's was 0.667", rate)
	}
}

// A phase's markers are its own: no two phases of a sample post the same one.
func TestMarkersArePerPhase(t *testing.T) {
	seen := map[string]bool{}
	for phase := range 3 {
		m := newModel(t)
		if err := BeginPhase(m, phase); err != nil {
			t.Fatal(err)
		}
		for i := range 100 {
			seq := m.nextSeq
			operation(opCompose).Build(m, workload.NewRand(3, "phase", uint64(phase)).Child(i))
			marker := m.compose(0, 1, seq).Marker
			if seen[marker] || m.nextSeq != seq+1 {
				t.Fatalf("phase %d: marker %s again (next %d)", phase, marker, m.nextSeq)
			}
			seen[marker] = true
		}
	}
}

// Follow changes build on the seeded graph and this phase's earlier changes: a follow is of an
// edge that is off, an unfollow of one that is on.
func TestEdgesBuildOnThePhasesChanges(t *testing.T) {
	m := newModel(t)
	follow, unfollow := operation(opFollow), operation(opUnfollow)
	stream := workload.NewRand(5, "phase", 0)
	for i := 0; i < 2000; i++ {
		op, on := follow, true
		if i%2 == 1 {
			op, on = unfollow, false
		}
		before := map[[2]int32]bool{}
		for e, v := range m.changed {
			before[e] = v
		}
		op.Build(m, stream.Child(i))
		var edge [2]int32
		for e, v := range m.changed {
			if was, ok := before[e]; !ok || was != v {
				edge = e
			}
		}
		if !m.follows(edge[0], edge[1]) == on || m.changed[edge] != on {
			t.Fatalf("operation %d: edge %v is %v after a change to %v", i, edge, m.follows(edge[0], edge[1]), on)
		}
		if was, ok := before[edge]; ok && was == on || !ok && m.data.Follows(int(edge[0]), int(edge[1])) == on {
			t.Fatalf("operation %d changes edge %v to its current state %v", i, edge, on)
		}
	}
}
