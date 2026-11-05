package snworkload

import (
	"math/rand"
	"net/url"
	"regexp"
	"testing"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/workload"

	"vibesys/microservice-evaluator/api"
	"vibesys/social-network-k8s-evaluator/internal/apps/socialnetwork"
	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/oracle"
)

var markerToken = regexp.MustCompile(`#[0-9a-z]+`) // the one "#" token of a compose: its marker

// earlierWorkload is the servicebench workload of the earlier adapter (workload/*.toml).
func earlierWorkload() api.Workload {
	return api.Workload{
		Version: 1, Name: "social-network-k8s-closed-loop", Application: "social-network",
		Targets: []api.Target{{Name: "gateway", Protocol: "http", Address: "http://unused", SessionPolicy: "reuse"}},
		Operations: []api.Operation{
			{Name: opUserTimeline, Target: "gateway", Weight: 500}, {Name: opHomeTimeline, Target: "gateway", Weight: 400},
			{Name: opCompose, Target: "gateway", Weight: 100}, {Name: opFollow, Target: "gateway", Weight: 5},
			{Name: opUnfollow, Target: "gateway", Weight: 5},
		},
	}
}

// TestRequestsAreTheEarlierAdapters: from the same 64-bit word, every operation sends the
// request the servicebench adapter (internal/apps/socialnetwork) sent, the compose's marker
// aside (its namespace is the sample's now, the trial's then).
func TestRequestsAreTheEarlierAdapters(t *testing.T) {
	earlier, err := socialnetwork.NewWithJournal(earlierWorkload(), nil)
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewModel(dataset.Default(), workload.NewRand(1, "model"))
	if err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewSource(3))
	for i := 0; i < 20000; i++ {
		word := random.Uint64()
		name := []string{opUserTimeline, opHomeTimeline, opCompose, opFollow, opUnfollow}[i%5]
		plan, err := earlier.BuildOperation(api.Operation{Name: name, Target: "gateway"}, api.Sample{Counter: int64(i), Random: word}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var actions []oracle.Action
		switch name {
		case opUserTimeline:
			actions = []oracle.Action{oracle.Read(false, m.id(m.popularUser(word, streamUser)), Page(word), Page(word)+pageSize)}
		case opHomeTimeline:
			actions = []oracle.Action{oracle.Read(true, m.id(m.activeUser(word, streamUser)), Page(word), Page(word)+pageSize)}
		case opCompose:
			a := m.compose(m.activeUser(word, streamUser), word, int64(i))
			actions = []oracle.Action{a, oracle.Read(false, a.User, 0, pageSize)}
		default:
			u, v := m.pickEdge(name == opFollow, word)
			actions = []oracle.Action{oracle.Follow(m.id(u), m.id(v), name == opFollow)}
			m.oracle.Record(m.state, oracle.Call{Action: actions[0], Pending: true}) // as generation applies it
		}
		if len(actions) != len(plan.Invocations) {
			t.Fatalf("%s %d: %d steps, the adapter sent %d", name, i, len(actions), len(plan.Invocations))
		}
		for j, a := range actions {
			spec := plan.Invocations[j].Payload.(api.HTTPRequestSpec)
			method, path, values := oracle.Request(a)
			want := url.Values{}
			for key, value := range spec.Query {
				want.Set(key, value)
			}
			for key, value := range spec.Form {
				want.Set(key, value)
			}
			if name == opCompose && j == 0 {
				for _, v := range []url.Values{values, want} {
					v.Set("text", markerToken.ReplaceAllString(v.Get("text"), "#M"))
				}
			}
			got := values.Encode()
			if method != spec.Method || path != spec.Path || got != want.Encode() {
				t.Fatalf("%s %d step %d: %s %s %s, the adapter sent %s %s %s", name, i, j, method, path, got, spec.Method, spec.Path, want.Encode())
			}
		}
		earlier.FinishOperation(plan) // the adapter's edges are no longer in flight
	}
}
