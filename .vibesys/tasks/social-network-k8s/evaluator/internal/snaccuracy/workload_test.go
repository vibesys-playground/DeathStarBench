package snaccuracy

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"
	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/workload"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/fakeapp"
	"vibesys/social-network-k8s-evaluator/internal/snapi"
	"vibesys/social-network-k8s-evaluator/internal/snspec"
)

var data = sync.OnceValue(dataset.Default)

// drawn is one operation as the generator draws it: its name, request and action.
type drawn struct {
	op      string
	request workload.Request
	action  snapi.Action
}

// draw builds n operations of phase as the SDK's generator does: the phase stream picks each by
// weight and builds it from its own child stream, on a model built afresh for the phase.
func draw(t *testing.T, seed uint64, phase, n int) []drawn {
	t.Helper()
	w := Workload()
	m, err := w.Model(data(), workload.NewRand(seed, "model"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.BeginPhase(m, phase); err != nil {
		t.Fatal(err)
	}
	weights := make([]float64, len(w.Operations))
	for i, op := range w.Operations {
		weights[i] = op.Weight
	}
	stream := workload.NewRand(seed, "phase", uint32(phase))
	out := make([]drawn, n)
	for i := range out {
		op := w.Operations[stream.Weighted(weights)]
		request := op.Build(m, stream.Child(uint64(i)))
		info, err := json.Marshal(request.Info)
		if err != nil {
			t.Fatal(err)
		}
		// The oracle reads Info strictly (snspec's decode).
		dec := json.NewDecoder(bytes.NewReader(info))
		dec.DisallowUnknownFields()
		var a snapi.Action
		if err := dec.Decode(&a); err != nil {
			t.Fatalf("%s: info %s: %v", op.Name, info, err)
		}
		out[i] = drawn{op.Name, request, a}
	}
	return out
}

func TestEveryOperationIsOneActionAndItsRequest(t *testing.T) {
	seen := map[string]int{}
	for _, d := range draw(t, 7, 0, 20000) {
		seen[d.op]++
		if len(d.request.Steps) != 1 {
			t.Fatalf("%s: %d steps, want 1", d.op, len(d.request.Steps))
		}
		if !reflect.DeepEqual(d.action, d.request.Info) {
			t.Fatalf("%s: the info %+v does not decode to itself (%+v)", d.op, d.request.Info, d.action)
		}
		method, path, _ := snapi.Request(d.action)
		if got, want := d.request.Steps[0].String(), "("+method+" "+path+")"; !strings.HasSuffix(got, want) {
			t.Fatalf("%s: step %s, want %s", d.op, got, want)
		}
	}
	for _, op := range Workload().Operations {
		if seen[op.Name] == 0 {
			t.Errorf("%s never drawn", op.Name)
		}
	}
}

func TestTheTrafficIsOnTheHotUsers(t *testing.T) {
	d := data()
	hot := Hot(d)
	if len(hot) != ActiveHot+PopularHot {
		t.Fatalf("%d hot users", len(hot))
	}
	ids := map[int64]bool{}
	for _, u := range hot {
		ids[d.Users[u].ID] = true
	}
	if !slices.Contains(hot, d.PopularityRank[0]) || !slices.Contains(hot, d.ActivityRank[0]) {
		t.Fatal("the most followed and the most active users are hot")
	}
	markers := map[string]bool{}
	for phase := range 3 {
		for _, x := range draw(t, 7, phase, 5000) {
			a := x.action
			switch x.op {
			case "compose":
				if !ids[a.User] || markers[a.Marker] || snapi.MarkerOf(a.Text) != a.Marker {
					t.Fatalf("compose %+v: a hot author and a fresh marker in the text", a)
				}
				markers[a.Marker] = true
				for _, v := range a.Mentions {
					if !ids[v] || v == a.User || !strings.Contains(a.Text, "@user") {
						t.Fatalf("compose %+v mentions %d", a, v)
					}
				}
			case "follow", "unfollow":
				if !ids[a.User] || !ids[a.Followee] || a.User == a.Followee || a.On != (x.op == "follow") {
					t.Fatalf("%s %+v", x.op, a)
				}
			case "home_read", "user_read":
				if !ids[a.User] || a.Home != (x.op == "home_read") {
					t.Fatalf("%s %+v", x.op, a)
				}
			case "register":
				if lo := FirstRegistered + int64(phase)<<20; a.User < lo || a.User >= lo+1<<20 {
					t.Fatalf("phase %d registers %d", phase, a.User)
				}
			}
		}
	}
}

// run sends phases of the workload to the fake application from clients concurrent clients, in
// stream order, restarting a service after phase 0 and flushing a cache after phase 1, and returns
// the history.
func run(t *testing.T, options fakeapp.Options, phases, perPhase int) *check.History {
	t.Helper()
	app := fakeapp.New(data(), options)
	server := httptest.NewServer(app)
	defer server.Close()
	system := snapi.NewHTTPSystem(server.URL, 10*time.Second, &http.Transport{MaxIdleConnsPerHost: 64})
	const clients = 8
	var history [][]check.Call
	var faults []check.Fault
	for phase := range phases {
		ops := draw(t, 11, phase, perPhase)
		calls := make([]check.Call, len(ops))
		next := make(chan int)
		var wg sync.WaitGroup
		for range clients {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range next {
					calls[i] = snspec.FromOracleCall(system.Invoke(context.Background(), ops[i].action), phase, uint64(i))
					calls[i].Op = ops[i].op
				}
			}()
		}
		for i := range ops {
			next <- i
			time.Sleep(200 * time.Microsecond)
		}
		close(next)
		wg.Wait()
		slices.SortStableFunc(calls, func(a, b check.Call) int { return int(a.Start - b.Start) })
		history = append(history, calls)
		if phase < phases-1 {
			f := check.Fault{Kind: "restart", Target: "home-timeline-service", Start: time.Now().UnixNano(), AfterPhase: phase}
			if phase%2 == 1 {
				f.Kind, f.Target = "exec", "social-graph-redis"
				app.Flush(f.Target)
			} else {
				app.Restart(f.Target)
			}
			f.End = time.Now().UnixNano()
			faults = append(faults, f)
		}
	}
	return check.NewHistory(history, faults)
}

// TestTheOracleJudgesTheWorkload runs the workload against the in-process fake application and
// checks the history with the oracle's two specs: the correct fake passes, faulty ones fail.
func TestTheOracleJudgesTheWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	for _, c := range []struct {
		name    string
		options fakeapp.Options
		want    check.Verdict
	}{
		{"correct", fakeapp.Options{}, check.Pass},
		{"stale cache", fakeapp.Options{StaleCache: true}, check.Fail},
		{"unfollow ignored", fakeapp.Options{IgnoreUnfollow: true}, check.Fail},
		{"posts lost on restart", fakeapp.Options{LosePostsOnRestart: true}, check.Fail},
		{"partial follower cache after a flush", fakeapp.Options{FollowerCache: true}, check.Fail},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := run(t, c.options, 3, 3000)
			r := check.Check(h, []check.AnySpec{snspec.DefaultAuthorSpec(data()), snspec.DefaultReaderSpec(data())},
				check.Options{PartitionTimeout: 20 * time.Second, Workers: 4, MaxLostShare: 0.01})
			var parts []string
			for _, s := range r.Specs {
				parts = append(parts, s.Name+": "+string(s.Verdict))
			}
			t.Logf("%s: %s (%s) in %v", c.name, r.Verdict, strings.Join(parts, ", "), r.Elapsed.Round(time.Millisecond))
			if r.Verdict != c.want {
				t.Fatalf("%s, want %s\n%s", r.Verdict, c.want, r.Text(3))
			}
		})
	}
}
