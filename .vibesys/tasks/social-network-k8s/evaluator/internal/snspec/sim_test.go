// simulate drives the task's fakeapp (in-process Social Network fake with injectable
// faults) with concurrent clients over HTTP and records snapi.Calls plus fault events.
package snspec

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/fakeapp"
	"vibesys/social-network-k8s-evaluator/internal/snapi"
)

// Mangle post-processes read responses (injected bugs the fake has no option for).
type Mangle struct {
	Phantom     bool // one home read lists a post that was never composed
	SwapOrder   bool // one read lists its first two posts swapped
	DropFirst   bool // every 7th home read of a non-empty page loses its newest post
	ShiftPage   bool // home reads with start>0 are served from start+1
	SlowCompose bool // every 5th compose is applied, then answered after 60ms (client times out at 30ms: unknown outcome)
}

type Params struct {
	Seed          int64
	Hot           int // hot user set size (dataset users with live traffic)
	Clients, Ops  int // per client per phase
	Phases        int
	Think         time.Duration // mean think time
	Bound         time.Duration
	App           fakeapp.Options
	Mangle        Mangle
	InvalidEvery  int // every n-th op is an invalid request (0: none)
	PreFollows    int // follows per hot user created before phase 1
	ClientTimeout time.Duration
}

type simOut struct {
	Calls  [][]check.Call // by phase
	Faults []check.Fault
	Data   *dataset.Dataset
}

func runSim(p Params) simOut {
	data := dataset.Default()
	app := fakeapp.New(data, p.App)
	var handler http.Handler = app
	if p.Mangle != (Mangle{}) {
		handler = mangler(app, p.Mangle)
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()
	ct := p.ClientTimeout
	if ct == 0 {
		ct = 10 * time.Second
	}
	sys := snapi.NewHTTPSystem(srv.URL, ct, &http.Transport{MaxIdleConnsPerHost: 64})
	rng := rand.New(rand.NewSource(p.Seed))
	// hot users: dataset users that have posts
	var hot []int64
	for _, u := range data.ActivityRank {
		if len(hot) >= p.Hot {
			break
		}
		hot = append(hot, data.Users[u].ID)
	}
	out := simOut{Data: data}
	var mu sync.Mutex
	nextIndex := make([]uint64, p.Phases)
	record := func(ph int, c snapi.Call) {
		mu.Lock()
		out.Calls[ph] = append(out.Calls[ph], FromOracleCall(c, ph, nextIndex[ph]))
		nextIndex[ph]++
		mu.Unlock()
	}
	out.Calls = make([][]check.Call, p.Phases)
	ctx := context.Background()
	var markerSeq atomic.Int64
	// initial follows among the hot set (sequential, one client)
	for _, u := range hot {
		for i := 0; i < p.PreFollows; i++ {
			v := hot[rng.Intn(len(hot))]
			if v == u {
				continue
			}
			record(0, sys.Invoke(ctx, snapi.Follow(u, v, true)))
		}
	}
	prefix := fmt.Sprintf("m%08x", p.Seed&0xffffffff)
	for ph := 0; ph < p.Phases; ph++ {
		var wg sync.WaitGroup
		for c := 0; c < p.Clients; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				r := rand.New(rand.NewSource(p.Seed*1000 + int64(ph*100+c)))
				for i := 0; i < p.Ops; i++ {
					u := hot[r.Intn(len(hot))]
					page := []int{0, 0, 0, 10, 20}[r.Intn(5)]
					var a snapi.Action
					x := r.Intn(100)
					switch {
					case p.InvalidEvery > 0 && i%p.InvalidEvery == p.InvalidEvery-1:
						switch r.Intn(5) {
						case 0:
							a = snapi.RawRead(true, "abc", "0", "10")
						case 1:
							a = snapi.RawRead(false, strconv.FormatInt(u, 10), "5", "5")
						case 2:
							a = snapi.RawRead(true, strconv.FormatInt(u, 10), "-1", "10")
						case 3:
							a = snapi.RawRead(true, "999999999", "0", "10")
						default:
							a = snapi.Action{Kind: snapi.KindCompose, User: u, Name: "user" + strconv.FormatInt(u, 10), Text: ""}
						}
					case x < 22:
						mk := prefix + strconv.FormatInt(markerSeq.Add(1), 36)
						text := "w" + strconv.Itoa(r.Intn(100)) + " x" + strconv.Itoa(r.Intn(100))
						a = snapi.Action{Kind: snapi.KindCompose, User: u, Name: "user" + strconv.FormatInt(u, 10), Marker: mk}
						if r.Intn(5) == 0 {
							v := hot[r.Intn(len(hot))]
							if v != u {
								a.Mentions = []int64{v}
								text += " @user" + strconv.FormatInt(v, 10)
							}
						}
						text += " #" + mk
						if r.Intn(7) == 0 {
							a.URL = fmt.Sprintf("http://example%d.com", r.Intn(1000000))
							text += " " + a.URL
						}
						a.Text = text
					case x < 30:
						v := hot[r.Intn(len(hot))]
						if v == u {
							v = hot[(r.Intn(len(hot)-1)+1+indexOf(hot, u))%len(hot)]
						}
						a = snapi.Follow(u, v, r.Intn(3) != 0)
					case x < 60:
						a = snapi.Read(false, u, page, page+10)
					default:
						a = snapi.Read(true, u, page, page+10)
					}
					call := sys.Invoke(ctx, a)
					record(ph, call)
					if a.Kind == snapi.KindCompose && a.Text != "" && call.Status == 200 && p.Think > 0 {
						// read your write, as the benchmark does
						rd := snapi.Read(false, u, 0, 10)
						rd.Holds = []string{a.Marker}
						record(ph, sys.Invoke(ctx, rd))
					}
					if p.Think > 0 {
						time.Sleep(time.Duration(r.Int63n(int64(2 * p.Think))))
					}
				}
			}(c)
		}
		wg.Wait()
		if ph < p.Phases-1 {
			time.Sleep(2 * time.Millisecond)
			t := time.Now().UnixNano()
			f := check.Fault{Kind: "restart", Target: "home-timeline-service", Start: t, AfterPhase: ph}
			if ph%2 == 0 {
				app.Restart("home-timeline-service")
			} else {
				f = check.Fault{Kind: "flush", Target: "social-graph-redis", Start: t, AfterPhase: ph}
				app.Flush("social-graph-redis")
			}
			f.End = time.Now().UnixNano()
			out.Faults = append(out.Faults, f)
			time.Sleep(2 * time.Millisecond)
		}
	}
	for _, calls := range out.Calls {
		sort.SliceStable(calls, func(i, j int) bool { return calls[i].Start < calls[j].Start })
	}
	return out
}

func indexOf(l []int64, x int64) int {
	for i, v := range l {
		if v == x {
			return i
		}
	}
	return 0
}

type rec struct {
	http.ResponseWriter
	code int
	buf  []byte
}

func (r *rec) WriteHeader(c int)           { r.code = c }
func (r *rec) Write(b []byte) (int, error) { r.buf = append(r.buf, b...); return len(b), nil }

func mangler(next http.Handler, m Mangle) http.Handler {
	var n atomic.Int64
	var phantomDone, swapDone atomic.Bool
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		isRead := req.URL.Path == "/wrk2-api/home-timeline/read" || req.URL.Path == "/wrk2-api/user-timeline/read"
		if !isRead {
			next.ServeHTTP(w, req)
			if m.SlowCompose && req.URL.Path == "/wrk2-api/post/compose" && n.Add(1)%5 == 0 {
				time.Sleep(60 * time.Millisecond)
			}
			return
		}
		if m.ShiftPage && req.URL.Path == "/wrk2-api/home-timeline/read" {
			q := req.URL.Query()
			if st, _ := strconv.Atoi(q.Get("start")); st > 0 {
				sp, _ := strconv.Atoi(q.Get("stop"))
				q.Set("start", strconv.Itoa(st+1))
				q.Set("stop", strconv.Itoa(sp+1))
				req.URL.RawQuery = q.Encode()
			}
		}
		rw := &rec{ResponseWriter: w, code: 200}
		next.ServeHTTP(rw, req)
		var posts []map[string]any
		body := rw.buf
		if json.Unmarshal(body, &posts) == nil && len(posts) > 0 {
			home := req.URL.Path == "/wrk2-api/home-timeline/read"
			k := n.Add(1)
			switch {
			case m.Phantom && home && !phantomDone.Load() && k > 50:
				ghost := map[string]any{}
				for kk, v := range posts[0] {
					ghost[kk] = v
				}
				ghost["post_id"] = "999999999999"
				posts = append([]map[string]any{ghost}, posts...)
				phantomDone.Store(true)
			case m.SwapOrder && len(posts) >= 2 && !swapDone.Load() && k > 50 && posts[0]["timestamp"] != posts[1]["timestamp"]:
				posts[0], posts[1] = posts[1], posts[0]
				swapDone.Store(true)
			case m.DropFirst && home && k%7 == 0:
				posts = posts[1:]
			}
			body, _ = json.Marshal(posts)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rw.code)
		w.Write(body)
	})
}

type scen struct {
	name   string
	want   check.Verdict
	params func() Params
}

const simBound = 300 * time.Millisecond

func baseParams(seed int64) Params {
	return Params{Seed: seed, Hot: 25, Clients: 8, Ops: 150, Phases: 3, Think: 3 * time.Millisecond, Bound: simBound, InvalidEvery: 17, PreFollows: 4}
}

func scenarios() []scen {
	mod := func(f func(*Params)) func() Params {
		return func() Params { p := baseParams(7); f(&p); return p }
	}
	return []scen{
		{"correct (sync fan-out)", check.Pass, mod(func(p *Params) {})},
		{"correct (async fan-out 0.6B)", check.Pass, mod(func(p *Params) { p.App.FanoutDelay = simBound * 6 / 10 })},
		{"correct (async 0.6B, 16 clients)", check.Pass, mod(func(p *Params) { p.App.FanoutDelay = simBound * 6 / 10; p.Clients = 16; p.Seed = 11 })},
		{"correct (unknown outcomes: compose timeouts)", check.Pass, mod(func(p *Params) { p.Mangle.SlowCompose = true; p.ClientTimeout = 30 * time.Millisecond })},
		{"bug: stale beyond bound (fan-out 3B)", check.Fail, mod(func(p *Params) { p.App.FanoutDelay = 3 * simBound })},
		{"bug: stale cache never invalidated", check.Fail, mod(func(p *Params) { p.App.StaleCache = true })},
		{"bug: stale replica", check.Fail, mod(func(p *Params) { p.App.StaleReplica = true })},
		{"bug: missing must-post (drop newest of every 7th)", check.Fail, mod(func(p *Params) { p.Mangle.DropFirst = true })},
		{"bug: unfollow ignored", check.Fail, mod(func(p *Params) { p.App.IgnoreUnfollow = true })},
		{"bug: nonexistent post", check.Fail, mod(func(p *Params) { p.Mangle.Phantom = true })},
		{"bug: wrong order", check.Fail, mod(func(p *Params) { p.Mangle.SwapOrder = true })},
		{"bug: posts lost on restart", check.Fail, mod(func(p *Params) { p.App.LosePostsOnRestart = true })},
		{"bug: home timelines lost on flush", check.Fail, mod(func(p *Params) { p.App.LoseHomeOnFlush = true })},
		{"bug: follower-cache partial after flush", check.Fail, mod(func(p *Params) { p.App.FollowerCache = true })},
		{"bug: page offset shifted", check.Fail, mod(func(p *Params) { p.Mangle.ShiftPage = true })},
	}
}

// TestSynthetic runs the in-process fake application with concurrent clients, correct and with
// each injected bug, through both specs. The bound of the model is the fake's (300 ms).
func TestSynthetic(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	for _, sc := range scenarios() {
		t.Run(sc.name, func(t *testing.T) {
			p := sc.params()
			out := runSim(p)
			h := check.NewHistory(out.Calls, out.Faults)
			o := opts()
			a := check.Check(h, []check.AnySpec{AuthorSpec(out.Data, p.Bound, Slack)}, o)
			r := check.Check(h, []check.AnySpec{ReaderSpec(out.Data, p.Bound, Slack)}, o)
			got := both(a, r)
			t.Logf("author %s (%v), reader %s (%v)", a.Verdict, a.Elapsed.Round(time.Millisecond), r.Verdict, r.Elapsed.Round(time.Millisecond))
			if got != sc.want {
				t.Errorf("got %s, want %s\n%s\n%s", got, sc.want, a.Text(3), r.Text(3))
			}
		})
	}
}
