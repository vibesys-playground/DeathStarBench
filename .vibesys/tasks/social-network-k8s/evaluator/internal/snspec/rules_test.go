package snspec

import (
	"encoding/json"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/snapi"
)

// Handcrafted histories, no dataset: users register first. Times in ms from t0.
var t0 = time.Unix(1_800_000_000, 0)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

type hb struct{ calls []snapi.Call }

func (h *hb) reg(id int64, ms int) {
	n := "u" + strconv.FormatInt(id, 10)
	h.calls = append(h.calls, snapi.Call{Action: snapi.Register(id, n), Start: at(ms), End: at(ms + 1), Status: 200, Body: []byte("Success!")})
}
func (h *hb) compose(u int64, marker string, s, e int, mentions ...int64) {
	a := snapi.Action{Kind: snapi.KindCompose, User: u, Name: "u" + strconv.FormatInt(u, 10), Marker: marker, Text: "hello #" + marker, Mentions: mentions}
	h.calls = append(h.calls, snapi.Call{Action: a, Start: at(s), End: at(e), Status: 200, Body: []byte("Successfully upload post")})
}
func (h *hb) composeErr(u int64, marker string, s, e int) {
	a := snapi.Action{Kind: snapi.KindCompose, User: u, Name: "u" + strconv.FormatInt(u, 10), Marker: marker, Text: "hello #" + marker}
	h.calls = append(h.calls, snapi.Call{Action: a, Start: at(s), End: at(e), Err: "timeout"})
}
func (h *hb) follow(u, v int64, on bool, s, e int) {
	h.calls = append(h.calls, snapi.Call{Action: snapi.Follow(u, v, on), Start: at(s), End: at(e), Status: 200, Body: []byte("Success!")})
}

type shown struct {
	id     string
	author int64
	marker string
	ts     int
	ment   []int64
}

func (h *hb) read(home bool, u int64, s, e int, posts ...shown) {
	var arr []map[string]any
	for _, p := range posts {
		um := []map[string]string{}
		for _, m := range p.ment {
			um = append(um, map[string]string{"user_id": strconv.FormatInt(m, 10), "username": "u" + strconv.FormatInt(m, 10)})
		}
		var ums any = um
		if len(um) == 0 {
			ums = map[string]any{}
		}
		arr = append(arr, map[string]any{"post_id": p.id, "req_id": "1", "text": "hello #" + p.marker, "timestamp": strconv.FormatInt(at(p.ts).UnixMilli(), 10),
			"post_type": 0, "creator": map[string]string{"user_id": strconv.FormatInt(p.author, 10), "username": "u" + strconv.FormatInt(p.author, 10)},
			"user_mentions": ums, "media": map[string]any{}, "urls": map[string]any{}})
	}
	var body []byte
	if len(arr) == 0 {
		body = []byte("{}")
	} else {
		body, _ = json.Marshal(arr)
	}
	h.calls = append(h.calls, snapi.Call{Action: snapi.Read(home, u, 0, 10), Start: at(s), End: at(e), Status: 200, Body: body})
}

func (h *hb) raw(home bool, uid, st, sp string, status int, body string, s int) {
	h.calls = append(h.calls, snapi.Call{Action: snapi.RawRead(home, uid, st, sp), Start: at(s), End: at(s + 1), Status: status, Body: []byte(body)})
}

// options: the unit histories lose responses on purpose.
func opts() check.Options {
	return check.Options{PartitionTimeout: 20 * time.Second, Workers: 4, MaxLostShare: 0.5}
}

// phased splits calls into phases at the faults (a call belongs to the phase of the faults that
// started before it) and converts them to the history's form.
func phased(calls []snapi.Call, faults []check.Fault) [][]check.Call {
	calls = append([]snapi.Call(nil), calls...)
	sort.SliceStable(calls, func(i, j int) bool { return calls[i].Start.Before(calls[j].Start) })
	phases := make([][]check.Call, len(faults)+1)
	next := make([]uint64, len(faults)+1)
	for _, c := range calls {
		if c.Start.IsZero() {
			continue
		}
		p := 0
		for p < len(faults) && c.Start.UnixNano() > faults[p].Start {
			p++
		}
		phases[p] = append(phases[p], FromOracleCall(c, p, next[p]))
		next[p]++
	}
	return phases
}

// checkBoth checks calls with both specs (the nil dataset: only registered users and live
// posts) and returns the two reports.
func checkBoth(d *dataset.Dataset, calls []snapi.Call, faults []check.Fault, o check.Options) (author, reader *check.Report) {
	h := check.NewHistory(phased(calls, faults), faults)
	author = check.Check(h, []check.AnySpec{DefaultAuthorSpec(d)}, o)
	reader = check.Check(h, []check.AnySpec{DefaultReaderSpec(d)}, o)
	return
}

func both(a, r *check.Report) check.Verdict {
	switch {
	case a.Verdict == check.Fail || r.Verdict == check.Fail:
		return check.Fail
	case a.Verdict == check.Unknown || r.Verdict == check.Unknown:
		return check.Unknown
	}
	return check.Pass
}

const A, R, M int64 = 3_000_000_001, 3_000_000_002, 3_000_000_003 // author, reader, mentioned

func baseH() *hb {
	h := &hb{}
	h.reg(A, 0)
	h.reg(R, 0)
	h.reg(M, 0)
	return h
}

func TestRules(t *testing.T) {
	post := func(id, mk string, ts int, ment ...int64) shown { return shown{id, A, mk, ts, ment} }
	ms := func(n int) int64 { return at(n).UnixNano() }
	cases := []struct {
		name   string
		want   check.Verdict
		f      func(h *hb)
		faults []check.Fault
	}{
		{"follow, compose, read after B shows post", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 2000, 2010)
			h.read(true, R, 4000, 4010, post("1001", "m1", 2005))
		}, nil},
		{"a follow not reflected: follow, compose, read after B MISSING post", check.Fail, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 2000, 2010)
			h.read(true, R, 4000, 4010)
		}, nil},
		{"read within B of ack: missing is fine", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 2000, 2010)
			h.read(true, R, 2500, 2510)
		}, nil},
		{"read within B of ack: shown is fine", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 2000, 2010)
			h.read(true, R, 2500, 2510, post("1001", "m1", 2005))
		}, nil},
		{"follow acked less than B before compose: may or may not (absent)", check.Pass, func(h *hb) {
			h.follow(R, A, true, 1900, 1950)
			h.compose(A, "m1", 2000, 2010)
			h.read(true, R, 4000, 4010)
		}, nil},
		{"post composed BEFORE the follow: absent is fine", check.Pass, func(h *hb) {
			h.compose(A, "m1", 100, 110)
			h.follow(R, A, true, 3000, 3010)
			h.read(true, R, 6000, 6010)
		}, nil},
		{"post composed BEFORE the follow: shown is fine", check.Pass, func(h *hb) {
			h.compose(A, "m1", 100, 110)
			h.follow(R, A, true, 3000, 3010)
			h.read(true, R, 6000, 6010, post("1001", "m1", 105))
		}, nil},
		{"never followed: shown post is a violation", check.Fail, func(h *hb) {
			h.compose(A, "m1", 100, 110)
			h.read(true, R, 4000, 4010, post("1001", "m1", 105))
		}, nil},
		{"unfollow >B before compose: post shown is a violation", check.Fail, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.follow(R, A, false, 1000, 1010)
			h.compose(A, "m1", 3000, 3010)
			h.read(true, R, 6000, 6010, post("1001", "m1", 3005))
		}, nil},
		{"unfollow >B before compose: absent is fine", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.follow(R, A, false, 1000, 1010)
			h.compose(A, "m1", 3000, 3010)
			h.read(true, R, 6000, 6010)
		}, nil},
		{"unfollow <B before compose: shown is fine", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.follow(R, A, false, 2500, 2510)
			h.compose(A, "m1", 3000, 3010)
			h.read(true, R, 6000, 6010, post("1001", "m1", 3005))
		}, nil},
		{"post composed before the unfollow stays: absent after is fine", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 1000, 1010)
			h.follow(R, A, false, 2000, 2010)
			h.read(true, R, 6000, 6010)
		}, nil},
		{"overlapping follow/unfollow of one edge: uncertain, absent", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 500)
			h.follow(R, A, false, 100, 600)
			h.compose(A, "m1", 3000, 3010)
			h.read(true, R, 6000, 6010)
		}, nil},
		{"overlapping follow/unfollow of one edge: uncertain, shown", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 500)
			h.follow(R, A, false, 100, 600)
			h.compose(A, "m1", 3000, 3010)
			h.read(true, R, 6000, 6010, post("1001", "m1", 3005))
		}, nil},
		{"mention delivers without an edge: missing is a violation", check.Fail, func(h *hb) {
			h.compose(A, "m1", 100, 110, M)
			h.read(true, M, 4000, 4010)
		}, nil},
		{"mention delivers without an edge: shown", check.Pass, func(h *hb) {
			h.compose(A, "m1", 100, 110, M)
			h.read(true, M, 4000, 4010, post("1001", "m1", 105, M))
		}, nil},
		{"a lost post: user timeline read after the compose misses it", check.Fail, func(h *hb) {
			h.compose(A, "m1", 100, 110)
			h.read(false, A, 120, 130)
		}, nil},
		{"user timeline: shown", check.Pass, func(h *hb) {
			h.compose(A, "m1", 100, 110)
			h.read(false, A, 120, 130, post("1001", "m1", 105))
		}, nil},
		{"failed (timeout) compose: shown later is fine", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.composeErr(A, "m1", 100, 5000)
			h.read(true, R, 9000, 9010, post("1001", "m1", 105))
		}, nil},
		{"failed (timeout) compose: absent is fine", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.composeErr(A, "m1", 100, 5000)
			h.read(true, R, 9000, 9010)
		}, nil},
		{"post never composed", check.Fail, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.read(true, R, 4000, 4010, post("1001", "mX", 105))
		}, nil},
		{"same post, two ids", check.Fail, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 100, 110)
			h.read(true, R, 4000, 4010, post("1001", "m1", 105))
			h.read(true, R, 5000, 5010, post("1002", "m1", 105))
		}, nil},
		{"order: older before newer", check.Fail, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 100, 110)
			h.compose(A, "m2", 200, 210)
			h.read(true, R, 4000, 4010, post("1001", "m1", 105), post("1002", "m2", 205))
		}, nil},
		{"order: newest first ok", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 100, 110)
			h.compose(A, "m2", 200, 210)
			h.read(true, R, 4000, 4010, post("1002", "m2", 205), post("1001", "m1", 105))
		}, nil},
		{"invalid read (non-numeric user) answered 200 with posts", check.Fail, func(h *hb) {
			h.compose(A, "m1", 100, 110)
			h.raw(true, "abc", "0", "10", 200, `[{"post_id":"1"}]`, 200)
		}, nil},
		{"invalid read (start>=stop) refused", check.Pass, func(h *hb) {
			h.raw(true, strconv.FormatInt(R, 10), "5", "5", 400, "bad", 200)
		}, nil},
		{"invalid read (start>=stop) answered empty", check.Pass, func(h *hb) {
			h.raw(true, strconv.FormatInt(R, 10), "5", "5", 200, "{}", 200)
		}, nil},
		{"read of unregistered user answered with posts", check.Fail, func(h *hb) {
			h.compose(A, "m1", 100, 110)
			h.read(false, 3_999_999_999, 200, 210, post("1001", "m1", 105))
		}, nil},
		{"restart in the middle changes nothing", check.Pass, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 1500, 1510)
			h.read(true, R, 4000, 4010, shown{"1001", A, "m1", 1505, nil})
		}, []check.Fault{{Kind: "restart", Target: "home-timeline-service", Start: ms(2000), End: ms(2100), AfterPhase: 0}}},
		{"a timeline entry missing after a restart (read >B after ack)", check.Fail, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 1500, 1510)
			h.read(true, R, 4000, 4010)
		}, []check.Fault{{Kind: "restart", Target: "home-timeline-service", Start: ms(2000), End: ms(2100), AfterPhase: 0}}},
		{"a post lost after a restart: user timeline", check.Fail, func(h *hb) {
			h.compose(A, "m1", 1500, 1510)
			h.read(false, A, 4000, 4010)
		}, []check.Fault{{Kind: "restart", Target: "post-storage-service", Start: ms(2000), End: ms(2100), AfterPhase: 0}}},
		{"home timelines flushed: entries must come back from the graph", check.Fail, func(h *hb) {
			h.follow(R, A, true, 10, 20)
			h.compose(A, "m1", 1500, 1510)
			h.read(true, R, 4000, 4010)
		}, []check.Fault{{Kind: "flush", Target: "home-timeline-redis", Start: ms(2000), End: ms(2010), AfterPhase: 0}}},
	}
	for _, c := range cases {
		h := baseH()
		c.f(h)
		a, r := checkBoth(nil, h.calls, c.faults, opts())
		got := both(a, r)
		if got != c.want {
			why := ""
			for _, rep := range []*check.Report{a, r} {
				if rep.Verdict != check.Pass {
					why += rep.Text(3)
				}
			}
			t.Errorf("%s: got %s (author %s, reader %s), want %s\n%s", c.name, got, a.Verdict, r.Verdict, c.want, why)
		}
	}
}
