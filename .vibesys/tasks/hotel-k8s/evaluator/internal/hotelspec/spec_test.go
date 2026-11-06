package hotelspec

import (
	"slices"
	"testing"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"
)

var cat = fakeCatalog{Hotels: 6, Cap: 3, Cluster: 3}

func opts() check.Options {
	return check.Options{PartitionTimeout: 20 * time.Second, Workers: 4, MaxLostShare: 0.5}
}

func verdictOf(t *testing.T, phases [][]check.Call, faults []check.Fault) *check.Report {
	t.Helper()
	r := check.Check(check.NewHistory(phases, faults), []check.AnySpec{Spec(cat)}, opts())
	if r.Verdict == check.Fail {
		t.Logf("%s", r.Text(3))
	}
	return r
}

func run(t *testing.T, p simParams) *check.Report {
	t.Helper()
	phases, faults := simulate(p, cat)
	return verdictOf(t, phases, faults)
}

func TestCorrectConcurrentHistoriesPass(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		p := defaultSim(seed)
		if seed%2 == 0 {
			p.Unknowns = 0.2
		}
		r := run(t, p)
		if r.Verdict != check.Pass {
			t.Fatalf("seed %d: %s\n%s", seed, r.Verdict, r.Text(10))
		}
	}
}

// Each bug, its simulator knobs, and how many of 10 seeds must catch it.
func TestInjectedBugsCaught(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*simParams)
		min  int
	}{
		{"overbooking race", func(p *simParams) { p.Bugs.Race = true; p.ReserveHotels = 2 }, 8},
		{"reservations lost after a restart", func(p *simParams) { p.Bugs.LoseOnRestart = true }, 8},
		{"stale availability beyond 1 s", func(p *simParams) {
			p.Bugs.StaleAvailability = 3 * time.Second
			p.OpsPerEpoch = 600 // longer epochs: reads more than 1 s after writes
		}, 8},
		{"search shows a full hotel", func(p *simParams) { p.Bugs.SearchOffByOne = true }, 8},
		{"flush loses durable profile writes", func(p *simParams) { p.Bugs.FlushLosesProfile = true }, 8},
		{"flush loses durable reservations", func(p *simParams) { p.Bugs.FlushLosesReservations = true }, 8},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			caught := 0
			for seed := int64(1); seed <= 10; seed++ {
				p := defaultSim(seed)
				c.mod(&p)
				switch r := run(t, p); r.Verdict {
				case check.Fail:
					caught++
				case check.Unknown:
					t.Errorf("seed %d: unknown: %s", seed, r.Reason)
				}
			}
			t.Logf("caught in %d/10 seeds", caught)
			if caught < c.min {
				t.Fatalf("caught in %d/10 seeds, want >= %d", caught, c.min)
			}
		})
	}
}

// -------------------------------------------------------------- crafted edge cases

const ms = int64(time.Millisecond)

// b is a builder over one phase for crafted histories.
func b1() *builder { return &builder{cat: cat} }

func (b *builder) reserve(call, ret int64, hotel string, night string, rooms int, msg string) {
	b.add("reserve", ReserveInfo{Hotel: hotel, Nights: []string{night}, Rooms: rooms, Valid: true}, call, ret, 200, msgBody(msg))
}

func (b *builder) search(call, ret int64, hotel string, night string, present bool, p Profile) {
	var shown []string
	if present {
		shown = []string{hotel}
	}
	b.add("search", SearchInfo{Nights: []string{night}, Candidates: []string{hotel}}, call, ret, 200, b.features(shown, map[string]Profile{hotel: p}))
}

func (b *builder) setProfile(call, ret int64, hotel, phone string) {
	b.add("set_profile", ProfileInfo{Hotel: hotel, Phone: phone}, call, ret, 200, msgBody(msgProfile))
}

func (b *builder) recommend(call, ret int64, hotel string, p Profile) {
	b.add("recommend_dis", RecommendInfo{Require: "dis", Expected: []string{hotel}}, call, ret, 200, b.features([]string{hotel}, map[string]Profile{hotel: p}))
}

// The 1 s bound edge: a profile write acknowledged at 100 ms; a read that starts 1 s later may
// still show the old value, one that starts any later must show the new one.
func TestBoundEdge(t *testing.T) {
	old := Profile{"Hotel 1", "seed-1"}
	nw := Profile{"Hotel 1", "new"}
	cases := []struct {
		at   int64
		p    Profile
		want check.Verdict
	}{
		{50 * ms, old, check.Pass},                     // concurrent: either
		{50 * ms, nw, check.Pass},                      // may be visible early
		{100*ms + int64(time.Second), old, check.Pass}, // exactly at the bound: not "more than 1 s later"
		{100*ms + int64(time.Second) + 1, old, check.Fail},
		{100*ms + int64(time.Second) + 1, nw, check.Pass},
	}
	for _, c := range cases {
		b := b1()
		b.setProfile(0, 100*ms, "1", "new")
		b.recommend(c.at, c.at+5*ms, "1", c.p)
		if got := verdictOf(t, b.calls, nil).Verdict; got != c.want {
			t.Errorf("read at %d ms showing %v: %s, want %s", c.at/ms, c.p, got, c.want)
		}
	}
}

// Availability is under the bound, reservations are not: right after a reservation fills a
// night a search may still show the hotel, a second later it must not, and a reservation
// right after must be refused.
func TestAvailabilityBoundAndStrictReservations(t *testing.T) {
	seed := Profile{"Hotel 1", "seed-1"}
	for _, c := range []struct {
		name string
		at   int64
		want check.Verdict
	}{{"stale availability within 1 s", 20 * ms, check.Pass}, {"stale availability after 2 s", 2000 * ms, check.Fail}} {
		b := b1()
		b.reserve(0, 10*ms, "1", "d1", 3, MsgReserved)
		b.search(c.at, c.at+5*ms, "1", "d1", true, seed)
		if v := verdictOf(t, b.calls, nil).Verdict; v != c.want {
			t.Errorf("%s: %s, want %s", c.name, v, c.want)
		}
	}
	b := b1()
	b.reserve(0, 10*ms, "1", "d1", 3, MsgReserved)
	b.reserve(20*ms, 30*ms, "1", "d1", 1, MsgReserved)
	if v := verdictOf(t, b.calls, nil).Verdict; v != check.Fail {
		t.Errorf("overbooking right after a fill: %s", v)
	}
}

// A search that shows a hotel whose night has been full for 2 s fails, one that omits a hotel
// with free rooms fails too.
func TestSearchAvailabilityBothWays(t *testing.T) {
	seed := Profile{"Hotel 1", "seed-1"}
	b := b1()
	b.search(0, 5*ms, "1", "d1", false, Profile{})
	if v := verdictOf(t, b.calls, nil).Verdict; v != check.Fail {
		t.Errorf("a hotel with free rooms omitted: %s", v)
	}
	b = b1()
	b.search(0, 5*ms, "1", "d1", true, seed)
	if v := verdictOf(t, b.calls, nil).Verdict; v != check.Pass {
		t.Errorf("a hotel with free rooms shown: %s", v)
	}
}

// The pinned frontend reserves rooms even when the login failed.
func TestFailedLoginStillReserves(t *testing.T) {
	b := b1()
	b.add("reserve", ReserveInfo{Hotel: "1", Nights: []string{"d1"}, Rooms: 3, Valid: false}, 0, 10*ms, 200, msgBody(MsgLoginFail))
	b.reserve(20*ms, 30*ms, "1", "d1", 1, MsgFull)
	if v := verdictOf(t, b.calls, nil).Verdict; v != check.Pass {
		t.Errorf("rooms taken by a failed login: %s", v)
	}
	b = b1()
	b.add("reserve", ReserveInfo{Hotel: "1", Nights: []string{"d1"}, Rooms: 3, Valid: false}, 0, 10*ms, 200, msgBody(MsgLoginFail))
	b.reserve(20*ms, 30*ms, "1", "d1", 1, MsgReserved)
	if v := verdictOf(t, b.calls, nil).Verdict; v != check.Fail {
		t.Errorf("rooms not taken by a failed login: %s", v)
	}
}

// A lost response: the reservation may or may not have happened.
func TestUnknownOutcome(t *testing.T) {
	for _, msg := range []string{MsgReserved, MsgFull} {
		b := b1()
		b.add("reserve", ReserveInfo{Hotel: "1", Nights: []string{"d1"}, Rooms: 3, Valid: true}, 0, 10*ms, 0, nil)
		b.reserve(2000*ms, 2010*ms, "1", "d1", 1, msg)
		if v := verdictOf(t, b.calls, nil).Verdict; v != check.Pass {
			t.Errorf("after a lost reservation, %q: %s", msg, v)
		}
	}
	// A lost write takes effect within EffectWindow or never: after that the answer is settled.
	b := b1()
	b.add("reserve", ReserveInfo{Hotel: "1", Nights: []string{"d1"}, Rooms: 3, Valid: true}, 0, 10*ms, 0, nil)
	late := int64(EffectWindow) + 5000*ms
	b.reserve(late, late+10*ms, "1", "d1", 3, MsgReserved)
	b.reserve(late+100*ms, late+110*ms, "1", "d1", 1, MsgFull)
	if v := verdictOf(t, b.calls, nil).Verdict; v != check.Pass {
		t.Errorf("a lost reservation that never applied: %s", v)
	}
}

// Faults change nothing: a reservation acknowledged before a restart still counts after it, and
// a fault with an operation in flight is a capture problem.
func TestRestartKeepsReservations(t *testing.T) {
	mk := func() *builder {
		b := b1()
		b.reserve(0, 10*ms, "1", "d1", 3, MsgReserved)
		return b
	}
	b := mk()
	b.phase = 1
	b.reserve(5000*ms, 5010*ms, "1", "d1", 1, MsgReserved)
	faults := []check.Fault{{Kind: "restart_all", Target: "app", Start: 1000 * ms, End: 4000 * ms, AfterPhase: 0}}
	if v := verdictOf(t, b.calls, faults).Verdict; v != check.Fail {
		t.Errorf("a reservation lost across a restart: %s", v)
	}
	b = mk()
	if r := verdictOf(t, b.calls, []check.Fault{{Kind: "restart_all", Start: 5 * ms, End: 6 * ms}}); r.Verdict != check.Unknown {
		t.Errorf("a fault inside a call: %s, want unknown", r.Verdict)
	}
}

// Violations: a body that is not what the application answers fails without a search.
func TestStaticViolations(t *testing.T) {
	seed := Profile{"Hotel 1", "seed-1"}
	bad := map[string]func(b *builder){
		"search shows a hotel that is not a candidate": func(b *builder) {
			b.add("search", SearchInfo{Nights: []string{"d1"}, Candidates: []string{"2"}}, 0, ms, 200, b.features([]string{"1"}, map[string]Profile{"1": seed}))
		},
		"wrong recommended hotel": func(b *builder) {
			b.add("recommend_rate", RecommendInfo{Require: "rate", Expected: []string{"2"}}, 0, ms, 200, b.features([]string{"1"}, map[string]Profile{"1": seed}))
		},
		"login answered wrong": func(b *builder) {
			b.add("login", LoginInfo{Valid: true}, 0, ms, 200, msgBody(MsgLoginFail))
		},
		"reservation answered a made-up message": func(b *builder) { b.reserve(0, ms, "1", "d1", 1, "ok") },
		"a body that is not JSON": func(b *builder) {
			b.add("rates", HotelInfo{Hotel: "1"}, 0, ms, 200, []byte("<html>"))
		},
		"an unknown field in GeoJSON": func(b *builder) {
			b.add("search", SearchInfo{Nights: []string{"d1"}, Candidates: []string{"1"}}, 0, ms, 200, []byte(`{"type":"FeatureCollection","features":[],"extra":1}`))
		},
		"a 404": func(b *builder) {
			b.add("search", SearchInfo{Nights: []string{"d1"}, Candidates: []string{"1"}}, 0, ms, 404, nil)
		},
	}
	for name, f := range bad {
		b := b1()
		f(b)
		r := verdictOf(t, b.calls, nil)
		if r.Verdict != check.Fail || len(r.Specs[0].Violations) != 1 {
			t.Errorf("%s: %s, %d violations", name, r.Verdict, len(r.Specs[0].Violations))
		}
	}
	b := b1()
	b.add("teleport", HotelInfo{Hotel: "1"}, 0, ms, 200, nil)
	if r := verdictOf(t, b.calls, nil); r.Verdict != check.Unknown {
		t.Errorf("an operation the oracle does not know: %s, want unknown", r.Verdict)
	}
}

// Too many lost responses fail the check at the default share.
func TestLostShare(t *testing.T) {
	b := b1()
	for i := range 20 {
		b.add("reserve", ReserveInfo{Hotel: "1", Nights: []string{"d1"}, Rooms: 0, Valid: true}, int64(i)*ms*10, int64(i)*ms*10+ms, 0, nil)
	}
	r := check.Check(check.NewHistory(b.calls, nil), []check.AnySpec{Spec(cat)}, check.Options{})
	if r.Verdict != check.Fail || !r.Specs[0].LostShareExceeded {
		t.Errorf("all responses lost: %s", r.Verdict)
	}
}

// Pruning by Floor keeps states small: a long hot history stays cheap and the verdict does not
// depend on the seed of the read sample.
func TestSampleSeedDoesNotChangeVerdicts(t *testing.T) {
	for seed := int64(1); seed <= 3; seed++ {
		for _, bug := range []bool{false, true} {
			p := defaultSim(seed)
			p.Bugs.SearchOffByOne = bug
			phases, faults := simulate(p, cat)
			var got []check.Verdict
			for _, s := range []int64{1, 2} {
				o := opts()
				o.Seed, o.MaxReadsPerKey = s, 40
				got = append(got, check.Check(check.NewHistory(phases, faults), []check.AnySpec{Spec(cat)}, o).Verdict)
			}
			if !bug && !slices.Equal(got, []check.Verdict{check.Pass, check.Pass}) {
				t.Errorf("seed %d: %v on a correct history", seed, got)
			}
		}
	}
}
