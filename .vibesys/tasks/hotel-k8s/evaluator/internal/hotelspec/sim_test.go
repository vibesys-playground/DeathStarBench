package hotelspec

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"
)

// fakeCatalog is a tiny seeded catalog for synthetic histories: hotels "1".."Hotels", all of
// capacity Cap, two rate plans, and search candidates in groups of Cluster.
type fakeCatalog struct {
	Hotels  int
	Cap     int
	Cluster int
}

func (c fakeCatalog) id(i int) string { return fmt.Sprint(i + 1) }

func (c fakeCatalog) Hotel(id string) (*Hotel, bool) {
	n, err := strconv.Atoi(id)
	if err != nil || n < 1 || n > c.Hotels {
		return nil, false
	}
	return &Hotel{
		ID: id, Capacity: c.Cap, Profile: Profile{Name: "Hotel " + id, Phone: "seed-" + id},
		Lon: float64(n) + 0.25, Lat: float64(n) + 0.5,
		Plans: []Plan{
			{Code: "A", InDate: "2015-04-09", OutDate: "2015-04-10", RoomCode: "RA", RoomDescription: "room A", Prices: Prices{100, 100, 110}},
			{Code: "B", InDate: "2015-04-09", OutDate: "2015-04-10", RoomCode: "RB", RoomDescription: "room B", Prices: Prices{200, 200, 220}},
		},
	}, true
}

// bugs injectable into the simulated application.
type bugs struct {
	// Race: a reservation checks the rooms when it starts and takes them later, so overlapping
	// reservations do not see each other (check-then-act).
	Race bool
	// LoseOnRestart: a restart loses the reservations taken since the previous fault (they
	// lived in a cache that was never written back).
	LoseOnRestart bool
	// StaleAvailability: search reads availability up to this old (> 1 s is a bug).
	StaleAvailability time.Duration
	// SearchOffByOne: search shows a hotel whose night is exactly full.
	SearchOffByOne bool
	// FlushLosesProfile: profile writes live only in the cache; a flush reverts them.
	FlushLosesProfile bool
	// FlushLosesReservations: reservations live only in memcached; a flush forgets them.
	FlushLosesReservations bool
}

// simParams shape a synthetic run: epochs (phases) of concurrent closed-loop clients, with a
// fault between epochs (restart, flush, alternating) at a quiescent point.
type simParams struct {
	Seed                 int64
	Clients, OpsPerEpoch int
	Epochs               int
	Nights               int
	MeanDur, MeanThink   time.Duration
	Lag                  time.Duration // legitimate staleness of reads (< 1 s)
	FaultGap             time.Duration // a restart's downtime
	Unknowns             float64       // share of writes whose response is lost
	Mix                  [7]int        // weights: reserve, search, read-profile, read-rates, set-capacity, set-profile, set-rate
	ReserveHotels        int           // reservations go to the first ReserveHotels hotels (0: all)
	Bugs                 bugs
}

func defaultSim(seed int64) simParams {
	return simParams{Seed: seed, Clients: 8, OpsPerEpoch: 300, Epochs: 4, Nights: 24,
		MeanDur: 20 * time.Millisecond, MeanThink: 30 * time.Millisecond, Lag: 500 * time.Millisecond,
		FaultGap: 3 * time.Second, Mix: [7]int{30, 35, 10, 10, 3, 8, 4}}
}

type tv[V any] struct {
	t int64
	v V
}

// versioned is a value's history in effect order (the simulated application's truth).
type versioned[V any] struct{ h []tv[V] }

func (x *versioned[V]) at(t int64) V {
	i := sort.Search(len(x.h), func(i int) bool { return x.h[i].t > t })
	if i == 0 {
		return x.h[0].v
	}
	return x.h[i-1].v
}
func (x *versioned[V]) latest() V        { return x.h[len(x.h)-1].v }
func (x *versioned[V]) set(t int64, v V) { x.h = append(x.h, tv[V]{t, v}) }

type slot struct {
	call, ret, eff int64 // eff: when it takes effect (its linearization point)
}

func js(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// builder assembles the calls of one run, with the bodies the application answers.
type builder struct {
	cat   fakeCatalog
	phase int
	index uint64
	calls [][]check.Call
}

func (b *builder) add(op string, info any, call, ret int64, status int, body []byte) {
	c := check.Call{Phase: b.phase, Index: b.index, Client: -1, Op: op, Info: js(info), Start: call, End: ret}
	b.index++
	st := check.Step{Start: call, End: ret, Status: status, Body: body}
	if status == 0 {
		st.Error = "timeout"
	}
	c.Steps = []check.Step{st}
	for len(b.calls) <= b.phase {
		b.calls = append(b.calls, nil)
	}
	b.calls[b.phase] = append(b.calls[b.phase], c)
}

func msgBody(m string) []byte { return js(map[string]string{"message": m}) }

func (b *builder) features(hotels []string, profiles map[string]Profile) []byte {
	type feat struct {
		Type       string            `json:"type"`
		ID         string            `json:"id"`
		Properties map[string]string `json:"properties"`
		Geometry   map[string]any    `json:"geometry"`
	}
	out := struct {
		Type     string `json:"type"`
		Features []feat `json:"features"`
	}{Type: "FeatureCollection", Features: []feat{}}
	for _, id := range hotels {
		h, _ := b.cat.Hotel(id)
		p := profiles[id]
		out.Features = append(out.Features, feat{"Feature", id, map[string]string{"name": p.Name, "phone_number": p.Phone},
			map[string]any{"type": "Point", "coordinates": []float64{h.Lon, h.Lat}}})
	}
	return js(out)
}

func (b *builder) ratesBody(id string, prices map[string]Prices) []byte {
	h, _ := b.cat.Hotel(id)
	type room struct {
		Bookable    float64 `json:"bookableRate"`
		Code        string  `json:"code"`
		Description string  `json:"roomDescription"`
		Total       float64 `json:"totalRate"`
		Inclusive   float64 `json:"totalRateInclusive"`
	}
	type pl struct {
		Code     string `json:"code"`
		InDate   string `json:"inDate"`
		OutDate  string `json:"outDate"`
		RoomType room   `json:"roomType"`
	}
	var plans []pl
	for _, p := range h.Plans {
		pr := prices[p.Code]
		plans = append(plans, pl{p.Code, p.InDate, p.OutDate, room{pr.Bookable, p.RoomCode, p.RoomDescription, pr.Total, pr.Inclusive}})
	}
	return js(map[string]any{"hotelId": id, "ratePlans": plans})
}

// simulate runs the synthetic application and returns the history it would have recorded.
func simulate(p simParams, cat fakeCatalog) ([][]check.Call, []check.Fault) {
	rng := rand.New(rand.NewSource(p.Seed))
	nights := make([]string, p.Nights)
	for i := range nights {
		nights[i] = fmt.Sprintf("3000-%02d-%02d", 1+i/28, 1+i%28)
	}
	type hotelSim struct {
		capacity versioned[int]
		rooms    map[string]*versioned[int]
		profile  versioned[Profile]
		durable  Profile // what a flush falls back to under FlushLosesProfile
		prices   map[string]*versioned[Prices]
	}
	hotels := make([]*hotelSim, cat.Hotels)
	for i := range hotels {
		id := cat.id(i)
		h := &hotelSim{rooms: map[string]*versioned[int]{}, prices: map[string]*versioned[Prices]{}}
		hh, _ := cat.Hotel(id)
		h.capacity.set(-1<<62, cat.Cap)
		h.profile.set(-1<<62, hh.Profile)
		h.durable = hh.Profile
		for _, pl := range hh.Plans {
			v := &versioned[Prices]{}
			v.set(-1<<62, pl.Prices)
			h.prices[pl.Code] = v
		}
		for _, d := range nights {
			v := &versioned[int]{}
			v.set(-1<<62, 0)
			h.rooms[d] = v
		}
		hotels[i] = h
	}
	b := &builder{cat: cat}
	var faults []check.Fault
	t := int64(time.Second)
	seq := 0
	ms := func(d time.Duration) int64 { return int64(d) }
	for epoch := 0; epoch < p.Epochs; epoch++ {
		b.phase, b.index = epoch, 0
		// Schedule the epoch's slots per client, then apply them in effect order.
		var slots []slot
		end := t
		for c := 0; c < p.Clients; c++ {
			at := t + rng.Int63n(ms(p.MeanThink)+1)
			for i := 0; i < p.OpsPerEpoch/p.Clients; i++ {
				d := 1 + rng.Int63n(2*ms(p.MeanDur))
				s := slot{call: at, ret: at + d}
				s.eff = s.call + rng.Int63n(d+1)
				slots = append(slots, s)
				at += d + 1 + rng.Int63n(2*ms(p.MeanThink)+1)
				end = max(end, s.ret)
			}
		}
		sort.Slice(slots, func(i, j int) bool { return slots[i].eff < slots[j].eff })
		epochStart := map[*versioned[int]]int{}
		for _, h := range hotels {
			for _, r := range h.rooms {
				epochStart[r] = len(r.h)
			}
		}
		total := 0
		for _, w := range p.Mix {
			total += w
		}
		for _, s := range slots {
			seq++
			hi := rng.Intn(cat.Hotels)
			// Nights come from a window of three that slides over the run, so each night is
			// contended for a while. Stays are one night: the base does not take a multi-night
			// stay's rooms atomically.
			window := (seq - 1) * max(1, len(nights)-3) / max(1, p.Epochs*p.OpsPerEpoch)
			k := min(len(nights)-1, window+rng.Intn(3))
			stay := []string{nights[k]}
			lost := rng.Float64() < p.Unknowns
			x := rng.Intn(total)
			kind := 0
			for kind = range p.Mix {
				if x < p.Mix[kind] {
					break
				}
				x -= p.Mix[kind]
			}
			if kind == 0 && p.ReserveHotels > 0 {
				hi = rng.Intn(p.ReserveHotels)
			}
			h := hotels[hi]
			id := cat.id(hi)
			status := 200
			if lost {
				status = 0
			}
			body := func(m string) []byte {
				if lost {
					return nil
				}
				return msgBody(m)
			}
			switch kind {
			case 0: // reserve
				rooms := 1 + rng.Intn(2)
				valid := rng.Intn(10) > 0
				decideAt := s.eff
				if p.Bugs.Race {
					decideAt = s.call
				}
				fits := true
				c := h.capacity.at(decideAt)
				for _, d := range stay {
					if h.rooms[d].at(decideAt)+rooms > c {
						fits = false
					}
				}
				msg := MsgFull
				if fits {
					for _, d := range stay {
						h.rooms[d].set(s.eff, h.rooms[d].latest()+rooms)
					}
					msg = MsgReserved
					if !valid {
						msg = MsgLoginFail
					}
				}
				b.add("reserve", ReserveInfo{Hotel: id, Nights: stay, Rooms: rooms, Valid: valid}, s.call, s.ret, status, body(msg))
			case 1: // search over the hotel's cluster
				base := hi / cat.Cluster * cat.Cluster
				var cands, shown []string
				profiles := map[string]Profile{}
				for k := base; k < base+cat.Cluster && k < cat.Hotels; k++ {
					hk := hotels[k]
					lag := p.Lag
					if p.Bugs.StaleAvailability > 0 {
						lag = p.Bugs.StaleAvailability
					}
					at := s.eff - rng.Int63n(ms(lag)+1)
					c := hk.capacity.at(at)
					present := true
					for _, d := range stay {
						r := hk.rooms[d].at(at)
						if p.Bugs.SearchOffByOne {
							if r > c {
								present = false
							}
						} else if r+1 > c {
							present = false
						}
					}
					cands = append(cands, cat.id(k))
					if present {
						shown = append(shown, cat.id(k))
						profiles[cat.id(k)] = hk.profile.at(s.eff - rng.Int63n(ms(p.Lag)+1))
					}
				}
				var bd []byte
				if !lost {
					bd = b.features(shown, profiles)
				}
				b.add("search", SearchInfo{Nights: stay, Candidates: cands}, s.call, s.ret, status, bd)
			case 2: // a recommendation's profile
				prof := h.profile.at(s.eff - rng.Int63n(ms(p.Lag)+1))
				var bd []byte
				if !lost {
					bd = b.features([]string{id}, map[string]Profile{id: prof})
				}
				b.add("recommend_dis", RecommendInfo{Require: "dis", Expected: []string{id}}, s.call, s.ret, status, bd)
			case 3: // rates
				at := s.eff - rng.Int63n(ms(p.Lag)+1)
				prices := map[string]Prices{}
				for _, code := range []string{"A", "B"} {
					prices[code] = h.prices[code].at(at)
				}
				var bd []byte
				if !lost {
					bd = b.ratesBody(id, prices)
				}
				b.add("rates", HotelInfo{Hotel: id}, s.call, s.ret, status, bd)
			case 4: // set capacity
				rooms := cat.Cap - 1 + rng.Intn(3)
				most := 0
				for _, r := range h.rooms {
					most = max(most, r.latest())
				}
				if rooms >= most {
					h.capacity.set(s.eff, rooms)
					b.add("set_capacity", CapacityInfo{Hotel: id, Rooms: rooms}, s.call, s.ret, status, body(msgCapacity))
				} else {
					st := 409
					if lost {
						st = 0
					}
					b.add("set_capacity", CapacityInfo{Hotel: id, Rooms: rooms}, s.call, s.ret, st, nil)
				}
			case 5: // set profile
				prof := h.profile.latest()
				prof.Phone = fmt.Sprintf("p%d", seq)
				h.profile.set(s.eff, prof)
				if !p.Bugs.FlushLosesProfile {
					h.durable = prof
				}
				b.add("set_profile", ProfileInfo{Hotel: id, Phone: prof.Phone}, s.call, s.ret, status, body(msgProfile))
			case 6: // set rate
				code := []string{"A", "B"}[rng.Intn(2)]
				v := float64(50 + rng.Intn(500))
				pr := Prices{v, v, v * 1.1}
				h.prices[code].set(s.eff, pr)
				b.add("set_rate", RateInfo{Hotel: id, Code: code, Prices: [3]float64{pr.Bookable, pr.Total, pr.Inclusive}}, s.call, s.ret, status, body(msgRate))
			}
		}
		t = end + 1
		if epoch == p.Epochs-1 {
			break
		}
		kind, target := "restart_all", "app"
		if epoch%2 == 1 {
			kind, target = "flush", "memcached"
		}
		at := t + int64(time.Millisecond)
		faults = append(faults, check.Fault{Kind: kind, Target: target, Start: at, End: at + int64(p.FaultGap) - 1, AfterPhase: epoch})
		for _, h := range hotels {
			switch {
			case kind == "restart_all" && p.Bugs.LoseOnRestart,
				kind == "flush" && p.Bugs.FlushLosesReservations:
				for _, r := range h.rooms {
					if n := epochStart[r]; n > 0 {
						r.set(at, r.h[n-1].v)
					}
				}
			case kind == "flush" && p.Bugs.FlushLosesProfile:
				h.profile.set(at, h.durable)
			}
		}
		t = at + int64(p.FaultGap)
	}
	for len(b.calls) < p.Epochs {
		b.calls = append(b.calls, nil)
	}
	for _, calls := range b.calls {
		sort.SliceStable(calls, func(i, j int) bool { return calls[i].Start < calls[j].Start })
	}
	return b.calls, faults
}
