// Package hotelspec is the Hotel Reservation accuracy oracle on vibesys-evaluator's `check`
// package: the semantics of OBJECTIVE.md "Writes and their visibility" as one check.Spec over
// one hotel's state, and the projection of the accuracy workload's recorded calls onto it.
//
// Partition key: the hotel ("h:<id>"). Everything the contract says about a hotel is about that
// hotel alone: its capacity, its rooms reserved per night, its profile and its rate plans'
// prices. A search (up to 5 hotels) and a recommendation are projected into one piece per hotel
// they could return; each piece is checked in its hotel's history (that hotel's view, not a
// cross-hotel snapshot, which the contract does not promise). Logins and the hotel sets of
// recommendations depend on no state and are checked when projecting.
//
// Rules, which model the pinned application as it is:
//   - Reservations and capacity writes are strict (linearizable): a reservation is decided
//     against the rooms reserved and the capacity at its linearization point; it takes rooms on
//     every night of the stay or none. A capacity below a night's reserved rooms (seeded or
//     reserved) is 409. The accuracy workload books one-night stays only: the base gives a
//     multi-night stay's rooms back night by night when a later night is full, so a stay is not
//     atomic to concurrent observers and this model (which takes all nights or none) would
//     reject the base.
//   - Capacity writes race with reservations in the base: a capacity write is ordered with a
//     reservation only when they do not overlap. The model does not allow the race, so the
//     accuracy workload must not overlap a capacity write with a reservation of the same hotel
//     (README, "Contract with the accuracy workload").
//   - The pinned frontend reserves rooms even when the login failed: a reservation with a wrong
//     password answers the login failure and still takes the rooms (when they fit).
//   - Everything read by search, recommendations and /rates is under the 1 s bound: a read may
//     show, per item, any version not certainly overwritten in real time more than 1 s before it
//     started (strictly more than 1 s: a read exactly 1 s after a write may still be stale).
//     Profile fields and prices are check.Window items and each night's reserved rooms a
//     check.Counter (order-independent relaxations of a linearized register, so concurrent
//     writes do not multiply the search's states); capacity is a precise check.Register.
//   - An outcome that is unknown (a lost response or a 5xx) may or may not have taken effect
//     within EffectWindow: both states.
//   - Faults (restarts of the application tier, flushes of memcached) change nothing: an
//     acknowledged write is durable (MongoDB holds it; the caches are rebuilt from it), so the
//     bound keeps applying across them and Spec.Fault is nil. A reservation, capacity or profile
//     write lost across a fault therefore shows as a later answer the model cannot reach.
package hotelspec

import (
	"fmt"
	"hash/maphash"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"
)

// Bound is the visibility bound of OBJECTIVE.md: a read may show a version overwritten up to
// this long (inclusive) before it started.
const Bound = time.Second

// EffectWindow is how long after its call an unknown outcome may still take effect: loadgen's
// default step timeout, which also bounds the application's own request deadline.
const EffectWindow = 10 * time.Second

// Messages of the pinned frontend.
const (
	MsgReserved   = "Reserve successfully!"
	MsgFull       = "Failed. Already reserved. "
	MsgLoginFail  = "Failed. Please check your username and password. "
	MsgOK         = "ok"  // an acknowledged admin write (200 with its message)
	MsgConflict   = "409" // a refused capacity write
	MsgBadRequest = "400"
)

// Profile is a hotel's name and phone as GeoJSON shows them.
type Profile struct{ Name, Phone string }

// Prices of one rate plan.
type Prices struct{ Bookable, Total, Inclusive float64 }

// Plan is a seeded rate plan: what /rates shows besides its prices, and the prices.
type Plan struct {
	Code, InDate, OutDate, RoomCode, RoomDescription string
	Prices                                           Prices
}

// Hotel is a hotel's seeded data: what the model starts its state from and what the projection
// checks static fields against.
type Hotel struct {
	ID string
	// Capacity is the seeded room capacity and SeededMaxRooms the most rooms the seeded bookings
	// hold on one night (a capacity below it is refused).
	Capacity, SeededMaxRooms int
	Profile                  Profile
	// Lon and Lat are the coordinates as GeoJSON shows them (float32-encoded).
	Lon, Lat float64
	// Plans are sorted by code.
	Plans []Plan
}

// Catalog is the task's dataset as the model sees it.
type Catalog interface {
	// Hotel returns the hotel, or false when the id is not a seeded hotel.
	Hotel(id string) (*Hotel, bool)
}

// PlanPrices is one rate plan's code and prices.
type PlanPrices struct {
	Code   string
	Prices Prices
}

// Kind of a model input.
type Kind uint8

const (
	Reserve     Kind = iota + 1 // strict
	Search                      // piece: is this hotel in the answer, with which profile
	ReadProfile                 // piece of a recommendation: the profile shown
	ReadRates                   // GET /rates
	SetCapacity                 // strict against reservations
	SetProfile
	SetRate
)

func (k Kind) String() string {
	return [...]string{"?", "reserve", "search", "read-profile", "read-rates", "set-capacity", "set-profile", "set-rate"}[k]
}

// In is a model input (one piece, of one hotel).
type In struct {
	Kind      Kind
	Hotel     string
	Nights    []string // reserve, search: the stay's nights (YYYY-MM-DD), may be empty
	Rooms     int      // reserve, set-capacity
	ValidUser bool     // reserve
	Name      string   // set-profile ("" unchanged)
	Phone     string   // set-profile ("" unchanged)
	Code      string   // set-rate
	Prices    Prices   // set-rate
}

// Out is an observed output.
type Out struct {
	Msg     string       // reserve, set-*
	Present bool         // search
	Profile Profile      // search (when present), read-profile
	Rates   []PlanPrices // read-rates, sorted by code
}

// State is one hotel's state.
type State struct {
	hotel    string
	seeded   int                       // seeded max rooms on a night (capacity floor)
	capacity check.Register[int]       // strict: reservations and capacity writes are serialized per hotel
	nights   check.PMap[check.Counter] // rooms reserved by night
	name     check.Window[string]      // profile fields, under the bound
	phone    check.Window[string]
	prices   []plan // sorted by code
	hash     uint64
}

type plan struct {
	code   string
	prices check.Window[Prices]
}

func (s *State) clone() *State {
	n := *s
	n.prices = slices.Clone(s.prices)
	return &n
}

func (s *State) rooms(d string) check.Counter {
	r, _ := s.nights.Get(d)
	return r
}

func (s *State) setRooms(d string, r check.Counter) { s.nights = s.nights.Set(d, r) }

func (s *State) mayShowProfile(p Profile, call int64) bool {
	return s.name.MayShow(p.Name, call, Bound) && s.phone.MayShow(p.Phone, call, Bound)
}

func (s *State) planIndex(code string) (int, bool) {
	return sort.Find(len(s.prices), func(i int) int { return strings.Compare(code, s.prices[i].code) })
}

func (s *State) maxReserved() int {
	m := s.seeded
	s.nights.Range(func(_ string, r check.Counter) { m = max(m, r.Value()) })
	return m
}

func (s *State) prune(floor int64) {
	s.capacity = s.capacity.Prune(floor, Bound)
	s.name = s.name.Prune(floor, Bound)
	s.phone = s.phone.Prune(floor, Bound)
	s.nights = s.nights.MapRecent(func(r check.Counter) check.Counter { return r.Prune(floor, Bound) })
	for i := range s.prices {
		s.prices[i].prices = s.prices[i].prices.Prune(floor, Bound)
	}
}

var hseed = maphash.MakeSeed()

func (s *State) rehash() *State {
	var h maphash.Hash
	h.SetSeed(hseed)
	h.WriteString(s.hotel)
	write := func(x uint64) {
		var b [8]byte
		for i := range b {
			b[i] = byte(x >> (8 * i))
		}
		h.Write(b[:])
	}
	write(s.capacity.Hash())
	write(s.name.Hash())
	write(s.phone.Hash())
	write(s.nights.Hash())
	for _, p := range s.prices {
		h.WriteString(p.code)
		write(p.prices.Hash())
	}
	s.hash = h.Sum64()
	return s
}

func equal(a, b *State) bool {
	if a.hash != b.hash || a.hotel != b.hotel || len(a.prices) != len(b.prices) ||
		!a.capacity.Equal(b.capacity) || !a.name.Equal(b.name) || !a.phone.Equal(b.phone) || !a.nights.Equal(b.nights) {
		return false
	}
	for i := range a.prices {
		if a.prices[i].code != b.prices[i].code || !a.prices[i].prices.Equal(b.prices[i].prices) {
			return false
		}
	}
	return true
}

// expectReserve is the strict decision: the message a reservation must get, and whether it
// takes the rooms.
func expectReserve(s *State, in In) (string, bool) {
	c := s.capacity.Latest()
	for _, d := range in.Nights {
		if s.rooms(d).Value()+in.Rooms > c {
			return MsgFull, false
		}
	}
	if !in.ValidUser {
		return MsgLoginFail, true // the pinned frontend reserves even after a failed login
	}
	return MsgReserved, true
}

// availability says what a search that started at call may show for this hotel.
func availability(s *State, nights []string, call int64) (mayBePresent, mayBeAbsent bool) {
	for _, c := range s.capacity.Allowed(call, Bound) {
		present, absent := true, false
		for _, d := range nights {
			lo, hi := s.rooms(d).Range(call, Bound)
			if lo+1 > c {
				present = false
			}
			if hi+1 > c {
				absent = true
			}
		}
		mayBePresent = mayBePresent || present
		mayBeAbsent = mayBeAbsent || absent
	}
	return
}

// Spec builds the oracle over a catalog.
func Spec(cat Catalog) *check.Spec[*State, In, Out] {
	init := func(keys []string) *State {
		if len(keys) != 1 || !strings.HasPrefix(keys[0], "h:") {
			panic(fmt.Sprintf("hotelspec: partition keys %v, want one hotel", keys))
		}
		id := keys[0][2:]
		h, _ := cat.Hotel(id)
		if h == nil {
			h = &Hotel{ID: id}
		}
		s := &State{hotel: id, seeded: h.SeededMaxRooms, capacity: check.NewRegister(h.Capacity),
			name: check.NewWindow(h.Profile.Name), phone: check.NewWindow(h.Profile.Phone)}
		for _, p := range h.Plans {
			s.prices = append(s.prices, plan{p.Code, check.NewWindow(p.Prices)})
		}
		sort.Slice(s.prices, func(i, j int) bool { return s.prices[i].code < s.prices[j].code })
		return s.rehash()
	}
	step := func(s *State, op check.Op[In, Out]) ([]*State, string) {
		in, out := op.In, op.Out
		next := func(change func(n *State)) *State {
			n := s.clone()
			if change != nil {
				change(n)
			}
			n.prune(op.Floor)
			return n.rehash()
		}
		same := func() ([]*State, string) { return []*State{next(nil)}, "" }
		one := func(n *State) ([]*State, string) { return []*State{n}, "" }
		reject := func() ([]*State, string) { return nil, explain(s, op) }
		switch in.Kind {
		case Reserve:
			msg, takes := expectReserve(s, in)
			applied := func() *State {
				return next(func(n *State) {
					for _, d := range in.Nights {
						n.setRooms(d, n.rooms(d).Add(in.Rooms, op.Call, op.Return))
					}
				})
			}
			if op.Unknown {
				if takes && len(in.Nights) > 0 && in.Rooms > 0 {
					return []*State{next(nil), applied()}, ""
				}
				return same()
			}
			if out.Msg != msg {
				return reject()
			}
			if takes && len(in.Nights) > 0 && in.Rooms > 0 {
				return one(applied())
			}
			return same()
		case SetCapacity:
			ok := in.Rooms >= s.maxReserved()
			applied := func() *State { return next(func(n *State) { n.capacity = n.capacity.Write(in.Rooms, op.Return) }) }
			if op.Unknown {
				if ok {
					return []*State{next(nil), applied()}, ""
				}
				return same()
			}
			if (out.Msg == MsgOK) != ok || (out.Msg != MsgOK && out.Msg != MsgConflict) {
				return reject()
			}
			if ok {
				return one(applied())
			}
			return same()
		case SetProfile:
			applied := func() *State {
				return next(func(n *State) {
					if in.Name != "" {
						n.name = n.name.Write(in.Name, op.Call, op.Return)
					}
					if in.Phone != "" {
						n.phone = n.phone.Write(in.Phone, op.Call, op.Return)
					}
				})
			}
			if op.Unknown {
				return []*State{next(nil), applied()}, ""
			}
			if out.Msg != MsgOK {
				return reject()
			}
			return one(applied())
		case SetRate:
			i, known := s.planIndex(in.Code)
			if !known {
				if op.Unknown || out.Msg != MsgOK {
					return same()
				}
				return reject()
			}
			applied := func() *State {
				return next(func(n *State) { n.prices[i].prices = n.prices[i].prices.Write(in.Prices, op.Call, op.Return) })
			}
			if op.Unknown {
				return []*State{next(nil), applied()}, ""
			}
			if out.Msg != MsgOK {
				return reject()
			}
			return one(applied())
		case Search:
			if op.Unknown {
				return same()
			}
			present, absent := availability(s, in.Nights, op.Call)
			if out.Present && (!present || !s.mayShowProfile(out.Profile, op.Call)) {
				return reject()
			}
			if !out.Present && !absent {
				return reject()
			}
			return same()
		case ReadProfile:
			if op.Unknown || s.mayShowProfile(out.Profile, op.Call) {
				return same()
			}
			return reject()
		case ReadRates:
			if op.Unknown {
				return same()
			}
			if len(out.Rates) != len(s.prices) {
				return reject()
			}
			for i, p := range out.Rates {
				if p.Code != s.prices[i].code || !s.prices[i].prices.MayShow(p.Prices, op.Call, Bound) {
					return reject()
				}
			}
			return same()
		}
		panic(fmt.Sprintf("hotelspec: unknown input kind %d", in.Kind))
	}
	return &check.Spec[*State, In, Out]{
		Name:          "hotel",
		Project:       project(cat),
		Init:          init,
		Step:          step,
		Fault:         nil, // acknowledged state survives restarts and flushes
		Equal:         equal,
		Hash:          func(s *State) uint64 { return s.hash },
		EffectWindow:  EffectWindow,
		DescribeState: describeState,
		DescribeOp:    describeOp,
	}
}

func describeState(s *State) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "hotel %s: capacity %d", s.hotel, s.capacity.Latest())
	if s.capacity.Len() > 1 {
		fmt.Fprintf(&sb, " (%d versions in the 1 s window)", s.capacity.Len())
	}
	if s.nights.Len() > 0 {
		sb.WriteString(", reserved")
		n := 0
		s.nights.Range(func(d string, r check.Counter) {
			if n++; n <= 12 {
				fmt.Fprintf(&sb, " %s=%d", d, r.Value())
			}
		})
		if n > 12 {
			fmt.Fprintf(&sb, " ... (%d nights)", n)
		}
	}
	fmt.Fprintf(&sb, ", names %q, phones %q (values a read may still show)", s.name.Allowed(math.MinInt64, 0), s.phone.Allowed(math.MinInt64, 0))
	return sb.String()
}

func describeOp(op check.Op[In, Out]) string {
	in, out := op.In, op.Out
	res := ""
	switch in.Kind {
	case Reserve:
		res = fmt.Sprintf("reserve %s %v x%d valid=%v", in.Hotel, in.Nights, in.Rooms, in.ValidUser)
		if !op.Unknown {
			res += fmt.Sprintf(" -> %q", out.Msg)
		}
	case SetCapacity:
		res = fmt.Sprintf("set-capacity %s %d", in.Hotel, in.Rooms)
		if !op.Unknown {
			res += " -> " + out.Msg
		}
	case SetProfile:
		res = fmt.Sprintf("set-profile %s name=%q phone=%q", in.Hotel, in.Name, in.Phone)
		if !op.Unknown {
			res += " -> " + out.Msg
		}
	case SetRate:
		res = fmt.Sprintf("set-rate %s %s %v", in.Hotel, in.Code, in.Prices)
		if !op.Unknown {
			res += " -> " + out.Msg
		}
	case Search:
		res = fmt.Sprintf("search %v sees %s", in.Nights, in.Hotel)
		if !op.Unknown {
			if out.Present {
				res += fmt.Sprintf(": present %q/%q", out.Profile.Name, out.Profile.Phone)
			} else {
				res += ": absent"
			}
		}
	case ReadProfile:
		res = fmt.Sprintf("recommendation shows %s %q/%q", in.Hotel, out.Profile.Name, out.Profile.Phone)
	case ReadRates:
		res = fmt.Sprintf("rates %s -> %v", in.Hotel, out.Rates)
	}
	if op.Unknown {
		res += " -> ? (outcome unknown)"
	}
	return res
}

func explain(s *State, op check.Op[In, Out]) string {
	in, out := op.In, op.Out
	switch in.Kind {
	case Reserve:
		msg, _ := expectReserve(s, in)
		var nights []string
		for _, d := range in.Nights {
			nights = append(nights, fmt.Sprintf("%s has %d of %d reserved", d, s.rooms(d).Value(), s.capacity.Latest()))
		}
		return fmt.Sprintf("reservation of %d rooms: %s, so it must answer %q, got %q", in.Rooms, strings.Join(nights, ", "), msg, out.Msg)
	case SetCapacity:
		return fmt.Sprintf("capacity %d vs %d rooms reserved on its fullest night: must answer %s, got %s", in.Rooms, s.maxReserved(), map[bool]string{true: MsgOK, false: MsgConflict}[in.Rooms >= s.maxReserved()], out.Msg)
	case Search:
		present, absent := availability(s, in.Nights, op.Call)
		var nights []string
		for _, d := range in.Nights {
			lo, hi := s.rooms(d).Range(op.Call, Bound)
			nights = append(nights, fmt.Sprintf("%s reserved %d..%d", d, lo, hi))
		}
		if out.Present && !present {
			return fmt.Sprintf("search shows hotel %s, but with capacity in %v and %s no allowed state has a room on every night", in.Hotel, s.capacity.Allowed(op.Call, Bound), strings.Join(nights, ", "))
		}
		if out.Present {
			return fmt.Sprintf("search shows hotel %s with profile %q/%q; allowed names %q, phones %q", in.Hotel, out.Profile.Name, out.Profile.Phone, s.name.Allowed(op.Call, Bound), s.phone.Allowed(op.Call, Bound))
		}
		if !absent {
			return fmt.Sprintf("search omits hotel %s, but with capacity in %v and %s every allowed state has a room on every night", in.Hotel, s.capacity.Allowed(op.Call, Bound), strings.Join(nights, ", "))
		}
	case ReadProfile:
		return fmt.Sprintf("shows %q/%q; allowed names %q, phones %q", out.Profile.Name, out.Profile.Phone, s.name.Allowed(op.Call, Bound), s.phone.Allowed(op.Call, Bound))
	case ReadRates:
		var parts []string
		for _, p := range s.prices {
			parts = append(parts, fmt.Sprintf("%s in %v", p.code, p.prices.Allowed(op.Call, Bound)))
		}
		return fmt.Sprintf("rates %v; allowed %s", out.Rates, strings.Join(parts, ", "))
	}
	return describeOp(op) + " cannot follow the state"
}
