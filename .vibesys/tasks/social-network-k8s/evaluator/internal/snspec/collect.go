package snspec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"strconv"
	"strings"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"

	"vibesys/social-network-k8s-evaluator/internal/snapi"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// The contract between the accuracy workload and this oracle: a call's Info is the JSON of its
// snapi.Action (what the call sends, never an expected answer), and its one step is the HTTP
// request that action makes (snapi.Request): Status and Body are the answer.
const (
	composeSuccess = "Successfully upload post"
	writeSuccess   = "Success"
)

// acall is one call as both specs read it: its action and its answer parsed once. It lives while
// its call is projected: the projection keeps the call (without bodies) as the pieces' Origin.
type acall struct {
	origin  *check.Call
	a       snapi.Action
	unknown bool // no response or a 5xx: a write may or may not have taken effect
	status  int
	acked   bool    // a write answered 200 with its route's success text
	ro      ReadOut // a read's answer (its page parsed only when asked for)
}

// facts are what the projections must know about the whole history before they add a piece:
// who is known, who follows and mentions whom, who reads. A first pass over the phases collects
// them (ids and names, never calls or bodies), a second adds the pieces, so memory stays at one
// phase plus the facts (check.Spec.Project may range the phases more than once).
type facts struct {
	names map[int64]string // acknowledged registrations
	known map[int64]bool
	// liveAuthors compose with text; mentioners: mentioned -> authors; changedBy: follower ->
	// followees with a change call.
	liveAuthors map[int64]bool
	mentioners  map[int64]map[int64]bool
	changedBy   map[int64]map[int64]bool
	// readers have a home read answered 200 with a valid window: the reader view's partitions.
	readers map[int64]bool
}

// collect is the first pass: the facts, from every call's Info and status (no page is parsed).
func (m *Model) collect(phases iter.Seq[[]check.Call]) (*facts, error) {
	h := &facts{names: map[int64]string{}, known: map[int64]bool{}, liveAuthors: map[int64]bool{},
		mentioners: map[int64]map[int64]bool{}, changedBy: map[int64]map[int64]bool{}, readers: map[int64]bool{}}
	homeReaders := map[int64]bool{}
	for calls := range phases {
		for i := range calls {
			c := &calls[i]
			ac, err := decode(c, false)
			if err != nil {
				return nil, fmt.Errorf("%s %d of phase %d: %w", c.Op, c.Index, c.Phase, err)
			}
			a := ac.a
			switch a.Kind {
			case snapi.KindRegister:
				if ac.acked {
					h.names[a.User] = a.Name
					h.known[a.User] = true
				}
			case snapi.KindCompose:
				if a.Text != "" {
					h.liveAuthors[a.User] = true
					for _, v := range a.Mentions {
						if h.mentioners[v] == nil {
							h.mentioners[v] = map[int64]bool{}
						}
						h.mentioners[v][a.User] = true
					}
				}
			case snapi.KindFollow:
				if h.changedBy[a.User] == nil {
					h.changedBy[a.User] = map[int64]bool{}
				}
				h.changedBy[a.User][a.Followee] = true
			case snapi.KindRead:
				if user, _, _, ok := pageWindow(a); ok && a.Home && !ac.unknown && ac.status == 200 {
					homeReaders[user] = true
				}
			}
		}
	}
	for u := range homeReaders {
		if h.known[u] || m.seededUser(u) >= 0 {
			h.readers[u] = true
		}
	}
	return h, nil
}

// pageWindow is a read's user and page window, and whether they are valid: numeric and 0 <= start
// < stop.
func pageWindow(a snapi.Action) (user int64, start, stop int, ok bool) {
	user, errU := strconv.ParseInt(a.UserID, 10, 64)
	start, errS := strconv.Atoi(a.Start)
	stop, errE := strconv.Atoi(a.Stop)
	return user, start, stop, errU == nil && errS == nil && errE == nil && start >= 0 && start < stop
}

// decode reads a call: its Info strictly, its single step, and (parse) a read's page.
func decode(c *check.Call, parse bool) (acall, error) {
	ac := acall{origin: c}
	dec := json.NewDecoder(bytes.NewReader(c.Info))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ac.a); err != nil {
		return ac, fmt.Errorf("info %.200s: %w", c.Info, err)
	}
	switch ac.a.Kind {
	case snapi.KindRegister, snapi.KindFollow, snapi.KindCompose, snapi.KindRead:
	default:
		return ac, fmt.Errorf("unknown action %q", ac.a.Kind)
	}
	if len(c.Steps) > 1 {
		return ac, fmt.Errorf("%d steps, want 1", len(c.Steps))
	}
	if c.Lost() {
		ac.unknown = true
		return ac, nil
	}
	st := c.Steps[0]
	ac.status = st.Status
	ac.unknown = st.Status >= 500
	if ac.unknown {
		return ac, nil
	}
	switch ac.a.Kind {
	case snapi.KindRegister, snapi.KindFollow:
		ac.acked = st.Status == 200 && strings.Contains(string(st.Body), writeSuccess)
	case snapi.KindCompose:
		ac.acked = st.Status == 200 && strings.Contains(string(st.Body), composeSuccess)
	case snapi.KindRead:
		ac.ro = ReadOut{Status: st.Status}
		if parse && st.Status == 200 {
			page, err := timeline.Parse(st.Body)
			ac.ro.Page = page
			if err != nil {
				ac.ro.ParseErr = err.Error()
			}
		}
	}
	return ac, nil
}

// calls is the second pass: every call, decoded with its page.
func calls(phases iter.Seq[[]check.Call], each func(*acall)) error {
	for cs := range phases {
		for i := range cs {
			ac, err := decode(&cs[i], true)
			if err != nil {
				return fmt.Errorf("%s %d of phase %d: %w", cs[i].Op, cs[i].Index, cs[i].Phase, err)
			}
			each(&ac)
		}
	}
	return nil
}
