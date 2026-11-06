package snspec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"strings"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"

	"vibesys/social-network-k8s-evaluator/internal/oracle"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// The contract between the accuracy workload and this oracle: a call's Info is the JSON of its
// oracle.Action (what the call sends, never an expected answer), and its one step is the HTTP
// request that action makes (oracle.Request): Status and Body are the answer.
const (
	composeSuccess = "Successfully upload post"
	writeSuccess   = "Success"
)

// acall is one call, as both specs read it: its action, its answer parsed once, and the call
// without its response bodies for reports. Calls are released with their phase, so only this
// stays in memory.
type acall struct {
	origin  *check.Call
	a       oracle.Action
	unknown bool // no response or a 5xx: a write may or may not have taken effect
	status  int
	acked   bool    // a write answered 200 with its route's success text
	ro      ReadOut // a read's answer
}

// history is every call and the facts about the whole history that the projections resolve
// before they add pieces.
type history struct {
	calls []acall
	names map[int64]string // acknowledged registrations
	known map[int64]bool
	// liveAuthors compose with text; mentioners: mentioned -> authors; changedBy: follower ->
	// followees with a change call.
	liveAuthors map[int64]bool
	mentioners  map[int64]map[int64]bool
	changedBy   map[int64]map[int64]bool
}

// collect reads the phases once (a phase's bodies are released when the next is read).
func collect(phases iter.Seq[[]check.Call]) (*history, error) {
	h := &history{names: map[int64]string{}, known: map[int64]bool{}, liveAuthors: map[int64]bool{},
		mentioners: map[int64]map[int64]bool{}, changedBy: map[int64]map[int64]bool{}}
	for calls := range phases {
		for i := range calls {
			c := &calls[i]
			ac, err := decode(c)
			if err != nil {
				return nil, fmt.Errorf("%s %d of phase %d: %w", c.Op, c.Index, c.Phase, err)
			}
			a := ac.a
			switch a.Kind {
			case oracle.KindRegister:
				if ac.acked {
					h.names[a.User] = a.Name
					h.known[a.User] = true
				}
			case oracle.KindCompose:
				if a.Text != "" {
					h.liveAuthors[a.User] = true
					for _, v := range a.Mentions {
						if h.mentioners[v] == nil {
							h.mentioners[v] = map[int64]bool{}
						}
						h.mentioners[v][a.User] = true
					}
				}
			case oracle.KindFollow:
				if h.changedBy[a.User] == nil {
					h.changedBy[a.User] = map[int64]bool{}
				}
				h.changedBy[a.User][a.Followee] = true
			}
			h.calls = append(h.calls, ac)
		}
	}
	return h, nil
}

// decode reads a call: its Info strictly, its single step, and a read's page.
func decode(c *check.Call) (acall, error) {
	var ac acall
	dec := json.NewDecoder(bytes.NewReader(c.Info))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ac.a); err != nil {
		return ac, fmt.Errorf("info %.200s: %w", c.Info, err)
	}
	switch ac.a.Kind {
	case oracle.KindRegister, oracle.KindFollow, oracle.KindCompose, oracle.KindRead:
	default:
		return ac, fmt.Errorf("unknown action %q", ac.a.Kind)
	}
	if len(c.Steps) > 1 {
		return ac, fmt.Errorf("%d steps, want 1", len(c.Steps))
	}
	o := *c
	o.Steps = nil
	for _, s := range c.Steps {
		s.Body = nil
		o.Steps = append(o.Steps, s)
	}
	ac.origin = &o
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
	case oracle.KindRegister, oracle.KindFollow:
		ac.acked = st.Status == 200 && strings.Contains(string(st.Body), writeSuccess)
	case oracle.KindCompose:
		ac.acked = st.Status == 200 && strings.Contains(string(st.Body), composeSuccess)
	case oracle.KindRead:
		ac.ro = ReadOut{Status: st.Status}
		if st.Status == 200 {
			page, err := timeline.Parse(st.Body)
			ac.ro.Page = page
			if err != nil {
				ac.ro.ParseErr = err.Error()
			}
		}
	}
	return ac, nil
}
