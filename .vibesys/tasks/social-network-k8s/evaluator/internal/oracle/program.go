package oracle

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// MaxParallel bounds a parallel group: the verifier searches the orderings of its calls.
const MaxParallel = 8

// settleMargin is added to the bound when a program waits for its writes to settle.
const settleMargin = 20 * time.Millisecond

// Program is a replayable sequence of events. It holds stimuli only, never expected answers:
// the Model decides whether an execution is correct.
type Program struct {
	ID    string `json:"id"`
	Steps []Step `json:"steps"`
}

// Step is one event of a program; exactly one field is set.
//
//   - Call: one call, after the previous step completed.
//   - Parallel: calls sent at once (at most MaxParallel), each on its own connection.
//   - Restart: one pod of a stateless deployment is deleted; the step ends when the
//     deployment has all its replicas again and the API answers.
//   - Flush: a cache tier is emptied in place.
//   - Settle: wait until the bound (plus a small margin) has passed since the last answer, so
//     that every write before it must be visible to every read after it.
//
// Restarts and flushes happen between steps, never during a call.
type Step struct {
	Call     *Action  `json:"call,omitempty"`
	Parallel []Action `json:"parallel,omitempty"`
	Restart  string   `json:"restart,omitempty"`
	Flush    string   `json:"flush,omitempty"`
	Settle   bool     `json:"settle,omitempty"`
}

func (s Step) validate() error {
	set := 0
	for _, on := range []bool{s.Call != nil, len(s.Parallel) > 0, s.Restart != "", s.Flush != "", s.Settle} {
		if on {
			set++
		}
	}
	if set != 1 {
		return fmt.Errorf("a step sets exactly one event, this one %d", set)
	}
	if len(s.Parallel) > MaxParallel {
		return fmt.Errorf("a parallel group of %d calls (at most %d)", len(s.Parallel), MaxParallel)
	}
	return nil
}

// Event is a step as executed: its calls with their times and answers, or the lifecycle
// event and its error.
type Event struct {
	Calls    []Call `json:"calls,omitempty"`
	Parallel bool   `json:"parallel,omitempty"`
	Restart  string `json:"restart,omitempty"`
	Flush    string `json:"flush,omitempty"`
	Settle   bool   `json:"settle,omitempty"`
	Error    string `json:"error,omitempty"`
}

// System is the application under test.
type System interface {
	// Invoke sends one action and records its times and answer.
	Invoke(ctx context.Context, action Action) Call
	// Restart deletes one pod of a stateless deployment and returns when the deployment has
	// all its replicas again and the API answers.
	Restart(ctx context.Context, deployment string) error
	// Flush empties a cache tier in place.
	Flush(ctx context.Context, cache string) error
}

// Mismatch is a failed verification: the shortest prefix of an execution that no ordering
// of the model explains.
type Mismatch struct {
	Program string  `json:"program"`
	Step    int     `json:"step"`
	Events  []Event `json:"-"`
	Reason  string  `json:"reason"`
}

func (e *Mismatch) Error() string { return fmt.Sprintf("step %d: %s", e.Step, e.Reason) }

// Trace is the execution's events, as JSON (for artifacts).
func (e *Mismatch) Trace() string {
	encoded, err := json.Marshal(e.Events)
	if err != nil {
		return err.Error()
	}
	return string(encoded)
}

// Verifier holds every state the model reaches by some legal ordering of the events so far.
// An execution is correct exactly while at least one remains.
type Verifier struct {
	model  *Model
	states []*State
	last   time.Time // the latest answer of a call run through Run
}

// NewVerifier starts from initial (nil: NewState()).
func NewVerifier(model *Model, initial *State) *Verifier {
	if initial == nil {
		initial = NewState()
	}
	return &Verifier{model: model, states: []*State{initial}}
}

// States are the states reached so far (one per distinct state).
func (v *Verifier) States() []*State { return v.states }

// Advance applies one event to every state reached so far. A single call advances each state
// by Step. A parallel group explores every ordering of its calls that keeps real-time order (a
// call that answered before another was sent comes first) and keeps the states of the orderings
// whose every answer Step admits. A restart or flush maps each state by AfterRestart or
// AfterFlush. It returns why no ordering explains the event, leaving the states as they were.
func (v *Verifier) Advance(event Event) error {
	if event.Error != "" {
		return fmt.Errorf("%s", event.Error)
	}
	var next []*State
	var reason error
	switch {
	case event.Restart != "":
		for _, s := range v.states {
			next = append(next, v.model.AfterRestart(s, event.Restart))
		}
	case event.Flush != "":
		for _, s := range v.states {
			next = append(next, v.model.AfterFlush(s, event.Flush))
		}
	case event.Settle:
		next = v.states
	case len(event.Calls) > MaxParallel:
		return fmt.Errorf("a parallel group of %d calls (at most %d)", len(event.Calls), MaxParallel)
	default:
		next, reason = v.orderings(event.Calls)
	}
	if len(next) == 0 {
		if reason == nil {
			reason = fmt.Errorf("no state")
		}
		return reason
	}
	v.states = dedupe(next)
	return nil
}

// orderings is the search behind Advance for calls (one call, or a parallel group): depth
// first over the calls not yet ordered whose real-time predecessors are, memoized on the
// calls used and the state reached, so orderings that reach the same state are explored once.
func (v *Verifier) orderings(calls []Call) ([]*State, error) {
	var out []*State
	var deepest error
	deepestDepth := -1
	seen := map[string]bool{}
	used := make([]bool, len(calls))
	parsed := make([]*page, len(calls))
	for i := range calls {
		parsed[i] = parse(calls[i])
	}
	var explore func(s *State, depth int)
	explore = func(s *State, depth int) {
		if depth == len(calls) {
			out = append(out, s)
			return
		}
		key := fmt.Sprint(used) + s.Fingerprint()
		if seen[key] {
			return
		}
		seen[key] = true
		for i := range calls {
			if used[i] || hasUnorderedPredecessor(calls, used, i) {
				continue
			}
			next, err := v.model.step(s, calls[i], parsed[i])
			if err != nil {
				if depth > deepestDepth {
					deepest, deepestDepth = err, depth
				}
				continue
			}
			used[i] = true
			explore(next, depth+1)
			used[i] = false
		}
	}
	for _, s := range v.states {
		explore(s, 0)
	}
	if len(out) == 0 && len(calls) > 1 {
		deepest = fmt.Errorf("no ordering of the %d parallel calls explains their answers (after %d of them: %w)", len(calls), deepestDepth, deepest)
	}
	return out, deepest
}

// hasUnorderedPredecessor: some call not yet ordered answered before call i was sent.
func hasUnorderedPredecessor(calls []Call, used []bool, i int) bool {
	for j := range calls {
		if j != i && !used[j] && calls[j].End.Before(calls[i].Start) {
			return true
		}
	}
	return false
}

func dedupe(states []*State) []*State {
	if len(states) < 2 {
		return states
	}
	seen := map[string]bool{}
	var out []*State
	for _, s := range states {
		key := s.Fingerprint()
		if !seen[key] {
			seen[key] = true
			out = append(out, s)
		}
	}
	return out
}

// Run executes a program against the system and verifies each step as soon as it completed,
// from initial (nil: a fresh state). It returns the events and the states reached, or a
// *Mismatch at the first step no ordering explains; any other error is the context's.
func (m *Model) Run(ctx context.Context, program Program, initial *State, system System) ([]Event, []*State, error) {
	verifier := NewVerifier(m, initial)
	events, err := verifier.Run(ctx, program, system)
	return events, verifier.States(), err
}

// Run executes a program against the system from the states reached so far, advancing them
// by each step as soon as it completed. It stops at the first step no ordering explains (a
// *Mismatch; the states stay those before it) or when the context ends.
func (v *Verifier) Run(ctx context.Context, program Program, system System) ([]Event, error) {
	var events []Event
	for index, step := range program.Steps {
		if err := step.validate(); err != nil {
			return events, fmt.Errorf("program %s step %d: %w", program.ID, index, err)
		}
		if err := ctx.Err(); err != nil {
			return events, err
		}
		event := execute(ctx, step, system, v.last.Add(v.model.Bound+settleMargin))
		if err := ctx.Err(); err != nil {
			return events, err
		}
		for _, c := range event.Calls {
			if c.End.After(v.last) {
				v.last = c.End
			}
		}
		events = append(events, event)
		if err := v.Advance(event); err != nil {
			return events, &Mismatch{Program: program.ID, Step: index, Events: events, Reason: err.Error()}
		}
	}
	return events, nil
}

// execute runs one step. settleUntil is when a Settle step may end.
func execute(ctx context.Context, step Step, system System, settleUntil time.Time) Event {
	switch {
	case step.Call != nil:
		return Event{Calls: []Call{system.Invoke(ctx, *step.Call)}}
	case len(step.Parallel) > 0:
		calls := make([]Call, len(step.Parallel))
		ready := make(chan struct{})
		var wait sync.WaitGroup
		for i := range step.Parallel {
			wait.Add(1)
			go func(i int) {
				defer wait.Done()
				<-ready
				calls[i] = system.Invoke(ctx, step.Parallel[i])
			}(i)
		}
		close(ready)
		wait.Wait()
		return Event{Calls: calls, Parallel: true}
	case step.Restart != "":
		event := Event{Restart: step.Restart}
		if err := system.Restart(ctx, step.Restart); err != nil {
			event.Error = fmt.Sprintf("restart %s: %v", step.Restart, err)
		}
		return event
	case step.Flush != "":
		event := Event{Flush: step.Flush}
		if err := system.Flush(ctx, step.Flush); err != nil {
			event.Error = fmt.Sprintf("flush %s: %v", step.Flush, err)
		}
		return event
	}
	timer := time.NewTimer(time.Until(settleUntil))
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	return Event{Settle: true}
}
