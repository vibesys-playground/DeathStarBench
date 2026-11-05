package oracle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/fakeapp"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

func registered(t *testing.T, m *Model, names ...string) *State {
	t.Helper()
	s := NewState()
	for i, name := range names {
		apply(t, m, s, writeOK(Register(3_000_000_001+int64(i), name), 0, 1))
	}
	return s
}

// TestVerifierOrdersParallelCalls: a parallel group is accepted exactly when some ordering
// that keeps real-time order admits every answer.
func TestVerifierOrdersParallelCalls(t *testing.T) {
	m := NewModel(nil, time.Second)
	a := post(3_000_000_001, "alice", "m1")
	p := served(a, 501, 15)
	cases := []struct {
		name  string
		calls []Call
		want  string // "" accepted
	}{
		{"a read overlapping the compose shows it", []Call{composeOK(a, 10, 20), read(false, a.User, 0, 10, 12, 18, p)}, ""},
		{"a read overlapping the compose misses it", []Call{composeOK(a, 10, 20), read(false, a.User, 0, 10, 12, 18)}, ""},
		{"a read that ended before the compose was sent shows it", []Call{read(false, a.User, 0, 10, 2, 8, p), composeOK(a, 10, 20)}, "never composed"},
		{"a read after the compose was acknowledged misses it", []Call{composeOK(a, 10, 20), read(false, a.User, 0, 10, 21, 25)}, "empty"},
		{"two reads disagree on the post's identity", []Call{
			composeOK(a, 10, 20), read(false, a.User, 0, 10, 12, 18, p), read(false, a.User, 0, 10, 12, 18, served(a, 502, 15)),
		}, "first served as post"},
		{"one read shows the post, a concurrent one misses it", []Call{
			composeOK(a, 10, 20), read(false, a.User, 0, 10, 12, 18, p), read(false, a.User, 0, 10, 12, 19),
		}, ""},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			v := NewVerifier(m, registered(t, m, "alice"))
			err := v.Advance(Event{Calls: test.calls, Parallel: true})
			if test.want == "" && err != nil {
				t.Fatalf("rejected: %v", err)
			}
			if test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("got %v, want an error with %q", err, test.want)
			}
		})
	}
	// Real-time order: a read that answered before a later group of the program is ordered
	// before it.
	v := NewVerifier(m, registered(t, m, "alice"))
	if err := v.Advance(Event{Calls: []Call{composeOK(a, 10, 20)}}); err != nil {
		t.Fatal(err)
	}
	if err := v.Advance(Event{Calls: []Call{read(false, a.User, 0, 10, 30, 31)}}); err == nil {
		t.Fatal("a read after the acknowledgement missed the post")
	}
	big := make([]Call, MaxParallel+1)
	for i := range big {
		big[i] = read(false, a.User, 0, 10, 30, 31)
	}
	if err := v.Advance(Event{Calls: big, Parallel: true}); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("a group of %d: %v", len(big), err)
	}
}

// TestVerifierFollowInParallel: a follow concurrent with a compose may or may not deliver it.
func TestVerifierFollowInParallel(t *testing.T) {
	m := NewModel(nil, time.Second)
	a := post(3_000_000_002, "bob", "m1")
	p := served(a, 501, 15)
	follow := writeOK(Follow(3_000_000_001, 3_000_000_002, true), 10, 20)
	for _, page := range [][]timeline.Post{nil, {p}} {
		v := NewVerifier(m, registered(t, m, "alice", "bob"))
		group := []Call{follow, composeOK(a, 11, 19), read(true, 3_000_000_001, 0, 10, 12, 30, page...)}
		if err := v.Advance(Event{Calls: group, Parallel: true}); err != nil {
			t.Fatalf("page %v: %v", page, err)
		}
		// Long after: still optional (composed while the edge changed).
		if err := v.Advance(Event{Calls: []Call{read(true, 3_000_000_001, 0, 10, 5000, 5001, page...)}}); err != nil {
			t.Fatalf("page %v later: %v", page, err)
		}
	}
	// A post the follower never could see: composed after the follow settled, by another user.
	v := NewVerifier(m, registered(t, m, "alice", "bob", "carol"))
	c := post(3_000_000_003, "carol", "m2")
	group := []Call{follow, composeOK(c, 11, 19), read(true, 3_000_000_001, 0, 10, 12, 30, served(c, 502, 15))}
	if err := v.Advance(Event{Calls: group, Parallel: true}); err == nil {
		t.Fatal("a post of an unfollowed author was accepted in a home timeline")
	}
}

// randomProgram is a program of random calls over a few users of its own, with parallel
// groups, settles, restarts and flushes.
func randomProgram(r *rand.Rand, round int) Program {
	base := int64(3_100_000_000) + int64(round)*100
	users := 2 + r.Intn(4)
	var steps []Step
	call := func(a Action) { steps = append(steps, Step{Call: &a}) }
	for i := 0; i < users; i++ {
		call(Register(base+int64(i), fmt.Sprintf("r%du%d", round, i)))
	}
	posts := 0
	pick := func() int64 { return base + int64(r.Intn(users)) }
	randomAction := func() Action {
		switch x := r.Intn(10); {
		case x < 2:
			u, v := pick(), pick()
			for v == u {
				v = pick()
			}
			return Follow(u, v, r.Intn(3) > 0)
		case x < 5:
			posts++
			author := pick()
			marker := fmt.Sprintf("mr%dp%d", round, posts)
			a := Action{Kind: KindCompose, User: author, Name: fmt.Sprintf("r%du%d", round, author-base), Marker: marker}
			words := []string{"walk", "tree", "#" + marker}
			if r.Intn(3) == 0 {
				m := pick()
				a.Mentions = []int64{m}
				words = append(words, fmt.Sprintf("@r%du%d", round, m-base))
			}
			if r.Intn(4) == 0 {
				a.URL = fmt.Sprintf("http://example%d.com", r.Intn(1000))
				words = append(words, a.URL)
			}
			a.Text = strings.Join(words, " ")
			return a
		default:
			start := r.Intn(3) * 2
			return Read(r.Intn(2) == 0, pick(), start, start+2+r.Intn(10))
		}
	}
	for len(steps) < 60 {
		switch x := r.Intn(20); {
		case x < 3:
			group := make([]Action, 2+r.Intn(MaxParallel-1))
			for i := range group {
				group[i] = randomAction()
			}
			steps = append(steps, Step{Parallel: group})
		case x < 5:
			steps = append(steps, Step{Settle: true})
		case x == 5:
			steps = append(steps, Step{Restart: "nginx-thrift"})
		case x == 6:
			steps = append(steps, Step{Flush: "social-graph-redis"})
		default:
			call(randomAction())
		}
	}
	// Settle, then read every timeline whole.
	steps = append(steps, Step{Settle: true})
	for i := 0; i < users; i++ {
		call(Read(false, base+int64(i), 0, 50))
		call(Read(true, base+int64(i), 0, 50))
	}
	return Program{ID: fmt.Sprintf("random-%d", round), Steps: steps}
}

// verifyAll replays events from a fresh state: nil when some ordering explains them all.
func verifyAll(m *Model, events []Event) error {
	v := NewVerifier(m, nil)
	for i, event := range events {
		if err := v.Advance(event); err != nil {
			return fmt.Errorf("event %d: %w", i, err)
		}
	}
	return nil
}

type mutation struct {
	name string
	// apply changes one read's page (posts as served) and says whether it applied.
	apply func(posts []timeline.Post, c Call, events []Event, index int) ([]timeline.Post, bool)
}

var mutations = []mutation{
	{"swap two posts of different timestamps", func(posts []timeline.Post, _ Call, _ []Event, _ int) ([]timeline.Post, bool) {
		for i := 1; i < len(posts); i++ {
			if posts[i].Timestamp != posts[i-1].Timestamp {
				posts[i], posts[i-1] = posts[i-1], posts[i]
				return posts, true
			}
		}
		return nil, false
	}},
	{"duplicate a post", func(posts []timeline.Post, _ Call, _ []Event, _ int) ([]timeline.Post, bool) {
		if len(posts) < 2 {
			return nil, false
		}
		posts[1] = posts[0]
		return posts, true
	}},
	{"change a post's text", func(posts []timeline.Post, _ Call, _ []Event, _ int) ([]timeline.Post, bool) {
		if len(posts) == 0 {
			return nil, false
		}
		posts[0].Text = strings.Replace(posts[0].Text, "walk", "talk", 1)
		return posts, posts[0].Text != strings.Replace(posts[0].Text, "talk", "walk", 1)
	}},
	{"serve a post already seen under another id", func(posts []timeline.Post, c Call, events []Event, index int) ([]timeline.Post, bool) {
		if len(posts) == 0 {
			return nil, false
		}
		marker := MarkerOf(posts[0].Text)
		posts[0].PostID = "4" + posts[0].PostID
		return posts, servedBefore(events, index, marker, c.Start)
	}},
	{"drop a required post", func(posts []timeline.Post, c Call, events []Event, index int) ([]timeline.Post, bool) {
		// The newest post of an author's user timeline read from its start, composed and
		// already served before this read started.
		if c.Action.Home || c.Action.Start != "0" || len(posts) == 0 {
			return nil, false
		}
		marker := MarkerOf(posts[0].Text)
		acked := false
		for _, event := range events[:index] {
			for _, other := range event.Calls {
				if other.Action.Kind == KindCompose && other.Action.Marker == marker && other.End.Before(c.Start) {
					acked = true
				}
			}
		}
		seen := servedBefore(events, index, marker, c.Start)
		return posts[1:], acked && seen
	}},
}

// TestRandomHistories: executions of random programs on a correct application (the fake,
// with asynchronous fan-out within the bound) are accepted, and each mutation of one answer
// that the contract forbids is rejected.
func TestRandomHistories(t *testing.T) {
	m := NewModel(nil, 150*time.Millisecond)
	rounds := 12
	if testing.Short() {
		rounds = 3
	}
	applied := map[string]int{}
	for round := 0; round < rounds; round++ {
		r := rand.New(rand.NewSource(int64(round)))
		app := fakeapp.New(nil, fakeapp.Options{FanoutDelay: time.Duration(r.Intn(100)) * time.Millisecond})
		program := randomProgram(r, round)
		events, _, err := m.Run(context.Background(), program, nil, &handlerSystem{app: app})
		if err != nil {
			var mismatch *Mismatch
			if errors.As(err, &mismatch) {
				t.Fatalf("round %d: a correct execution was rejected: %v\ntrace %s", round, err, mismatch.Trace())
			}
			t.Fatal(err)
		}
		if err := verifyAll(m, events); err != nil {
			t.Fatalf("round %d: replay rejected: %v", round, err)
		}
		for _, mut := range mutations {
			tried := 0
			for index := len(events) - 1; index >= 0 && tried < 12; index-- {
				mutated, ok := mutate(events, index, mut)
				if !ok {
					continue
				}
				applied[mut.name]++
				tried++
				if err := verifyAll(m, mutated); err == nil {
					encoded, _ := json.Marshal(mutated[index])
					t.Fatalf("round %d: %s in event %d was accepted: %s", round, mut.name, index, encoded)
				}
			}
		}
	}
	for _, mut := range mutations {
		if applied[mut.name] == 0 {
			t.Errorf("mutation %q never applied", mut.name)
		}
		t.Logf("%s: %d mutated histories, all rejected", mut.name, applied[mut.name])
	}
}

// servedBefore: a read of the events before index, answered before start, served the post.
func servedBefore(events []Event, index int, marker string, start time.Time) bool {
	for _, event := range events[:index] {
		for _, other := range event.Calls {
			body := string(other.Body)
			if other.Action.Kind == KindRead && other.End.Before(start) &&
				(strings.Contains(body, "#"+marker+`"`) || strings.Contains(body, "#"+marker+" ")) {
				return true
			}
		}
	}
	return false
}

// mutate applies a mutation to the first read of events[index] it fits, on a copy.
func mutate(events []Event, index int, mut mutation) ([]Event, bool) {
	for ci, c := range events[index].Calls {
		if c.Action.Kind != KindRead || c.Status != 200 {
			continue
		}
		posts, err := timeline.Parse(c.Body)
		if err != nil {
			continue
		}
		changed, ok := mut.apply(posts, c, events, index)
		if !ok {
			continue
		}
		out := append([]Event(nil), events...)
		event := out[index]
		event.Calls = append([]Call(nil), event.Calls...)
		event.Calls[ci].Body = wire(changed...)
		out[index] = event
		return out, true
	}
	return nil, false
}

// TestRunReportsTheFirstMismatch: a faulty answer ends the program at its step.
func TestRunReportsTheFirstMismatch(t *testing.T) {
	m := NewModel(nil, time.Second)
	app := fakeapp.New(nil, fakeapp.Options{})
	a := post(3_000_000_001, "alice", "m1")
	program := Program{ID: "p", Steps: []Step{
		{Call: &Action{Kind: KindRegister, User: 3_000_000_001, Name: "alice"}},
		{Call: &a},
		{Call: ptr(Read(false, a.User, 0, 10))},
		{Call: ptr(Read(false, a.User, 0, 10))},
	}}
	calls := 0
	faulty := &handlerSystem{app: app, mutate: func(c *Call) {
		calls++
		if calls == 4 {
			c.Body = []byte("{}") // the post vanished
		}
	}}
	_, _, err := m.Run(context.Background(), program, nil, faulty)
	var mismatch *Mismatch
	if !errors.As(err, &mismatch) || mismatch.Step != 3 || !strings.Contains(mismatch.Reason, "empty") {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(mismatch.Trace(), `"kind":"read"`) {
		t.Fatalf("trace %s", mismatch.Trace())
	}
}

func ptr[T any](v T) *T { return &v }
