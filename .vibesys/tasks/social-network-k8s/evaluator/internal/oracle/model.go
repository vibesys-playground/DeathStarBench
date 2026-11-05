// Package oracle is the task's model of the Social Network's public API: an executable
// state machine that says, for every call, which answers are allowed and what state each
// leads to. It is the one definition of the correctness contract of OBJECTIVE.md ("Correctness
// bar"); the accuracy check (cmd/checker) and the benchmark's checks (internal/snworkload)
// both judge answers with it.
//
// State is what the calls so far have written (registered users, follow changes, composed
// posts), each write with the times it was sent and answered, on top of the seeded dataset.
// Step(state, call) applies one call with its answer: a write's answer must acknowledge it and
// the write joins the state; a timeline read's answer must lie between what must be visible
// and what may be visible (bounded staleness, read.go), and the posts it shows for the first
// time are learned (their ids and timestamps are the application's to choose). A program
// (program.go) is calls, parallel groups of calls, restarts, cache flushes and waits; the
// verifier accepts an execution of one exactly when some ordering of its calls, consistent
// with their real-time order, is a run of Step that admits every answer.
package oracle

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// Answers of the pinned application's write routes (nginx's Lua scripts).
const (
	composeSuccess = "Successfully upload post"
	writeSuccess   = "Success"
)

// DefaultSlack bounds the difference between the application's clock and the client's when a
// live post's timestamp is checked against the time it was composed.
const DefaultSlack = 5 * time.Second

// Model is the contract: the seeded data and the bounds. It holds no state of its own, so one
// Model judges any number of States.
type Model struct {
	// Data is the seeded dataset (nil: an empty data tier).
	Data *dataset.Dataset
	// Bound is B: home timelines and follow changes are visible to reads that start more than
	// B after the write was acknowledged.
	Bound time.Duration
	// Slack is the clock difference allowed in a live post's timestamp.
	Slack time.Duration

	seededIDs map[string]int32 // seeded post id -> post index
}

// NewModel is the model of a deployment seeded with data (nil: none) under bound B.
func NewModel(data *dataset.Dataset, bound time.Duration) *Model {
	m := &Model{Data: data, Bound: bound, Slack: DefaultSlack, seededIDs: map[string]int32{}}
	if data != nil {
		m.seededIDs = make(map[string]int32, len(data.Posts))
		for index := range data.Posts {
			m.seededIDs[strconv.FormatInt(data.Posts[index].ID, 10)] = int32(index)
		}
	}
	return m
}

// Post is a post composed through the API (a live post), as sent, with its compose's times and
// outcome and, once a read showed it, its identity as served.
type Post struct {
	Marker   string // the unique "#m..." token of its text, without "#"
	Author   int64
	Text     string // as sent
	URL      string // the expanded URL ending the text, or ""
	Mentions []int64
	Media    []timeline.Media
	// Sent is when the compose was sent; Acked when it was acknowledged at the latest.
	Sent, Acked time.Time
	// Done: the outcome is known (false while a benchmark phase is generated). OK: it was
	// acknowledged; a compose that failed may or may not have taken effect.
	Done, OK bool
	// Learned is the post as first served (nil until then).
	Learned *timeline.Post
}

func (p *Post) acked() bool { return p.Done && p.OK }

// Change is one follow (On) or unfollow of an edge, with its times and outcome.
type Change struct {
	On          bool
	Sent, Acked time.Time
	Done, OK    bool
}

// Edge is a follow edge, follower -> followee (user ids).
type Edge struct{ Follower, Followee int64 }

// State is what the calls so far wrote on top of the seeded data. Its zero value is not ready;
// use NewState.
type State struct {
	users     map[int64]string // registered by calls: id -> username
	names     map[string]int64
	posts     map[string]*Post // live posts by marker
	byAuthor  map[int64][]*Post
	byMention map[int64][]*Post
	byPostID  map[string]*Post  // learned post id -> post
	edges     map[Edge][]Change // changes in the order applied
	changed   map[int64][]int64 // follower -> followees whose edge changed
}

// NewState is the state of a freshly seeded deployment: nothing written yet.
func NewState() *State {
	return &State{
		users: map[int64]string{}, names: map[string]int64{}, posts: map[string]*Post{},
		byAuthor: map[int64][]*Post{}, byMention: map[int64][]*Post{}, byPostID: map[string]*Post{},
		edges: map[Edge][]Change{}, changed: map[int64][]int64{},
	}
}

// Clone is a deep copy (the seeded data is shared).
func (s *State) Clone() *State {
	c := NewState()
	for id, name := range s.users {
		c.users[id], c.names[name] = name, id
	}
	for _, p := range s.sortedPosts() {
		copied := *p
		if p.Learned != nil {
			learned := *p.Learned
			copied.Learned = &learned
		}
		c.addPost(&copied)
	}
	for edge, changes := range s.edges {
		c.edges[edge] = slices.Clone(changes)
	}
	for u, list := range s.changed {
		c.changed[u] = slices.Clone(list)
	}
	return c
}

func (s *State) sortedPosts() []*Post {
	out := make([]*Post, 0, len(s.posts))
	for _, p := range s.posts {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b *Post) int { return strings.Compare(a.Marker, b.Marker) })
	return out
}

func (s *State) addPost(p *Post) {
	s.posts[p.Marker] = p
	s.byAuthor[p.Author] = append(s.byAuthor[p.Author], p)
	for _, v := range p.Mentions {
		s.byMention[v] = append(s.byMention[v], p)
	}
	if p.Learned != nil {
		s.byPostID[p.Learned.PostID] = p
	}
}

// Post is the live post of a marker, or nil.
func (s *State) Post(marker string) *Post { return s.posts[marker] }

// Posts is the number of live posts.
func (s *State) Posts() int { return len(s.posts) }

// Fingerprint is a canonical text of the state: two states are equal exactly when their
// fingerprints are (the verifier's memo of explored orderings).
func (s *State) Fingerprint() string {
	var b strings.Builder
	ids := make([]int64, 0, len(s.users))
	for id := range s.users {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		fmt.Fprintf(&b, "u%d=%s;", id, s.users[id])
	}
	for _, p := range s.sortedPosts() {
		fmt.Fprintf(&b, "p%s:%d:%d:%t%t", p.Marker, p.Sent.UnixNano(), p.Acked.UnixNano(), p.Done, p.OK)
		if p.Learned != nil {
			fmt.Fprintf(&b, ":%s@%d", p.Learned.PostID, p.Learned.Timestamp)
		}
		b.WriteByte(';')
	}
	edges := make([]Edge, 0, len(s.edges))
	for edge := range s.edges {
		edges = append(edges, edge)
	}
	slices.SortFunc(edges, func(a, b Edge) int {
		if a.Follower != b.Follower {
			return compareInt(a.Follower, b.Follower)
		}
		return compareInt(a.Followee, b.Followee)
	})
	for _, edge := range edges {
		changes := slices.Clone(s.edges[edge])
		slices.SortFunc(changes, func(a, b Change) int { return a.Sent.Compare(b.Sent) })
		fmt.Fprintf(&b, "e%d>%d", edge.Follower, edge.Followee)
		for _, c := range changes {
			fmt.Fprintf(&b, ":%t%d,%d,%t%t", c.On, c.Sent.UnixNano(), c.Acked.UnixNano(), c.Done, c.OK)
		}
		b.WriteByte(';')
	}
	return b.String()
}

func compareInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// username is the username of a user id: registered by a call, or seeded.
func (m *Model) username(s *State, id int64) (string, bool) {
	if name, ok := s.users[id]; ok {
		return name, true
	}
	if index := m.seededUser(id); index >= 0 {
		return m.Data.Users[index].Username, true
	}
	return "", false
}

// seededUser is the dataset index of a user id, or -1.
func (m *Model) seededUser(id int64) int {
	if m.Data == nil {
		return -1
	}
	return m.Data.UserIndex(id)
}

// seededFollows is whether the dataset holds the edge u -> v.
func (m *Model) seededFollows(u, v int64) bool {
	iu, iv := m.seededUser(u), m.seededUser(v)
	return iu >= 0 && iv >= 0 && m.Data.Follows(iu, iv)
}

// Follows is the latest state of edge u -> v that was sent, pending changes included: what a
// benchmark's generation builds on.
func (m *Model) Follows(s *State, u, v int64) bool {
	if changes := s.edges[Edge{u, v}]; len(changes) > 0 {
		return changes[len(changes)-1].On
	}
	return m.seededFollows(u, v)
}

// Step applies one call with its answer to s and returns the next state, or why the answer is
// not one the contract allows in s. s itself is left unchanged.
func (m *Model) Step(s *State, c Call) (*State, error) { return m.step(s, c, parse(c)) }

func (m *Model) step(s *State, c Call, parsed *page) (*State, error) {
	next := s.Clone()
	if err := m.apply(next, c, parsed); err != nil {
		return nil, err
	}
	return next, nil
}

// Apply is Step in place: it checks the answer, then records the write or what the read
// taught (learned posts). On an error s may be partly updated; the verifier only applies to a
// copy.
func (m *Model) Apply(s *State, c Call) error { return m.apply(s, c, parse(c)) }

func (m *Model) apply(s *State, c Call, parsed *page) error {
	if err := m.Answer(s, c); err != nil {
		return err
	}
	if c.Action.Kind == KindRead {
		return m.read(s, c, parsed)
	}
	m.Record(s, c)
	return nil
}

// Answer checks a write's answer (a read's is checked by Read): it must acknowledge the write,
// except a compose without text and a registration of a username already taken, which must be
// refused.
func (m *Model) Answer(s *State, c Call) error {
	a := c.Action
	acked := func(want string) bool {
		return c.Err == "" && c.Status == 200 && strings.Contains(string(c.Body), want)
	}
	switch a.Kind {
	case KindRead:
		return nil
	case KindRegister:
		if _, taken := s.names[a.Name]; taken {
			if acked(writeSuccess) {
				return fmt.Errorf("register of the taken username %s was acknowledged", a.Name)
			}
			return nil
		}
		if !acked(writeSuccess) {
			return fmt.Errorf("register %d (%s): %s, want 200 %q", a.User, a.Name, c.answer(), writeSuccess)
		}
	case KindFollow:
		if !acked(writeSuccess) {
			return fmt.Errorf("%s %d -> %d: %s, want 200 %q", a.followName(), a.User, a.Followee, c.answer(), writeSuccess)
		}
	case KindCompose:
		if a.Text == "" {
			if acked(composeSuccess) {
				return fmt.Errorf("compose without text was acknowledged")
			}
			return nil
		}
		if !acked(composeSuccess) {
			return fmt.Errorf("compose by %d: %s, want 200 %q", a.User, c.answer(), composeSuccess)
		}
	default:
		return fmt.Errorf("unknown action kind %q", a.Kind)
	}
	return nil
}

// Record adds a write to s with its times and outcome: acknowledged when its answer
// acknowledged it, pending when the call is (a benchmark phase being generated), and
// otherwise failed, which the contract treats as possibly applied at any time after it was
// sent. A refused compose without text and a refused registration change nothing.
func (m *Model) Record(s *State, c Call) {
	a := c.Action
	done := !c.Pending
	switch a.Kind {
	case KindRegister:
		if done && c.Err == "" && c.Status == 200 && strings.Contains(string(c.Body), writeSuccess) {
			if _, taken := s.names[a.Name]; !taken {
				s.users[a.User], s.names[a.Name] = a.Name, a.User
			}
		}
	case KindFollow:
		ok := done && c.Err == "" && c.Status == 200 && strings.Contains(string(c.Body), writeSuccess)
		edge := Edge{a.User, a.Followee}
		if _, seen := s.edges[edge]; !seen {
			s.changed[a.User] = append(s.changed[a.User], a.Followee)
		}
		s.edges[edge] = append(s.edges[edge], Change{On: a.On, Sent: c.Start, Acked: c.End, Done: done, OK: ok})
	case KindCompose:
		if a.Text == "" {
			return
		}
		ok := done && c.Err == "" && c.Status == 200 && strings.Contains(string(c.Body), composeSuccess)
		if p := s.posts[a.Marker]; p != nil { // the outcome of a pending compose
			p.Sent, p.Acked, p.Done, p.OK = c.Start, c.End, done, ok
			return
		}
		s.addPost(&Post{
			Marker: a.Marker, Author: a.User, Text: a.Text, URL: a.URL,
			Mentions: slices.Clone(a.Mentions), Media: slices.Clone(a.Media),
			Sent: c.Start, Acked: c.End, Done: done, OK: ok,
		})
	}
}

// AfterRestart is the state after one pod of a stateless deployment was restarted: every
// acknowledged write survives (rule 6, durability).
func (m *Model) AfterRestart(s *State, deployment string) *State { return s }

// AfterFlush is the state after a cache tier was emptied: every acknowledged write survives
// (rule 6). home-timeline-redis is a store, not a cache: the programs never flush it.
func (m *Model) AfterFlush(s *State, cache string) *State { return s }
