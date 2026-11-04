package socialnetwork

import (
	"strconv"
	"sync"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// Model is what the application must serve: the seeded dataset plus the
// writes of this run (and of earlier phases, replayed from their journal),
// each with the times it was sent and acknowledged. It decides for every
// timeline read which posts must, may or must not be in it under the
// freshness contract (see INTERFACE.md):
//
//   - a post is in its author's user timeline once acknowledged;
//   - it is in the home timeline of each follower and mentioned user for
//     reads starting more than Bound after the acknowledgement;
//   - a follow edge changed during the run decides delivery of posts
//     composed more than Bound after the change was acknowledged; for
//     posts composed earlier, or while the edge was uncertain, either
//     outcome is allowed (push and pull designs both comply).
//
// All methods are called with mu held.
type Model struct {
	mu    sync.Mutex
	data  *dataset.Dataset
	Bound time.Duration

	posts     []*livePost
	byAuthor  map[int32][]*livePost
	byMention map[int32][]*livePost
	byMarker  map[string]*livePost
	byPostID  map[string]*livePost
	seededIDs map[string]int32

	edges   map[uint64]*edge
	changed map[int32][]int32 // follower -> authors whose edge changed

	journal []event
}

type livePost struct {
	seq      int64
	marker   string
	author   int32
	random   uint64
	text     string // as sent
	url      string // expanded URL ending the text, or ""
	mentions []int32
	media    []timeline.Media
	sentAt   time.Time
	ackedAt  time.Time // zero until acknowledged
	failed   bool      // the compose failed: the post may or may not exist
	learned  *timeline.Post
}

func (p *livePost) acked() bool { return !p.ackedAt.IsZero() && !p.failed }

type edge struct {
	initial bool
	changes []*change // in the order sent; never two in flight
}

type change struct {
	u, v    int32
	on      bool
	sentAt  time.Time
	ackedAt time.Time
	failed  bool
	done    bool
}

func edgeKey(u, v int32) uint64 { return uint64(uint32(u))<<32 | uint64(uint32(v)) }

func newModel(data *dataset.Dataset, bound time.Duration) *Model {
	model := &Model{
		data: data, Bound: bound,
		byAuthor: map[int32][]*livePost{}, byMention: map[int32][]*livePost{},
		byMarker: map[string]*livePost{}, byPostID: map[string]*livePost{},
		seededIDs: make(map[string]int32, len(data.Posts)),
		edges:     map[uint64]*edge{}, changed: map[int32][]int32{},
	}
	for index := range data.Posts {
		model.seededIDs[strconv.FormatInt(data.Posts[index].ID, 10)] = int32(index)
	}
	return model
}

// follows is the current (latest sent) state of edge u -> v.
func (m *Model) follows(u, v int32) bool {
	if e := m.edges[edgeKey(u, v)]; e != nil && len(e.changes) > 0 {
		return e.changes[len(e.changes)-1].on
	}
	return m.data.Follows(int(u), int(v))
}

// busy reports whether a change of edge u -> v is in flight.
func (m *Model) busy(u, v int32) bool {
	e := m.edges[edgeKey(u, v)]
	return e != nil && len(e.changes) > 0 && !e.changes[len(e.changes)-1].done
}

type certainty int

const (
	off certainty = iota
	on
	uncertain
)

// stateAt is the edge's certain state for an event at time t: the last
// change sent before t must have been acknowledged at least Bound before t.
func (m *Model) stateAt(e *edge, t time.Time) certainty {
	var last *change
	for _, c := range e.changes {
		if c.sentAt.Before(t) {
			last = c
		}
	}
	if last == nil {
		return boolState(e.initial)
	}
	if !last.done || last.failed || last.ackedAt.After(t.Add(-m.Bound)) {
		return uncertain
	}
	return boolState(last.on)
}

// pushState is the edge's state for a post sent at sent and acknowledged
// at acked (zero: not yet): uncertain if the edge changed meanwhile.
func (m *Model) pushState(e *edge, sent, acked time.Time) certainty {
	for _, c := range e.changes {
		if !c.sentAt.Before(sent) && (acked.IsZero() || !c.sentAt.After(acked)) {
			return uncertain
		}
	}
	return m.stateAt(e, sent)
}

func boolState(value bool) certainty {
	if value {
		return on
	}
	return off
}

// candidate is one post allowed in a timeline: seeded (index >= 0) or live.
type candidate struct {
	seeded int32
	live   *livePost
	must   bool
}

func (m *Model) timestampOf(c candidate) int64 {
	if c.seeded >= 0 {
		return m.data.Posts[c.seeded].Timestamp
	}
	if c.live.learned != nil {
		return c.live.learned.Timestamp
	}
	return 0
}

// liveVisible is whether a live post must be (true) or may be (false) in a
// timeline read starting at start, with delay the propagation allowance.
func liveMust(p *livePost, start time.Time, delay time.Duration) bool {
	return p.acked() && p.ackedAt.Add(delay).Before(start)
}

// userCandidates lists the posts of author's user timeline for a read
// starting at start.
func (m *Model) userCandidates(author int32, start time.Time) []candidate {
	row := m.data.UserPosts.Row(int(author))
	live := m.byAuthor[author]
	out := make([]candidate, 0, len(row)+len(live))
	for _, p := range live {
		out = append(out, candidate{seeded: -1, live: p, must: liveMust(p, start, 0)})
	}
	for _, p := range row {
		out = append(out, candidate{seeded: p, must: true})
	}
	return out
}

// homeCandidates lists the posts of user's home timeline for a read
// starting at start.
func (m *Model) homeCandidates(user int32, start time.Time) []candidate {
	changed := m.changed[user]
	isChanged := func(author int32) bool {
		for _, a := range changed {
			if a == author {
				return true
			}
		}
		return false
	}
	var out []candidate
	seen := map[*livePost]bool{}
	// Mentions deliver regardless of edges.
	for _, p := range m.byMention[user] {
		seen[p] = true
		out = append(out, candidate{seeded: -1, live: p, must: liveMust(p, start, m.Bound)})
	}
	mentioned := func(post *dataset.Post) bool {
		for _, v := range post.Mentions {
			if v == user {
				return true
			}
		}
		return false
	}
	for _, index := range m.data.Home.Row(int(user)) {
		post := &m.data.Posts[index]
		if isChanged(post.Author) && !mentioned(post) {
			continue // decided below with the edge's history
		}
		out = append(out, candidate{seeded: index, must: true})
	}
	// Live posts of followees whose edge never changed.
	for _, author := range m.data.Followees.Row(int(user)) {
		if isChanged(author) {
			continue
		}
		for _, p := range m.byAuthor[author] {
			if !seen[p] {
				seen[p] = true
				out = append(out, candidate{seeded: -1, live: p, must: liveMust(p, start, m.Bound)})
			}
		}
	}
	// Authors whose edge changed: every post of theirs by the edge history.
	for _, author := range changed {
		e := m.edges[edgeKey(user, author)]
		pull := m.stateAt(e, start)
		decide := func(push certainty) (bool, bool) { // (candidate, must)
			switch {
			case push == on && pull == on:
				return true, true
			case push == off && pull == off:
				return false, false
			default:
				return true, false
			}
		}
		for _, index := range m.data.UserPosts.Row(int(author)) {
			post := &m.data.Posts[index]
			if mentioned(post) {
				continue // already listed as a seeded home entry
			}
			stamp := time.UnixMilli(post.Timestamp)
			if ok, must := decide(m.pushState(e, stamp, stamp)); ok {
				out = append(out, candidate{seeded: index, must: must})
			}
		}
		for _, p := range m.byAuthor[author] {
			if seen[p] {
				continue
			}
			if ok, must := decide(m.pushState(e, p.sentAt, p.ackedAt)); ok {
				out = append(out, candidate{seeded: -1, live: p, must: must && liveMust(p, start, m.Bound)})
			}
		}
	}
	return out
}

// beginPost records a compose being sent.
func (m *Model) beginPost(p *livePost) {
	m.posts = append(m.posts, p)
	m.byAuthor[p.author] = append(m.byAuthor[p.author], p)
	for _, v := range p.mentions {
		m.byMention[v] = append(m.byMention[v], p)
	}
	m.byMarker[p.marker] = p
	m.journal = append(m.journal, event{Kind: eventPostBegin, Time: p.sentAt.UnixNano(), U: p.author, Seq: p.seq, Random: p.random, Marker: p.marker})
}

// endPost records the compose's outcome.
func (m *Model) endPost(p *livePost, ok bool, at time.Time) {
	if !p.ackedAt.IsZero() || p.failed {
		return
	}
	p.ackedAt, p.failed = at, !ok
	m.journal = append(m.journal, event{Kind: eventPostEnd, Time: at.UnixNano(), Marker: p.marker, Seq: p.seq, OK: ok})
}

// beginChange records a follow (on) or unfollow being sent.
func (m *Model) beginChange(u, v int32, on bool, at time.Time) *change {
	key := edgeKey(u, v)
	e := m.edges[key]
	if e == nil {
		e = &edge{initial: m.data.Follows(int(u), int(v))}
		m.edges[key] = e
		m.changed[u] = append(m.changed[u], v)
	}
	c := &change{u: u, v: v, on: on, sentAt: at}
	e.changes = append(e.changes, c)
	m.journal = append(m.journal, event{Kind: eventEdgeBegin, Time: at.UnixNano(), U: u, V: v, OK: on})
	return c
}

// endChange records the change's outcome.
func (m *Model) endChange(c *change, ok bool, at time.Time) {
	if c.done {
		return
	}
	c.done, c.ackedAt, c.failed = true, at, !ok
	m.journal = append(m.journal, event{Kind: eventEdgeEnd, Time: at.UnixNano(), U: c.u, V: c.v, OK: ok})
}

// learn records a live post's identity as first served.
func (m *Model) learn(p *livePost, post *timeline.Post) {
	copied := *post
	p.learned = &copied
	m.byPostID[post.PostID] = p
}
