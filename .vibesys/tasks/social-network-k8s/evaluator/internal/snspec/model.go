// Package snspec is the Social Network accuracy oracle on vibesys-evaluator's `check` package:
// the contract of OBJECTIVE.md and internal/oracle (time-window visibility with the bound B,
// edge uncertainty, mentions that deliver regardless of edges, live post identity learned at first
// sight, page windows) as two check.Specs over the same history, whose verdicts are ANDed:
//
//   - AuthorSpec, keyed by the user the pieces are "about" ("u:<id>": its composes, its
//     registration, the follow edges that point at it, its user timeline, and every home read
//     projected to the authors on its page as "posts by <id> seen"; "n:<name>" for username claims;
//     "x:<raw>" for requests with an unparsable user id). It checks each author's posts in each
//     reader's home timeline: visibility, content, order and the author's slice of the page.
//   - ReaderSpec, keyed by the reader ("r:<reader>"): the page arithmetic across all authors
//     that a per-author view cannot see (how many posts must or may precede a page, an empty page
//     with posts that must be there). Every compose is replicated into the partition of every
//     reader with a home read that could receive it.
//
// Faults (restarts of a service, flushes of a cache tier) change nothing in the contract: every
// acknowledged write survives them. Spec.Fault is nil in both specs, so a post, edge or
// timeline entry lost across a fault shows as a must-visible entry that is missing later, not as
// a state change. Times are Unix nanoseconds of the loadgen clock.
package snspec

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"
	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

const ms = int64(1e6)

type Model struct {
	Data  *dataset.Dataset // seeded data (nil: none)
	Bound int64            // B, ns
	Slack int64            // clock slack for live post timestamps, ns

	seededIDs map[string]int32
}

func NewModel(d *dataset.Dataset, bound, slack int64) *Model {
	m := &Model{Data: d, Bound: bound, Slack: slack, seededIDs: map[string]int32{}}
	if d != nil {
		for i := range d.Posts {
			m.seededIDs[strconv.FormatInt(d.Posts[i].ID, 10)] = int32(i)
		}
	}
	return m
}

// ---- piece inputs / outputs ----

type WriteOut struct {
	Status int
	Acked  bool // 200 and the route's success text
}

type Compose struct {
	Author       int64
	AuthorName   string
	Marker       string
	Text, URL    string
	Mentions     []int64
	MentionNames []string
	Media        []timeline.Media
	Empty        bool
}
type Follow struct {
	U, V int64
	On   bool
}
type Register struct{ ID int64 } // key u:ID
type Claim struct {
	Name  string
	Taken bool // a seeded username
}
type ReadOut struct {
	Status   int
	Page     []timeline.Post // parsed (all posts)
	ParseErr string
}

// UserRead: user timeline read, full check on key u:<user> (valid or not).
type UserRead struct {
	User       int64
	Valid      bool // numeric ids and 0 <= start < stop
	Start, End int
	Holds      []string
	Seeded     []SeedCand // seeded posts of the user (newest first), pruned below the page
}

// HomeCheck: the page-level and validity part of a home read, key u:<reader>.
type HomeCheck struct {
	Reader     int64
	Valid      bool
	Start, End int
	Static     []SeedCand // seeded must-posts of authors that have no partition of their own
}

// HomePart: the part of a home read about one author; key u:<author>.
type HomePart struct {
	Reader, Author int64
	Start, End     int
	Holds          []string
	Seeded         []SeedCand // seeded posts of Author in Reader's home row
	Extra          []SeedCand // seeded posts of Author not in the row (edge changed => may)
	SeededFollow   bool       // dataset edge Reader->Author
	// Page: posts by Author on the page; Info: whole page.
	Info PageInfo
}
type HomePartOut struct{ Posts []timeline.Post }

type PageInfo struct {
	Len    int
	Hi, Lo int64 // timestamps (ms) of first/last post of the whole page
}

type SeedCand struct {
	Idx       int32
	TS        int64 // ms
	Mentioned bool  // mentions the reader (home) -> must regardless of edge changes
}

// ---- state (immutable by convention: Step copies what it changes) ----

type Post struct {
	Marker       string
	Author       int64
	AuthorName   string
	Text, URL    string
	Mentions     []int64
	MentionNames []string
	Media        []timeline.Media
	Sent, Acked  int64
	OK           bool
	Learned      *timeline.Post
}
type Change struct {
	On          bool
	Sent, Acked int64
	OK          bool
}
type State struct {
	Known bool
	Names map[string]bool // claimed (acked) usernames (n: keys)
	Maybe map[string]bool // claimed by calls with unknown outcome
	Posts map[string]*Post
	ByID  map[string]string
	Edges map[int64][]Change // follower -> changes of edge follower -> this key's user
}

func NewState() State { return State{} }

func (s State) cloneMaps() State {
	n := s
	n.Posts = make(map[string]*Post, len(s.Posts)+1)
	for k, v := range s.Posts {
		n.Posts[k] = v
	}
	n.ByID = make(map[string]string, len(s.ByID)+1)
	for k, v := range s.ByID {
		n.ByID[k] = v
	}
	n.Edges = make(map[int64][]Change, len(s.Edges)+1)
	for k, v := range s.Edges {
		n.Edges[k] = v
	}
	n.Names = make(map[string]bool, len(s.Names)+1)
	for k, v := range s.Names {
		n.Names[k] = v
	}
	n.Maybe = make(map[string]bool, len(s.Maybe)+1)
	for k, v := range s.Maybe {
		n.Maybe[k] = v
	}
	return n
}

func (a State) Equal(b State) bool {
	if a.Known != b.Known || len(a.Posts) != len(b.Posts) || len(a.Edges) != len(b.Edges) || len(a.Names) != len(b.Names) || len(a.Maybe) != len(b.Maybe) || len(a.ByID) != len(b.ByID) {
		return false
	}
	for k, v := range a.Posts {
		w, ok := b.Posts[k]
		if !ok || v == w {
			if !ok {
				return false
			}
			continue
		}
		if v.Sent != w.Sent || v.Acked != w.Acked || v.OK != w.OK || (v.Learned == nil) != (w.Learned == nil) {
			return false
		}
		if v.Learned != nil && v.Learned.PostID != w.Learned.PostID {
			return false
		}
	}
	for k, v := range a.Edges {
		if w, ok := b.Edges[k]; !ok || !slices.Equal(v, w) {
			return false
		}
	}
	for k := range a.Names {
		if !b.Names[k] {
			return false
		}
	}
	for k := range a.Maybe {
		if !b.Maybe[k] {
			return false
		}
	}
	return true
}

func mix(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
func hstr(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h = (h ^ uint64(s[i])) * 1099511628211
	}
	return h
}

func (s State) Hash() uint64 {
	var h uint64
	if s.Known {
		h = 77
	}
	for k, p := range s.Posts {
		x := hstr(k) ^ uint64(p.Acked)
		if p.Learned != nil {
			x ^= hstr(p.Learned.PostID)
		}
		h += mix(x)
	}
	for k, c := range s.Edges {
		x := uint64(k) * 31
		for _, e := range c {
			x = x*1000003 ^ uint64(e.Sent) ^ uint64(e.Acked)<<1
		}
		h += mix(x ^ 1<<62)
	}
	for k := range s.Names {
		h += mix(hstr(k) ^ 3)
	}
	for k := range s.Maybe {
		h += mix(hstr(k) ^ 5)
	}
	return h
}
func (s State) Describe() string {
	return fmt.Sprintf("known=%t posts=%d edges-from=%d learned=%d", s.Known, len(s.Posts), len(s.Edges), len(s.ByID))
}

// ---- helpers ----

func (m *Model) seededUser(id int64) int {
	if m.Data == nil {
		return -1
	}
	return m.Data.UserIndex(id)
}
func (m *Model) seededFollows(u, v int64) bool {
	iu, iv := m.seededUser(u), m.seededUser(v)
	return iu >= 0 && iv >= 0 && m.Data.Follows(iu, iv)
}

type window struct{ start, end int64 }

func (m *Model) edgeAt(s State, u, v int64, seededFollow bool, t int64) (on, certain bool) {
	return m.edgeAtC(s.Edges[u], seededFollow, t)
}

// edgeAtC: the state of one edge at t from its changes (see oracle.edgeAt).
func (m *Model) edgeAtC(changes []Change, seededFollow bool, t int64) (on, certain bool) {
	var prior []Change
	for _, c := range changes {
		if c.Sent < t {
			prior = append(prior, c)
		}
	}
	if len(prior) == 0 {
		return seededFollow, true
	}
	last := 0
	for i := range prior {
		if prior[i].Sent > prior[last].Sent {
			last = i
		}
	}
	for i, c := range prior {
		if !c.OK {
			return false, false
		}
		if i != last && c.Acked >= prior[last].Sent {
			return false, false
		}
	}
	if !(prior[last].Acked+m.Bound < t) {
		return false, false
	}
	return prior[last].On, true
}

type vis int

const (
	excluded vis = iota
	may
	must
)

func (m *Model) delivery(s State, u, a int64, seededFollow bool, sent int64, r window) vis {
	return m.deliveryC(s.Edges[u], seededFollow, sent, r)
}

func (m *Model) deliveryC(changes []Change, seededFollow bool, sent int64, r window) vis {
	for _, c := range changes {
		if c.Sent >= sent && c.Sent < r.end {
			return may
		}
	}
	on, certain := m.edgeAtC(changes, seededFollow, sent)
	switch {
	case !certain:
		return may
	case on:
		return must
	}
	return excluded
}

func liveVis(p *Post, r window, delay int64, holds []string) vis {
	if slices.Contains(holds, p.Marker) {
		return must
	}
	if !(p.Sent < r.end) {
		return excluded
	}
	if p.OK && p.Acked+delay < r.start {
		return must
	}
	return may
}

func (m *Model) stampRange(p *Post, r window) (lo, hi int64) {
	upper := r.end
	if p.OK && p.Acked < upper {
		upper = p.Acked
	}
	return (p.Sent - m.Slack) / ms, (upper + m.Slack) / ms
}

type cand struct {
	seeded int32
	live   *Post
	must   bool
	ts     int64 // seeded ts or learned ts (ms); 0 unknown
}

func (c cand) bounds(m *Model, r window) (lo, hi int64) {
	switch {
	case c.ts != 0:
		return c.ts, c.ts
	}
	if c.live != nil {
		return m.stampRange(c.live, r)
	}
	return -1 << 62, 1 << 62
}

func isAck(o WriteOut) bool { return o.Acked }

// step is the model transition for one piece, as the specs' Step: the states after op, or none
// and why op cannot follow s.
func (m *Model) step(s State, op check.Op[any, any]) ([]State, string) {
	out := op.Out
	if op.Unknown {
		out = unknownOut{}
	}
	n, why := m.stepPiece(s, op.In, out, op.Call, op.Return)
	if why != "" {
		return nil, why
	}
	return []State{n}, ""
}

// stepPiece is the transition for one piece (state, why) where why != "" rejects it.
func (m *Model) stepPiece(s State, in, outAny any, call, ret int64) (State, string) {
	switch in := in.(type) {
	case Compose:
		return m.stepCompose(s, in, outAny, call, ret)
	case Follow:
		return m.stepFollow(s, in, outAny, call, ret)
	case Register:
		if isUnknownOut(outAny) {
			return s, ""
		}
		o := outAny.(WriteOut)
		if o.Acked {
			n := s
			n.Known = true
			return n, ""
		}
		return s, ""
	case Claim:
		return m.stepClaim(s, in, outAny)
	case UserRead:
		if isUnknownOut(outAny) {
			return s, ""
		}
		return m.stepUserRead(s, in, outAny.(ReadOut), call, ret)
	case HomeCheck:
		if isUnknownOut(outAny) {
			return s, ""
		}
		return m.stepHomeCheck(s, in, outAny.(ReadOut), call, ret)
	case HomePart:
		if isUnknownOut(outAny) {
			return s, ""
		}
		return m.stepHomePart(s, in, outAny.(HomePartOut), call, ret)
	}
	panic(fmt.Sprintf("unknown piece %T", in))
}

// unknownOut stands for the output of a piece whose outcome is unknown.
type unknownOut struct{}

func isUnknownOut(o any) bool { _, ok := o.(unknownOut); return ok }

func (m *Model) stepCompose(s State, in Compose, outAny any, call, ret int64) (State, string) {
	unk := isUnknownOut(outAny)
	if in.Empty {
		if !unk && outAny.(WriteOut).Acked {
			return s, "compose without text was acknowledged"
		}
		return s, ""
	}
	ok := false
	if !unk {
		o := outAny.(WriteOut)
		if !o.Acked {
			return s, fmt.Sprintf("compose #%s by %d: HTTP %d, want 200 acknowledged", in.Marker, in.Author, o.Status)
		}
		ok = true
	}
	if _, dup := s.Posts[in.Marker]; dup {
		return s, ""
	}
	n := s.cloneMaps()
	n.Posts[in.Marker] = &Post{Marker: in.Marker, Author: in.Author, AuthorName: in.AuthorName, Text: in.Text, URL: in.URL,
		Mentions: in.Mentions, MentionNames: in.MentionNames, Media: in.Media, Sent: call, Acked: ret, OK: ok}
	return n, ""
}

func (m *Model) stepFollow(s State, in Follow, outAny any, call, ret int64) (State, string) {
	unk := isUnknownOut(outAny)
	ok := false
	if !unk {
		o := outAny.(WriteOut)
		if !o.Acked {
			return s, fmt.Sprintf("follow/unfollow %d -> %d: HTTP %d, want 200 Success", in.U, in.V, o.Status)
		}
		ok = true
	}
	n := s.cloneMaps()
	n.Edges[in.U] = append(slices.Clone(s.Edges[in.U]), Change{On: in.On, Sent: call, Acked: ret, OK: ok})
	return n, ""
}

func (m *Model) stepClaim(s State, in Claim, outAny any) (State, string) {
	if isUnknownOut(outAny) {
		n := s.cloneMaps()
		n.Maybe[in.Name] = true
		return n, ""
	}
	o := outAny.(WriteOut)
	taken := s.Names[in.Name] || in.Taken
	if o.Acked {
		if taken {
			return s, fmt.Sprintf("register of the taken username %s was acknowledged", in.Name)
		}
		n := s.cloneMaps()
		n.Names[in.Name] = true
		return n, ""
	}
	if !taken && !s.Maybe[in.Name] {
		return s, fmt.Sprintf("register of the free username %s: HTTP %d, want 200 Success", in.Name, o.Status)
	}
	return s, ""
}

func (s State) known(m *Model, id int64) bool { return m.seededUser(id) >= 0 || s.Known }

// invalidRead: a request that must be refused or answered empty.
func invalidRead(what string, o ReadOut) string {
	if o.Status != 200 {
		return ""
	}
	if o.ParseErr != "" || len(o.Page) != 0 {
		return fmt.Sprintf("%s answered %d posts (%s), want none", what, len(o.Page), o.ParseErr)
	}
	return ""
}

func (m *Model) stepUserRead(s State, in UserRead, o ReadOut, call, ret int64) (State, string) {
	what := fmt.Sprintf("user timeline of %d [%d,%d)", in.User, in.Start, in.End)
	if !in.Valid || !s.known(m, in.User) {
		return s, invalidRead(what, o)
	}
	if o.Status != 200 {
		return s, fmt.Sprintf("%s: HTTP %d, want 200", what, o.Status)
	}
	if o.ParseErr != "" {
		return s, what + ": " + o.ParseErr
	}
	r := window{call, ret}
	var cands []cand
	for _, sc := range in.Seeded {
		cands = append(cands, cand{seeded: sc.Idx, must: true, ts: sc.TS})
	}
	for _, p := range s.Posts {
		if v := liveVis(p, r, 0, in.Holds); v != excluded {
			cands = append(cands, cand{seeded: -1, live: p, must: v == must, ts: learnedTS(p)})
		}
	}
	next, why := m.matchPage(s, what, cands, o.Page, r, in.Start, in.End, nil)
	if why != "" {
		return s, why
	}
	return next, m.windowFull(what, cands, o.Page, in.Start, in.End, r, next)
}

func learnedTS(p *Post) int64 {
	if p.Learned != nil {
		return p.Learned.Timestamp
	}
	return 0
}

// matchPage validates every served post against the candidates and learns live identities.
// It returns the (possibly cloned) state. matchedOut receives candidate index per page post.
func (m *Model) matchPage(s State, what string, cands []cand, page []timeline.Post, r window, start, end int, matchedOut *[]int) (State, string) {
	seededAt := map[int32]int{}
	liveAt := map[*Post]int{}
	for i, c := range cands {
		if c.live == nil {
			seededAt[c.seeded] = i
		} else {
			liveAt[c.live] = i
		}
	}
	cur := s
	cloned := false
	matched := make([]int, len(page))
	for i := range page {
		served := &page[i]
		if idx, ok := m.seededIDs[served.PostID]; ok {
			ci, allowed := seededAt[idx]
			if !allowed {
				return s, fmt.Sprintf("%s: unexpected: %s does not belong in this timeline", what, timeline.Describe(served))
			}
			exp := timeline.Seeded(m.Data, idx)
			if !timeline.Same(&exp, served) {
				return s, fmt.Sprintf("%s: content: %s differs from the stored %s", what, timeline.Describe(served), timeline.Describe(&exp))
			}
			matched[i] = ci
			continue
		}
		var p *Post
		if mk, ok := cur.ByID[served.PostID]; ok {
			p = cur.Posts[mk]
		}
		if p == nil {
			mk := markerOf(served.Text)
			p = cur.Posts[mk]
			if p == nil {
				return s, fmt.Sprintf("%s: unexpected: %s was never composed", what, timeline.Describe(served))
			}
			if p.Learned != nil {
				return s, fmt.Sprintf("%s: content: %s was first served as post %s", what, timeline.Describe(served), p.Learned.PostID)
			}
			if err := m.checkLive(p, served, r); err != "" {
				return s, fmt.Sprintf("%s: content: %s: %s", what, timeline.Describe(served), err)
			}
			if !cloned {
				cur = cur.cloneMaps()
				cloned = true
			}
			np := *p
			l := *served
			np.Learned = &l
			cur.Posts[mk] = &np
			cur.ByID[served.PostID] = mk
			// candidates referencing the old pointer: remap
			if ci, ok := liveAt[p]; ok {
				cands[ci].live = &np
				cands[ci].ts = served.Timestamp
				delete(liveAt, p)
				liveAt[&np] = ci
			}
			p = &np
		} else if !timeline.Same(p.Learned, served) {
			return s, fmt.Sprintf("%s: content: %s differs from its first read %s", what, timeline.Describe(served), timeline.Describe(p.Learned))
		}
		ci, allowed := liveAt[p]
		if !allowed {
			// p may be a pointer from the original state while liveAt holds remapped one
			found := false
			for q, i2 := range liveAt {
				if q.Marker == p.Marker {
					ci, found = i2, true
				}
			}
			if !found {
				return s, fmt.Sprintf("%s: unexpected: %s does not belong in this timeline", what, timeline.Describe(served))
			}
		}
		matched[i] = ci
	}
	if matchedOut != nil {
		*matchedOut = matched
	}
	return cur, ""
}

func (m *Model) checkLive(p *Post, served *timeline.Post, r window) string {
	if served.CreatorID != strconv.FormatInt(p.Author, 10) || served.Creator != p.AuthorName {
		return fmt.Sprintf("creator (%s, %s), want (%d, %s)", served.CreatorID, served.Creator, p.Author, p.AuthorName)
	}
	if served.PostType != 0 {
		return fmt.Sprintf("post_type %d, want 0", served.PostType)
	}
	if p.URL == "" {
		if served.Text != p.Text || len(served.URLs) != 0 {
			return fmt.Sprintf("text %q urls %v, want %q", served.Text, served.URLs, p.Text)
		}
	} else if len(served.URLs) != 1 || served.URLs[0].Expanded != p.URL || served.URLs[0].Shortened == "" ||
		served.Text != strings.TrimSuffix(p.Text, p.URL)+served.URLs[0].Shortened {
		return fmt.Sprintf("text %q urls %v, want the text sent with %s shortened", served.Text, served.URLs, p.URL)
	}
	var mentions []timeline.Mention
	for i, v := range p.Mentions {
		mentions = append(mentions, timeline.Mention{UserID: strconv.FormatInt(v, 10), Username: p.MentionNames[i]})
	}
	if !timeline.SameMentions(mentions, served.Mentions) {
		return fmt.Sprintf("mentions %v, want %v", served.Mentions, mentions)
	}
	if !slices.Equal(served.Media, p.Media) {
		return fmt.Sprintf("media %v, want %v", served.Media, p.Media)
	}
	if lo, hi := m.stampRange(p, r); served.Timestamp < lo || served.Timestamp > hi {
		return fmt.Sprintf("timestamp %d outside its compose's time %d..%d (ms, with %dms clock slack)", served.Timestamp, lo, hi, m.Slack/ms)
	}
	return ""
}

func markerOf(text string) string {
	at := strings.Index(text, "#m")
	if at < 0 {
		return ""
	}
	end := at + 1
	for end < len(text) && text[end] != ' ' {
		end++
	}
	return text[at+1 : end]
}

// windowFull: timeline.Window over every candidate (user timeline: one author, exact).
func (m *Model) windowFull(what string, cands []cand, page []timeline.Post, start, end int, r window, s State) string {
	// rebuild matched by identity
	matched := make([]int, len(page))
	for i := range page {
		matched[i] = -1
		for ci, c := range cands {
			if c.live == nil {
				if m.Data != nil && strconv.FormatInt(m.Data.Posts[c.seeded].ID, 10) == page[i].PostID {
					matched[i] = ci
				}
			} else if c.live.Learned != nil && c.live.Learned.PostID == page[i].PostID {
				matched[i] = ci
			}
		}
	}
	slots := make([]timeline.Candidate, len(cands))
	for i, c := range cands {
		if c.ts != 0 {
			slots[i] = timeline.Candidate{Timestamp: c.ts, Must: c.must}
		} else {
			lo, hi := c.bounds(m, r)
			slots[i] = timeline.Candidate{Lo: lo, Hi: hi, Must: c.must}
		}
	}
	err := timeline.Window(slots, page, matched, start, end, func(i int) string { return m.describe(cands[i]) })
	if err != nil {
		return what + ": " + err.Error()
	}
	return ""
}

func (m *Model) describe(c cand) string {
	if c.live == nil {
		w := timeline.Seeded(m.Data, c.seeded)
		return timeline.Describe(&w)
	}
	if c.live.Learned != nil {
		return timeline.Describe(c.live.Learned)
	}
	return fmt.Sprintf("post #%s by %d (acked %d)", c.live.Marker, c.live.Author, c.live.Acked)
}

// pageChecks: ordering, duplicates, size. Stateless.
func pageChecks(what string, page []timeline.Post, start, end int) string {
	for i := 1; i < len(page); i++ {
		if page[i].Timestamp > page[i-1].Timestamp {
			return fmt.Sprintf("%s: order: %s follows older %s", what, timeline.Describe(&page[i]), timeline.Describe(&page[i-1]))
		}
	}
	seen := map[string]bool{}
	for i := range page {
		if seen[page[i].PostID] {
			return fmt.Sprintf("%s: duplicate: %s appears twice", what, timeline.Describe(&page[i]))
		}
		seen[page[i].PostID] = true
	}
	if len(page) > end-start {
		return fmt.Sprintf("%s: page holds %d posts, asked for %d", what, len(page), end-start)
	}
	return ""
}

func pageInfo(page []timeline.Post) PageInfo {
	if len(page) == 0 {
		return PageInfo{}
	}
	return PageInfo{Len: len(page), Hi: page[0].Timestamp, Lo: page[len(page)-1].Timestamp}
}

func (m *Model) stepHomeCheck(s State, in HomeCheck, o ReadOut, call, ret int64) (State, string) {
	what := fmt.Sprintf("home timeline of %d [%d,%d)", in.Reader, in.Start, in.End)
	if !in.Valid || !s.known(m, in.Reader) {
		return s, invalidRead(what, o)
	}
	if o.Status != 200 {
		return s, fmt.Sprintf("%s: HTTP %d, want 200", what, o.Status)
	}
	if o.ParseErr != "" {
		return s, what + ": " + o.ParseErr
	}
	if w := pageChecks(what, o.Page, in.Start, in.End); w != "" {
		return s, w
	}
	// seeded must-posts of authors without a partition of their own (never touched)
	info := pageInfo(o.Page)
	short := info.Len < in.End-in.Start
	inPage := map[string]bool{}
	for i := range o.Page {
		inPage[o.Page[i].PostID] = true
	}
	for _, sc := range in.Static {
		if inPage[strconv.FormatInt(m.Data.Posts[sc.Idx].ID, 10)] {
			continue
		}
		if why := perCandidate(what, true, sc.TS, sc.TS, info, short, in.Start, func() string { return m.describe(cand{seeded: sc.Idx}) }); why != "" {
			return s, why
		}
	}
	return s, ""
}

// perCandidate is timeline.Window's per-candidate rule for one MUST candidate not in the page.
func perCandidate(what string, isMust bool, low, high int64, info PageInfo, short bool, start int, desc func() string) string {
	if !isMust {
		return ""
	}
	if info.Len == 0 {
		return "" // handled by the count rule
	}
	canBefore := high >= info.Hi
	canAfter := !short && low <= info.Lo
	if !canBefore && !canAfter {
		if short && high < info.Lo {
			return fmt.Sprintf("%s: missing: %s is older than the short page's last post", what, desc())
		}
		if !short && low > info.Lo {
			return fmt.Sprintf("%s: missing: %s lies within the page", what, desc())
		}
		return fmt.Sprintf("%s: missing: %s belongs in the page", what, desc())
	}
	if canBefore && !canAfter && start == 0 && low > info.Hi {
		return fmt.Sprintf("%s: missing: %s is newer than the first page's first post", what, desc())
	}
	return ""
}

func (m *Model) stepHomePart(s State, in HomePart, o HomePartOut, call, ret int64) (State, string) {
	what := fmt.Sprintf("home timeline of %d [%d,%d) (posts by %d)", in.Reader, in.Start, in.End, in.Author)
	r := window{call, ret}
	u, a := in.Reader, in.Author
	changedBefore := false
	for _, c := range s.Edges[u] {
		if c.Sent < r.end {
			changedBefore = true
		}
	}
	var cands []cand
	for _, sc := range in.Seeded {
		cands = append(cands, cand{seeded: sc.Idx, must: !changedBefore || sc.Mentioned, ts: sc.TS})
	}
	if changedBefore && !in.SeededFollow {
		for _, sc := range in.Extra {
			cands = append(cands, cand{seeded: sc.Idx, must: false, ts: sc.TS})
		}
	}
	for _, p := range s.Posts {
		if p.Author != a {
			continue
		}
		var v vis
		if slices.Contains(p.Mentions, u) {
			v = must
		} else {
			v = m.delivery(s, u, a, in.SeededFollow, p.Sent, r)
		}
		if v == excluded {
			continue
		}
		if lv := liveVis(p, r, m.Bound, in.Holds); lv != excluded {
			cands = append(cands, cand{seeded: -1, live: p, must: v == must && lv == must, ts: learnedTS(p)})
		}
	}
	var matched []int
	next, why := m.matchPage(s, what, cands, o.Posts, r, in.Start, in.End, &matched)
	if why != "" {
		return s, why
	}
	// per-author window rules (necessary conditions of timeline.Window)
	info := in.Info
	short := info.Len < in.End-in.Start
	inPage := map[int]bool{}
	for _, ci := range matched {
		inPage[ci] = true
	}
	before, mustOutside := 0, 0
	for ci, c := range cands {
		if inPage[ci] || !c.must {
			continue
		}
		lc := c
		if lc.live != nil && lc.live.Learned == nil {
			// learned may have been set by this very step
			if q := next.Posts[lc.live.Marker]; q != nil && q.Learned != nil {
				lc.ts = q.Learned.Timestamp
			}
		}
		low, high := lc.bounds(m, r)
		mustOutside++
		if info.Len > 0 {
			if w := perCandidate(what, true, low, high, info, short, in.Start, func() string { return m.describe(c) }); w != "" {
				return s, w
			}
			if low > info.Hi {
				before++
			}
		}
	}
	if info.Len == 0 && mustOutside > in.Start {
		return s, fmt.Sprintf("%s: page is empty but the timeline holds at least %d posts by this author", what, mustOutside)
	}
	if short && mustOutside > before {
		before = mustOutside
	}
	if info.Len > 0 && in.Start < before {
		return s, fmt.Sprintf("%s: page starts too new: %d newer posts by this author must precede it", what, before)
	}
	return next, ""
}
