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

// Visibility is how the contract places one post in one read: it must be in the timeline, it
// may be, or (absent from a list of candidates) it must not.
type Visibility int

const (
	Excluded Visibility = iota
	May
	Must
)

func (v Visibility) String() string {
	return [...]string{"excluded", "may", "must"}[v]
}

// candidate is one post a read may show: seeded (index >= 0) or live.
type candidate struct {
	seeded int32
	live   *Post
	must   bool
}

// window is a read's interval: sent at start, answered at end.
type window struct{ start, end time.Time }

// edgeAt is the state of edge u -> v for an event at t, from the changes sent before t: the
// last change's value when every one of them was acknowledged, the last more than the bound
// before t and after the others ended (the edge is certain); otherwise uncertain. Without a
// change it is the seeded edge.
func (m *Model) edgeAt(s *State, u, v int64, t time.Time) (on, certain bool) {
	var prior []Change
	for _, c := range s.edges[Edge{u, v}] {
		if c.Sent.Before(t) {
			prior = append(prior, c)
		}
	}
	if len(prior) == 0 {
		return m.seededFollows(u, v), true
	}
	last := 0
	for i := range prior {
		if prior[i].Sent.After(prior[last].Sent) {
			last = i
		}
	}
	for i, c := range prior {
		if !c.Done || !c.OK {
			return false, false // failed or in flight: possibly applied at any time
		}
		if i != last && !c.Acked.Before(prior[last].Sent) {
			return false, false // overlapping changes: their order is unknown
		}
	}
	if !prior[last].Acked.Add(m.Bound).Before(t) {
		return false, false
	}
	return prior[last].On, true
}

// delivery is rules 3 and 4 for a post of author a composed at sent (a seeded post: its
// timestamp), read by u over r, when u is not mentioned: Must means it is in u's home timeline
// once fresh (the edge certainly held when it was composed, and has not changed since), May
// that it may be (the edge was uncertain then, or changed since: posts composed before a
// change may or may not appear afterwards) and Excluded that it is not.
func (m *Model) delivery(s *State, u, a int64, sent time.Time, r window) Visibility {
	for _, c := range s.edges[Edge{u, a}] {
		if !c.Sent.Before(sent) && c.Sent.Before(r.end) {
			return May
		}
	}
	on, certain := m.edgeAt(s, u, a, sent)
	switch {
	case !certain:
		return May
	case on:
		return Must
	}
	return Excluded
}

// liveVisibility is a live post's place in a read over r: requests sent after the read ended
// are not visible; a post is visible once acknowledged more than delay before the read
// started (delay 0 for the author's user timeline, the bound for home timelines); in between
// (in flight, failed, or acknowledged within the delay) it may be.
func liveVisibility(p *Post, r window, delay time.Duration, holds []string) Visibility {
	if slices.Contains(holds, p.Marker) {
		return Must
	}
	if !p.Done || !p.Sent.Before(r.end) {
		return Excluded
	}
	if p.OK && p.Acked.Add(delay).Before(r.start) {
		return Must
	}
	return May
}

// userCandidates lists the posts of author's user timeline for a read over [start, end],
// with holds (markers) required: rule 2.
func (m *Model) userCandidates(s *State, author int64, r window, holds []string) []candidate {
	var out []candidate
	if index := m.seededUser(author); index >= 0 {
		for _, p := range m.Data.UserPosts.Row(index) {
			out = append(out, candidate{seeded: p, must: true})
		}
	}
	for _, p := range s.byAuthor[author] {
		if v := liveVisibility(p, r, 0, holds); v != Excluded {
			out = append(out, candidate{seeded: -1, live: p, must: v == Must})
		}
	}
	return out
}

// homeCandidates lists the posts of u's home timeline for a read over r: rules 3 and 4.
func (m *Model) homeCandidates(s *State, u int64, r window, holds []string) []candidate {
	var out []candidate
	changed := s.changed[u]
	// changedBefore: the edge to a changed before the read ended (later changes are invisible).
	changedBefore := map[int64]bool{}
	for _, a := range changed {
		for _, c := range s.edges[Edge{u, a}] {
			if c.Sent.Before(r.end) {
				changedBefore[a] = true
			}
		}
	}
	seen := map[*Post]bool{}
	addLive := func(p *Post, v Visibility) {
		if seen[p] {
			return
		}
		seen[p] = true
		if v == Excluded {
			return
		}
		if lv := liveVisibility(p, r, m.Bound, holds); lv != Excluded {
			out = append(out, candidate{seeded: -1, live: p, must: v == Must && lv == Must})
		}
	}
	// Mentions deliver whatever the edges.
	for _, p := range s.byMention[u] {
		addLive(p, Must)
	}
	authors := slices.Clone(changed)
	if index := m.seededUser(u); index >= 0 {
		data := m.Data
		mentioned := func(post *dataset.Post) bool { return slices.Contains(post.Mentions, int32(index)) }
		for _, p := range data.Home.Row(index) {
			post := &data.Posts[p]
			author := data.Users[post.Author].ID
			must := !changedBefore[author] || mentioned(post)
			out = append(out, candidate{seeded: p, must: must})
		}
		// Seeded posts of authors u did not follow, whose edge changed since: may.
		for a := range changedBefore {
			ia := m.seededUser(a)
			if ia < 0 || data.Follows(index, ia) {
				continue
			}
			for _, p := range data.UserPosts.Row(ia) {
				if !mentioned(&data.Posts[p]) {
					out = append(out, candidate{seeded: p, must: false})
				}
			}
		}
		for _, a := range data.Followees.Row(index) {
			if id := data.Users[a].ID; !slices.Contains(changed, id) {
				authors = append(authors, id)
			}
		}
	}
	for _, a := range authors {
		for _, p := range s.byAuthor[a] {
			if !seen[p] {
				addLive(p, m.delivery(s, u, a, p.Sent, r))
			}
		}
	}
	return out
}

// Read checks a timeline read's answer against the posts that must and may be in it, and
// learns the identity (post id, timestamp) of every live post it shows for the first time.
//
// A read of a known user with 0 <= start < stop must answer 200 with a page that is
// timeline[start:stop] of some timeline holding every post that must be in it, any of the
// posts that may be, and nothing else, ordered newest first (ties in any order), without
// duplicates; every post as stored (seeded) or as sent (live), and a live post the same in
// every read. Any other read (an unknown user, a non-numeric or negative window, start >= stop)
// must be refused or answered with no posts.
func (m *Model) Read(s *State, c Call) error { return m.read(s, c, parse(c)) }

// page is a read's answer, parsed.
type page struct {
	posts []timeline.Post
	err   error
}

func parse(c Call) *page {
	if c.Action.Kind != KindRead || c.Err != "" {
		return nil
	}
	posts, err := timeline.Parse(c.Body)
	return &page{posts, err}
}

func (m *Model) read(s *State, c Call, parsed *page) error {
	a := c.Action
	what := a.String()
	if c.Err != "" {
		return fmt.Errorf("%s: %s", what, c.Err)
	}
	user, errUser := strconv.ParseInt(a.UserID, 10, 64)
	start, errStart := strconv.Atoi(a.Start)
	stop, errStop := strconv.Atoi(a.Stop)
	_, known := m.username(s, user)
	if errUser != nil || errStart != nil || errStop != nil || !known || start < 0 || start >= stop {
		if c.Status != 200 {
			return nil
		}
		if parsed.err != nil || len(parsed.posts) != 0 {
			return fmt.Errorf("%s answered %d posts (%v), want none", what, len(parsed.posts), parsed.err)
		}
		return nil
	}
	if c.Status != 200 {
		return fmt.Errorf("%s: HTTP %d %q, want 200", what, c.Status, clip(c.Body))
	}
	if parsed.err != nil {
		return fmt.Errorf("%s: %w", what, parsed.err)
	}
	page := parsed.posts
	r := window{c.Start, c.End}
	var candidates []candidate
	if a.Home {
		candidates = m.homeCandidates(s, user, r, a.Holds)
	} else {
		candidates = m.userCandidates(s, user, r, a.Holds)
	}
	seededAt := make(map[int32]int, len(candidates))
	liveAt := map[*Post]int{}
	for index, cand := range candidates {
		if cand.seeded >= 0 {
			seededAt[cand.seeded] = index
		} else {
			liveAt[cand.live] = index
		}
	}
	matched := make([]int, len(page))
	for i := range page {
		served := &page[i]
		if seeded, ok := m.seededIDs[served.PostID]; ok {
			index, allowed := seededAt[seeded]
			if !allowed {
				return fmt.Errorf("%s: unexpected: %s does not belong in this timeline", what, timeline.Describe(served))
			}
			expected := timeline.Seeded(m.Data, seeded)
			if !timeline.Same(&expected, served) {
				return fmt.Errorf("%s: content: %s differs from the stored %s", what, timeline.Describe(served), timeline.Describe(&expected))
			}
			matched[i] = index
			continue
		}
		p := s.byPostID[served.PostID]
		if p == nil {
			p = s.posts[MarkerOf(served.Text)]
			if p == nil {
				return fmt.Errorf("%s: unexpected: %s was never composed", what, timeline.Describe(served))
			}
			if p.Learned != nil {
				return fmt.Errorf("%s: content: %s was first served as post %s", what, timeline.Describe(served), p.Learned.PostID)
			}
			if err := m.checkLive(s, p, served, r); err != nil {
				return fmt.Errorf("%s: content: %s: %v", what, timeline.Describe(served), err)
			}
			learned := *served
			p.Learned = &learned
			s.byPostID[served.PostID] = p
		} else if !timeline.Same(p.Learned, served) {
			return fmt.Errorf("%s: content: %s differs from its first read %s", what, timeline.Describe(served), timeline.Describe(p.Learned))
		}
		index, allowed := liveAt[p]
		if !allowed {
			return fmt.Errorf("%s: unexpected: %s does not belong in this timeline", what, timeline.Describe(served))
		}
		matched[i] = index
	}
	slots := make([]timeline.Candidate, len(candidates))
	for index, cand := range candidates {
		slots[index] = m.slot(cand, r)
	}
	err := timeline.Window(slots, page, matched, start, stop, func(index int) string { return m.describe(s, candidates[index]) })
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

// slot is a candidate as timeline.Window places it: its timestamp, or for a live post not yet
// seen the range its timestamp may lie in.
func (m *Model) slot(c candidate, r window) timeline.Candidate {
	if c.seeded >= 0 {
		return timeline.Candidate{Timestamp: m.Data.Posts[c.seeded].Timestamp, Must: c.must}
	}
	if c.live.Learned != nil {
		return timeline.Candidate{Timestamp: c.live.Learned.Timestamp, Must: c.must}
	}
	lo, hi := m.stampRange(c.live, r)
	return timeline.Candidate{Lo: lo, Hi: hi, Must: c.must}
}

// stampRange is where a live post's timestamp (ms) may lie: from its compose's start to its
// acknowledgement (or the read's end, when it was not acknowledged), within the clock slack.
func (m *Model) stampRange(p *Post, r window) (lo, hi int64) {
	upper := r.end
	if p.acked() && p.Acked.Before(upper) {
		upper = p.Acked
	}
	return p.Sent.Add(-m.Slack).UnixMilli(), upper.Add(m.Slack).UnixMilli()
}

func (m *Model) describe(s *State, c candidate) string {
	if c.seeded >= 0 {
		wire := timeline.Seeded(m.Data, c.seeded)
		return timeline.Describe(&wire)
	}
	if c.live.Learned != nil {
		return timeline.Describe(c.live.Learned)
	}
	return fmt.Sprintf("post #%s by %d (acknowledged %s)", c.live.Marker, c.live.Author, c.live.Acked.Format(time.StampMilli))
}

// checkLive checks a live post's first appearance against what was sent: rule 1 for a post
// the API composed (its id, request id and the shortened URL are the application's).
func (m *Model) checkLive(s *State, p *Post, served *timeline.Post, r window) error {
	name, _ := m.username(s, p.Author)
	if served.CreatorID != strconv.FormatInt(p.Author, 10) || served.Creator != name {
		return fmt.Errorf("creator (%s, %s), want (%d, %s)", served.CreatorID, served.Creator, p.Author, name)
	}
	if served.PostType != 0 {
		return fmt.Errorf("post_type %d, want 0", served.PostType)
	}
	if p.URL == "" {
		if served.Text != p.Text || len(served.URLs) != 0 {
			return fmt.Errorf("text %q urls %v, want %q", served.Text, served.URLs, p.Text)
		}
	} else if len(served.URLs) != 1 || served.URLs[0].Expanded != p.URL || served.URLs[0].Shortened == "" ||
		served.Text != strings.TrimSuffix(p.Text, p.URL)+served.URLs[0].Shortened {
		return fmt.Errorf("text %q urls %v, want the text sent with %s shortened", served.Text, served.URLs, p.URL)
	}
	var mentions []timeline.Mention
	for _, v := range p.Mentions {
		vname, _ := m.username(s, v)
		mentions = append(mentions, timeline.Mention{UserID: strconv.FormatInt(v, 10), Username: vname})
	}
	if !timeline.SameMentions(mentions, served.Mentions) {
		return fmt.Errorf("mentions %v, want %v", served.Mentions, mentions)
	}
	if !slices.Equal(served.Media, p.Media) {
		return fmt.Errorf("media %v, want %v", served.Media, p.Media)
	}
	if lo, hi := m.stampRange(p, r); served.Timestamp < lo || served.Timestamp > hi {
		return fmt.Errorf("timestamp %d outside its compose's time %d..%d (ms, with %v of clock slack)", served.Timestamp, lo, hi, m.Slack)
	}
	return nil
}

// Visibility is how a live post (by marker) or a seeded post (by id) is placed in a read of
// user's timeline over [start, end] in s: for tests and diagnostics.
func (m *Model) Visibility(s *State, home bool, user int64, key string, start, end time.Time) Visibility {
	r := window{start, end}
	var candidates []candidate
	if home {
		candidates = m.homeCandidates(s, user, r, nil)
	} else {
		candidates = m.userCandidates(s, user, r, nil)
	}
	for _, c := range candidates {
		match := (c.live != nil && c.live.Marker == key) ||
			(c.seeded >= 0 && strconv.FormatInt(m.Data.Posts[c.seeded].ID, 10) == key)
		if match {
			if c.must {
				return Must
			}
			return May
		}
	}
	return Excluded
}
