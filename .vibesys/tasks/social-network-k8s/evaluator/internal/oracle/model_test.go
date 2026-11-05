package oracle

import (
	"strconv"
	"testing"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// unfollowedPair is a pair of seeded users u, v where neither follows the other.
func unfollowedPair() (u, v int64) {
	iu, iv := 0, 1
	for small.Follows(iu, iv) || small.Follows(iv, iu) {
		iv++
	}
	return small.Users[iu].ID, small.Users[iv].ID
}

// TestStepFollowRules: rules 3 and 4, the edge's history deciding delivery to a home timeline.
func TestStepFollowRules(t *testing.T) {
	m := NewModel(small, time.Second)
	s := NewState()
	u, v := unfollowedPair()
	name := small.Users[v-1].Username
	n := 0
	compose := func(sent, acked int) string {
		n++
		marker := "mtest" + strconv.Itoa(n)
		apply(t, m, s, composeOK(post(v, name, marker), sent, acked))
		return marker
	}
	vis := func(marker string, ms int) Visibility { return m.Visibility(s, true, u, marker, at(ms), at(ms+5)) }
	expect := func(what, marker string, ms int, want Visibility) {
		t.Helper()
		if got := vis(marker, ms); got != want {
			t.Fatalf("%s: %v, want %v", what, got, want)
		}
	}
	before := compose(0, 10)
	expect("post of an unfollowed author", before, 5000, Excluded)
	apply(t, m, s, writeOK(Follow(u, v, true), 1000, 1100))
	during := compose(1500, 1600) // within the bound after the follow
	after := compose(2200, 2300)  // more than the bound after the follow
	expect("a post composed before the follow, read after it", before, 5000, May)
	expect("a post composed within the bound after the follow", during, 5000, May)
	expect("a post composed after the follow settled, read after the bound", after, 3400, Must)
	expect("a post read within the bound of its acknowledgement", after, 3000, May)
	expect("a compose sent after the read ended", compose(9_000_000, 9_000_010), 3400, Excluded)
	unfollow := Follow(u, v, false)
	// An unfollow in flight while the read runs (sent after it started): may (the old model
	// required the post).
	s2 := s.Clone()
	apply(t, m, s2, writeOK(unfollow, 4002, 4100))
	if got := m.Visibility(s2, true, u, after, at(4000), at(4050)); got != May {
		t.Fatalf("post during an unfollow sent while the read ran: %v, want may", got)
	}
	apply(t, m, s, writeOK(unfollow, 4000, 4100))
	expect("a post during an unfollow in flight", after, 4050, May)
	late := compose(5200, 5300)
	expect("a post composed after the unfollow settled", late, 7000, Excluded)
	expect("a post composed while followed, read after the unfollow", after, 7000, May)
	// Follow again: a post composed before the unfollow stays optional (it was composed before a
	// change; the old model required it again).
	apply(t, m, s, writeOK(Follow(u, v, true), 7500, 7600))
	expect("a post composed while followed, then unfollowed and followed again", after, 9000, May)
	expect("a post composed while unfollowed, then followed", late, 9000, May)
	// Mentions deliver whatever the edge.
	mention := post(v, name, "mtestmention")
	mention.Mentions = []int64{u}
	apply(t, m, s, composeOK(mention, 10000, 10100))
	expect("a mention", "mtestmention", 11200, Must)
	expect("a mention within the bound", "mtestmention", 10500, May)
	// A failed change leaves the edge uncertain: a post after it may or may not be delivered.
	m.Record(s, done(Follow(u, v, false), 12000, 12100, 500, nil))
	unknown := compose(14000, 14100)
	expect("a post after a failed unfollow", unknown, 16000, May)
}

// TestStepSeededHomeTimeline: the seeded home timeline is required, until an edge changes.
func TestStepSeededHomeTimeline(t *testing.T) {
	m := NewModel(small, time.Second)
	s := NewState()
	// A user with a followee who posted.
	var u, a int64
	var postID string
	for index := range small.Users {
		for _, followee := range small.Followees.Row(index) {
			if row := small.UserPosts.Row(int(followee)); len(row) > 0 {
				p := small.Posts[row[0]]
				if len(p.Mentions) == 0 {
					u, a, postID = int64(index)+1, int64(followee)+1, strconv.FormatInt(p.ID, 10)
					break
				}
			}
		}
		if u != 0 {
			break
		}
	}
	if got := m.Visibility(s, true, u, postID, at(0), at(1)); got != Must {
		t.Fatalf("seeded home post: %v", got)
	}
	apply(t, m, s, writeOK(Follow(u, a, false), 100, 200))
	if got := m.Visibility(s, true, u, postID, at(5000), at(5001)); got != May {
		t.Fatalf("seeded home post after an unfollow: %v", got)
	}
	if got := m.Visibility(s, true, u, postID, at(50), at(90)); got != Must {
		t.Fatalf("seeded home post read before the unfollow was sent: %v", got)
	}
	// The whole seeded home timeline, read once, is accepted; a post missing from it is not.
	user := int(u - 1)
	row := small.Home.Row(user)
	var page []timeline.Post
	for _, p := range row[:min(len(row), 10)] {
		page = append(page, timeline.Seeded(small, p))
	}
	fresh := NewState()
	accepts(t, m, fresh, read(true, u, 0, 10, 0, 1, page...))
	if len(page) >= 3 {
		gap := append(append([]timeline.Post(nil), page[:1]...), page[2:]...)
		rejects(t, m, fresh, read(true, u, 0, 10, 0, 1, gap...), "missing")
		changed := append([]timeline.Post(nil), page...)
		changed[1].Text += "!"
		rejects(t, m, fresh, read(true, u, 0, 10, 0, 1, changed...), "differs from the stored")
	}
}

// TestStepUserTimeline: rule 2 (read your write, no delay) and rule 1 (exact data).
func TestStepUserTimeline(t *testing.T) {
	m := NewModel(nil, time.Second)
	s := NewState()
	apply(t, m, s, writeOK(Register(3_000_000_001, "alice"), 0, 1))
	a := post(3_000_000_001, "alice", "mtest1")
	apply(t, m, s, composeOK(a, 10, 20))
	p := served(a, 501, 15)
	accepts(t, m, s, read(false, a.User, 0, 10, 21, 22, p))
	rejects(t, m, s, read(false, a.User, 0, 10, 21, 22), "empty")
	// A read concurrent with the compose may miss it.
	accepts(t, m, s, read(false, a.User, 0, 10, 12, 30))
	// A read that ended before the compose was sent cannot show it.
	rejects(t, m, s, read(false, a.User, 0, 10, 2, 9, p), "does not belong")
	// Content as sent.
	bad := p
	bad.Text = "hello #mtest1"
	rejects(t, m, s, read(false, a.User, 0, 10, 21, 22, bad), "text")
	bad = p
	bad.Creator = "bob"
	rejects(t, m, s, read(false, a.User, 0, 10, 21, 22, bad), "creator")
	bad = p
	bad.Timestamp = at(20).Add(6 * time.Second).UnixMilli()
	rejects(t, m, s, read(false, a.User, 0, 10, 21, 22, bad), "timestamp")
	bad = p
	bad.Media = []timeline.Media{{ID: "1", Type: "png"}}
	rejects(t, m, s, read(false, a.User, 0, 10, 21, 22, bad), "media")
	// Learned at first sight; served the same afterwards.
	apply(t, m, s, read(false, a.User, 0, 10, 21, 22, p))
	again := p
	again.ReqID = "78"
	rejects(t, m, s, read(false, a.User, 0, 10, 23, 24, again), "differs from its first read")
	other := served(a, 502, 15)
	rejects(t, m, s, read(false, a.User, 0, 10, 23, 24, other), "first served as post 501")
	// Order, duplicates, foreign posts.
	b := post(3_000_000_001, "alice", "mtest2")
	apply(t, m, s, composeOK(b, 40, 50))
	q := served(b, 503, 45)
	accepts(t, m, s, read(false, a.User, 0, 10, 51, 52, q, p))
	rejects(t, m, s, read(false, a.User, 0, 10, 51, 52, p, q), "order")
	rejects(t, m, s, read(false, a.User, 0, 10, 51, 52, q, q), "duplicate")
	stranger := served(Action{User: 3_000_000_001, Name: "alice", Text: "x #mnever"}, 999, 45)
	rejects(t, m, s, read(false, a.User, 0, 10, 51, 52, q, stranger, p), "never composed")
	// Windows.
	accepts(t, m, s, read(false, a.User, 1, 2, 51, 52, p))
	rejects(t, m, s, read(false, a.User, 1, 2, 51, 52, q), "too old")
	accepts(t, m, s, read(false, a.User, 2, 5, 51, 52))
	// The page is short: the newer post, not yet seen, must be in it.
	fresh := NewState()
	apply(t, m, fresh, writeOK(Register(3_000_000_001, "alice"), 0, 1), composeOK(a, 10, 20), composeOK(b, 40, 50))
	rejects(t, m, fresh, read(false, a.User, 0, 10, 51, 52, p), "starts too new")
}

// TestStepWritesAndInvalidRequests: writes are acknowledged; refused requests change nothing.
func TestStepWritesAndInvalidRequests(t *testing.T) {
	m := NewModel(nil, time.Second)
	s := NewState()
	rejects(t, m, s, done(Register(3_000_000_001, "alice"), 0, 1, 500, []byte("error")), "want 200")
	apply(t, m, s, writeOK(Register(3_000_000_001, "alice"), 0, 1))
	rejects(t, m, s, writeOK(Register(3_000_000_002, "alice"), 2, 3), "taken username")
	accepts(t, m, s, done(Register(3_000_000_002, "alice"), 2, 3, 500, []byte("already existed")))
	rejects(t, m, s, done(Follow(3_000_000_001, 5, true), 4, 5, 200, []byte("Failed")), "want 200")
	empty := Action{Kind: KindCompose, User: 3_000_000_001, Name: "alice"}
	rejects(t, m, s, composeOK(empty, 6, 7), "without text was acknowledged")
	s = accepts(t, m, s, done(empty, 6, 7, 400, []byte("Incomplete arguments")))
	if s.Posts() != 0 {
		t.Fatal("a refused compose left a post")
	}
	rejects(t, m, s, composeOK(post(3_000_000_001, "alice", "m1"), 8, 9).withError("connection reset"), "connection reset")
	// Invalid reads: refused, or answered with no posts.
	for _, a := range []Action{
		RawRead(false, "3000000999", "0", "10"), RawRead(true, "3000000001", "5", "2"),
		RawRead(true, "3000000001", "-1", "3"), RawRead(false, "3000000001", "x", "y"), RawRead(false, "", "0", "10"),
	} {
		accepts(t, m, s, done(a, 10, 11, 400, nil))
		accepts(t, m, s, done(a, 10, 11, 200, []byte("[]")))
		rejects(t, m, s, done(a, 10, 11, 200, wire(served(post(3_000_000_001, "alice", "m1"), 1, 10))), "want none")
	}
	rejects(t, m, s, done(Read(false, 3_000_000_001, 0, 10), 10, 11, 500, nil), "want 200")
}

func (c Call) withError(err string) Call {
	c.Err, c.Status, c.Body = err, 0, nil
	return c
}

// TestFailedWritesArePossiblyApplied: a compose that failed may or may not show, at any later
// read.
func TestFailedWritesArePossiblyApplied(t *testing.T) {
	m := NewModel(nil, time.Second)
	s := NewState()
	apply(t, m, s, writeOK(Register(3_000_000_001, "alice"), 0, 1))
	a := post(3_000_000_001, "alice", "m1")
	m.Record(s, done(a, 10, 20, 0, nil).withError("timeout"))
	accepts(t, m, s, read(false, a.User, 0, 10, 100000, 100001))
	accepts(t, m, s, read(false, a.User, 0, 10, 100000, 100001, served(a, 7, 15)))
}

// TestPendingWritesBuildTheNextRequest: generation sees the latest edge state sent.
func TestPendingWritesBuildTheNextRequest(t *testing.T) {
	m := NewModel(small, time.Second)
	s := NewState()
	u, v := unfollowedPair()
	m.Record(s, Call{Action: Follow(u, v, true), Pending: true})
	if !m.Follows(s, u, v) {
		t.Fatal("a pending follow is not the latest state")
	}
	m.Record(s, Call{Action: Follow(u, v, false), Pending: true})
	if m.Follows(s, u, v) {
		t.Fatal("a pending unfollow is not the latest state")
	}
}
