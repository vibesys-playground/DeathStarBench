package socialnetwork

import (
	"testing"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
)

// status is how a live post appears among user's home candidates at t:
// "must", "may" or "no".
func status(m *Model, user int32, p *livePost, t time.Time) string {
	for _, c := range m.homeCandidates(user, t) {
		if c.live == p {
			if c.must {
				return "must"
			}
			return "may"
		}
	}
	return "no"
}

func TestModelFollowRules(t *testing.T) {
	data := dataset.Generate(5, dataset.Config{Users: 200, FolloweeScale: 4, MaxFollowees: 50, Reciprocity: 0, PostScale: 2, MaxPosts: 10})
	m := newModel(data, time.Second)
	// Find a pair u, v with u not following v.
	var u, v int32 = 0, 1
	for data.Follows(int(u), int(v)) || data.Follows(int(v), int(u)) {
		v++
	}
	base := time.Unix(2_000_000_000, 0)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	post := func(sent, acked int) *livePost {
		p := newLivePost(data, v, uint64(sent)*7919, int64(sent), "mtest"+string(rune('a'+len(m.posts))))
		p.mentions = nil
		p.sentAt = at(sent)
		m.beginPost(p)
		m.endPost(p, true, at(acked))
		return p
	}
	before := post(0, 10)
	if got := status(m, u, before, at(5000)); got != "no" {
		t.Fatalf("post of an unfollowed author: %s", got)
	}
	follow := m.beginChange(u, v, true, at(1000))
	m.endChange(follow, true, at(1100))
	during := post(1500, 1600) // within the bound after the follow
	after := post(2200, 2300)  // more than the bound after the follow
	if got := status(m, u, before, at(5000)); got != "may" {
		t.Fatalf("pre-follow post after the follow: %s, want may (pull designs)", got)
	}
	if got := status(m, u, during, at(5000)); got != "may" {
		t.Fatalf("post composed within the bound after the follow: %s", got)
	}
	if got := status(m, u, after, at(3400)); got != "must" {
		t.Fatalf("post composed after the follow settled: %s", got)
	}
	if got := status(m, u, after, at(3000)); got != "may" {
		t.Fatalf("post read within the bound of its ack: %s", got)
	}
	unfollow := m.beginChange(u, v, false, at(4000))
	if got := status(m, u, after, at(4500)); got != "may" {
		t.Fatalf("post during an unfollow in flight: %s", got)
	}
	m.endChange(unfollow, true, at(4100))
	late := post(5200, 5300)
	if got := status(m, u, late, at(7000)); got != "no" {
		t.Fatalf("post composed after the unfollow settled: %s", got)
	}
	if got := status(m, u, after, at(7000)); got != "may" {
		t.Fatalf("post composed while followed, read after the unfollow: %s", got)
	}
	// Mentions deliver regardless of the edge.
	mention := newLivePost(data, v, 1, 99, "mtestmention")
	mention.mentions = []int32{u}
	mention.sentAt = at(6000)
	m.beginPost(mention)
	m.endPost(mention, true, at(6100))
	if got := status(m, u, mention, at(7200)); got != "must" {
		t.Fatalf("mention: %s", got)
	}
	// A failed write leaves the edge uncertain until a later change settles.
	failed := m.beginChange(u, v, true, at(8000))
	m.endChange(failed, false, at(8100))
	unknown := post(10000, 10100)
	if got := status(m, u, unknown, at(12000)); got != "may" {
		t.Fatalf("post after a failed follow: %s", got)
	}
}
