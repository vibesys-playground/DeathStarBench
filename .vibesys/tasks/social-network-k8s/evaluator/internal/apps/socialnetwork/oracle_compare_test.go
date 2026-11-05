package socialnetwork

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/oracle"
)

// TestOracleAgreesWithTheEarlierModel compares, on synthesized histories of one follow edge
// and the followee's posts, how this adapter's model and the oracle place each post in the
// follower's home timeline (must, may, excluded) at many read times. They agree except in
// the cases where the oracle reads the contract differently, each counted by its reason; any
// other difference fails the test.
func TestOracleAgreesWithTheEarlierModel(t *testing.T) {
	data := dataset.Generate(5, dataset.Config{Users: 200, FolloweeScale: 4, MaxFollowees: 50, Reciprocity: 0, PostScale: 2, MaxPosts: 10})
	base := time.Unix(2_000_000_000, 0)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	reasons := map[string]int{}
	compared := 0
	for history := 0; history < 400; history++ {
		r := rand.New(rand.NewSource(int64(history)))
		old := newModel(data, time.Second)
		om := oracle.NewModel(data, time.Second)
		state := oracle.NewState()
		// A pair u -> v, followed in the dataset half of the time.
		var u, v int32
		for {
			u, v = int32(r.Intn(200)), int32(r.Intn(200))
			if u != v && data.Follows(int(u), int(v)) == (history%2 == 0) && !data.Follows(int(v), int(u)) {
				break
			}
		}
		type change struct {
			on           bool
			sent, acked  int
			ok           bool
			earlierState bool
		}
		// Sequential follow changes (never two in flight) and composes, at random times.
		var posts []*livePost
		marker := 0
		clock := 0
		following := data.Follows(int(u), int(v))
		var events []func()
		for clock < 20000 {
			clock += 50 + r.Intn(2500)
			sent, acked := clock, clock+5+r.Intn(300)
			if r.Intn(3) == 0 {
				on := !following
				if r.Intn(10) == 0 {
					on = following // a change that changes nothing
				}
				ok := r.Intn(20) != 0
				following = on
				c := old.beginChange(u, v, on, at(sent))
				old.endChange(c, ok, at(acked))
				call := oracle.Call{Action: oracle.Follow(int64(u)+1, int64(v)+1, on), Start: at(sent), End: at(acked), Status: 200, Body: []byte("Success")}
				if !ok {
					call.Status, call.Body = 500, nil
				}
				om.Record(state, call)
				clock = acked
				continue
			}
			marker++
			p := newLivePost(data, v, uint64(r.Int63()), int64(marker), fmt.Sprintf("mc%dp%d", history, marker))
			if r.Intn(10) == 0 {
				p.mentions = []int32{u}
			} else {
				p.mentions = nil
			}
			p.sentAt = at(sent)
			old.beginPost(p)
			ok := r.Intn(20) != 0
			old.endPost(p, ok, at(acked))
			mentions := []int64{}
			for _, m := range p.mentions {
				mentions = append(mentions, int64(m)+1)
			}
			call := oracle.Call{Action: oracle.Action{Kind: oracle.KindCompose, User: int64(v) + 1, Name: data.Users[v].Username,
				Marker: p.marker, Text: p.text, URL: p.url, Mentions: mentions, Media: p.media},
				Start: at(sent), End: at(acked), Status: 200, Body: []byte("Successfully upload post")}
			if !ok {
				call.Status, call.Body = 0, nil
				call.Err = "timeout"
			}
			om.Record(state, call)
			posts = append(posts, p)
		}
		_ = events
		for read := 0; read < 60; read++ {
			rs := r.Intn(clock + 3000)
			re := rs + 1 + r.Intn(20)
			for _, c := range old.userCandidates(v, at(rs)) {
				if c.live == nil {
					continue
				}
				oldVis := oracle.May
				if c.must {
					oldVis = oracle.Must
				}
				newVis := om.Visibility(state, false, int64(v)+1, c.live.marker, at(rs), at(re))
				compared++
				switch {
				case oldVis == newVis:
				case !c.live.sentAt.Before(at(re)) && newVis == oracle.Excluded:
					reasons["user timeline: the compose was sent after the read ended (earlier may, oracle excluded)"]++
				default:
					t.Fatalf("user timeline, history %d read [%d, %d]: %s earlier %v, oracle %v", history, rs, re, c.live.marker, oldVis, newVis)
				}
			}
			candidates := old.homeCandidates(u, at(rs))
			for _, p := range posts {
				oldVis := oracle.Excluded
				for _, c := range candidates {
					if c.live == p {
						oldVis = oracle.May
						if c.must {
							oldVis = oracle.Must
						}
					}
				}
				newVis := om.Visibility(state, true, int64(u)+1, p.marker, at(rs), at(re))
				compared++
				if oldVis == newVis {
					continue
				}
				// Why the two differ.
				e := old.edges[edgeKey(u, v)]
				changeAfterPost, changeDuringRead := false, false
				if e != nil {
					for _, c := range e.changes {
						if !c.sentAt.Before(p.sentAt) && c.sentAt.Before(at(re)) {
							changeAfterPost = true
							if !c.sentAt.Before(at(rs)) {
								changeDuringRead = true
							}
						}
					}
				}
				changeAfterRead := false
				if e != nil {
					for _, c := range e.changes {
						if !c.sentAt.Before(at(re)) {
							changeAfterRead = true
						}
					}
				}
				failedBefore := false
				if e != nil {
					for _, c := range e.changes {
						if c.failed && c.sentAt.Before(p.sentAt) {
							failedBefore = true
						}
					}
				}
				boundary := false
				if e != nil {
					for _, c := range e.changes {
						for _, t := range []time.Time{p.sentAt, at(rs)} {
							if c.ackedAt.Add(time.Second).Equal(t) {
								boundary = true
							}
						}
					}
				}
				var reason string
				switch {
				case !p.sentAt.Before(at(re)) && newVis == oracle.Excluded:
					reason = "the compose was sent after the read ended: the earlier model allowed it, the oracle excludes it"
				case changeDuringRead && newVis == oracle.May:
					reason = "a change of the edge was sent while the read ran: the earlier model ignored it, the oracle allows either"
				case changeAfterPost && newVis == oracle.May:
					reason = "the edge changed after the post was composed: the earlier model decided by the edge's state at the read, the oracle allows either (rule 4)"
				case changeAfterRead && oldVis == oracle.May && newVis == oracle.Excluded:
					reason = "a change of the edge was sent after the read ended: the earlier model let it make the post optional, the oracle ignores it"
				case failedBefore && newVis == oracle.May:
					reason = "a change of the edge failed before the post: the oracle counts it possibly applied at any later time, the earlier model only until the next change"
				case boundary:
					reason = "boundary: an acknowledgement exactly the bound before"
				default:
					t.Fatalf("history %d read [%d, %d] ms: post %s (sent %v acked %v failed %v mentions %v) earlier %v, oracle %v; changes %+v",
						history, rs, re, p.marker, p.sentAt.Sub(base), p.ackedAt.Sub(base), p.failed, p.mentions, oldVis, newVis, describeChanges(e, base))
				}
				reasons[reason+fmt.Sprintf(" (earlier %v, oracle %v)", oldVis, newVis)]++
			}
		}
	}
	keys := make([]string, 0, len(reasons))
	for key := range reasons {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	differ := 0
	for _, key := range keys {
		differ += reasons[key]
		t.Logf("%6d  %s", reasons[key], key)
	}
	t.Logf("%d placements compared, %d differ (%.1f%%)", compared, differ, 100*float64(differ)/float64(compared))
}

func describeChanges(e *edge, base time.Time) string {
	if e == nil {
		return "none"
	}
	out := ""
	for _, c := range e.changes {
		out += fmt.Sprintf("[on %v sent %v acked %v failed %v] ", c.on, c.sentAt.Sub(base), c.ackedAt.Sub(base), c.failed)
	}
	return out
}
