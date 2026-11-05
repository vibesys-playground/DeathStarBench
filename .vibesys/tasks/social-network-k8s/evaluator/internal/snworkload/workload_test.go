package snworkload

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/workload"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

func newModel(t *testing.T) *Model {
	t.Helper()
	m, err := NewModel(dataset.Default(), workload.NewRand(7, "model"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func operation(name string) workload.Operation[*Model] {
	for _, op := range Workload().Operations {
		if op.Name == name {
			return op
		}
	}
	panic(name)
}

func infoOf(t *testing.T, request workload.Request) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(request.Info)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

var t0 = time.Unix(2_000_000_000, 0)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

// TestRequestDistribution measures, over 100k operations of the mix, how often a timeline
// request repeats one already made (the hit rate of an unbounded cache keyed by the exact
// request); the earlier adapter's was 66.7% (user timelines 77%, home timelines 57%).
func TestRequestDistribution(t *testing.T) {
	m := newModel(t)
	ops := Workload().Operations
	weights := make([]float64, len(ops))
	for i, op := range ops {
		weights[i] = op.Weight
	}
	stream := workload.NewRand(1, "phase", 0)
	seen := map[string]bool{}
	hits, reads := 0, 0
	byKind := map[string][2]int{}
	pages := map[int]int{}
	for i := 0; i < 100000; i++ {
		op := ops[stream.Weighted(weights)]
		request := op.Build(m, stream.Child(i))
		info := infoOf(t, request)
		var key string
		switch op.Name {
		case opUserTimeline, opHomeTimeline:
			var r readInfo
			_ = json.Unmarshal(info, &r)
			key = op.Name + strconv.Itoa(int(r.User)) + "/" + strconv.Itoa(r.Start)
			pages[r.Start/pageSize]++
		case opCompose:
			var c composeInfo
			_ = json.Unmarshal(info, &c)
			key = opUserTimeline + strconv.Itoa(int(c.Author)) + "/0"
		}
		if op.Apply != nil {
			if err := op.Apply(m, workload.Call{Info: info, Pending: true}); err != nil {
				t.Fatal(err)
			}
		}
		if key == "" {
			continue
		}
		reads++
		counts := byKind[op.Name]
		counts[0]++
		if seen[key] {
			hits++
			counts[1]++
		}
		byKind[op.Name] = counts
		seen[key] = true
	}
	rate := float64(hits) / float64(reads)
	t.Logf("exact-request cache hit rate: %.3f of %d timeline requests; pages %v", rate, reads, pages)
	for name, counts := range byKind {
		t.Logf("  %s: %d requests, hit rate %.3f", name, counts[0], float64(counts[1])/float64(counts[0]))
	}
	if rate < 0.6 || rate > 0.75 {
		t.Fatalf("hit rate %.3f, the earlier adapter's was 0.667", rate)
	}
}

// TestComposeIsCheckedByTheOracle: a compose's Apply records the post and its Check requires
// it in the author's timeline read right after (read your write).
func TestComposeIsCheckedByTheOracle(t *testing.T) {
	m := newModel(t)
	op := operation(opCompose)
	request := op.Build(m, workload.NewRand(3, "phase", 0).Child(0))
	info := infoOf(t, request)
	if err := op.Apply(m, workload.Call{Info: info, Pending: true}); err != nil {
		t.Fatal(err)
	}
	if m.nextSeq != 1 || m.State().Posts() != 1 {
		t.Fatalf("pending apply: seq %d, posts %d", m.nextSeq, m.State().Posts())
	}
	// The check phase: a fresh model with the write's outcome.
	m = newModel(t)
	a, _, err := m.composeOf(workload.Call{Info: info})
	if err != nil {
		t.Fatal(err)
	}
	author := m.data.UserIndex(a.User)
	served := timeline.Post{PostID: "99000001", CreatorID: strconv.FormatInt(a.User, 10), Creator: a.Name, ReqID: "5",
		Text: a.Text, Timestamp: at(5).UnixMilli(), Media: a.Media}
	for _, v := range a.Mentions {
		served.Mentions = append(served.Mentions, timeline.Mention{UserID: strconv.FormatInt(v, 10), Username: m.data.Users[v-1].Username})
	}
	if a.URL != "" {
		served.URLs = []timeline.URL{{Shortened: "http://short-url/abc", Expanded: a.URL}}
		served.Text = strings.TrimSuffix(a.Text, a.URL) + "http://short-url/abc"
	}
	page := []timeline.Post{served}
	for _, p := range m.data.UserPosts.Row(author) {
		if len(page) == pageSize {
			break
		}
		page = append(page, timeline.Seeded(m.data, p))
	}
	call := workload.Call{Info: info, Start: at(0), End: at(10), OK: true, Responses: []workload.Response{
		{Status: 200, Body: []byte("Successfully upload post")}, {Status: 200, Body: encode(page)},
	}}
	if err := op.Apply(m, call); err != nil {
		t.Fatal(err)
	}
	if err := op.Check(m, call); err != nil {
		t.Fatalf("a correct compose: %v", err)
	}
	m = newModel(t)
	if err := op.Apply(m, call); err != nil {
		t.Fatal(err)
	}
	stale := call
	stale.Responses = []workload.Response{call.Responses[0], {Status: 200, Body: encode(page[1:])}}
	if err := op.Check(m, stale); err == nil || !strings.Contains(err.Error(), "read your write") {
		t.Fatalf("a compose missing from the read after it: %v", err)
	}
	refused := call
	refused.Responses = []workload.Response{{Status: 500, Body: []byte("error")}, call.Responses[1]}
	if err := op.Check(m, refused); err == nil {
		t.Fatal("a refused compose passed")
	}
}

// TestEdgesBuildOnPendingWrites: an unfollow is drawn among the edges the pending follows and
// unfollows left, and holds the edge's exclusive key.
func TestEdgesBuildOnPendingWrites(t *testing.T) {
	m := newModel(t)
	follow, unfollow := operation(opFollow), operation(opUnfollow)
	stream := workload.NewRand(5, "phase", 0)
	for i := 0; i < 2000; i++ {
		op := follow
		if i%2 == 1 {
			op = unfollow
		}
		request := op.Build(m, stream.Child(i))
		var e edgeInfo
		if err := json.Unmarshal(infoOf(t, request), &e); err != nil {
			t.Fatal(err)
		}
		if m.follows(e.U, e.V) == e.On {
			t.Fatalf("operation %d changes edge %d -> %d to its current state %v", i, e.U, e.V, e.On)
		}
		if request.Exclusive != "edge:"+strconv.Itoa(int(e.U))+":"+strconv.Itoa(int(e.V)) {
			t.Fatalf("exclusive key %q", request.Exclusive)
		}
		if err := op.Apply(m, workload.Call{Info: infoOf(t, request), Pending: true}); err != nil {
			t.Fatal(err)
		}
		if m.follows(e.U, e.V) != e.On {
			t.Fatal("the pending change is not the latest state")
		}
	}
}

func encode(posts []timeline.Post) []byte {
	list := make([]map[string]any, len(posts))
	for i, p := range posts {
		empty := func(n int, item func(int) any) any {
			if n == 0 {
				return map[string]any{}
			}
			out := make([]any, n)
			for j := range out {
				out[j] = item(j)
			}
			return out
		}
		list[i] = map[string]any{
			"post_id": p.PostID, "req_id": p.ReqID, "text": p.Text, "timestamp": strconv.FormatInt(p.Timestamp, 10), "post_type": 0,
			"creator": map[string]any{"user_id": p.CreatorID, "username": p.Creator},
			"user_mentions": empty(len(p.Mentions), func(j int) any {
				return map[string]any{"user_id": p.Mentions[j].UserID, "username": p.Mentions[j].Username}
			}),
			"media": empty(len(p.Media), func(j int) any { return map[string]any{"media_id": p.Media[j].ID, "media_type": p.Media[j].Type} }),
			"urls": empty(len(p.URLs), func(j int) any {
				return map[string]any{"shortened_url": p.URLs[j].Shortened, "expanded_url": p.URLs[j].Expanded}
			}),
		}
	}
	encoded, _ := json.Marshal(list)
	return encoded
}
