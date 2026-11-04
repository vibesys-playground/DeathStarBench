package socialnetwork

import (
	"math/rand"
	"net/url"
	"testing"

	"vibesys/microservice-evaluator/api"
)

// TestRequestDistribution measures, over 100k samples of the task's mix, how
// often a timeline request repeats one already made (the hit rate of an
// unbounded cache keyed by the exact request), and the skew of the users.
func TestRequestDistribution(t *testing.T) {
	application, err := NewWithJournal(testWorkload("http://unused", 1, 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	application.namespace = "mtest"
	weights := []struct {
		name   string
		weight int
	}{{opUserTimeline, 500}, {opHomeTimeline, 400}, {opCompose, 100}, {opFollow, 5}, {opUnfollow, 5}}
	total := 0
	for _, w := range weights {
		total += w.weight
	}
	random := rand.New(rand.NewSource(1))
	seen := map[string]bool{}
	hits, reads := 0, 0
	readsByKind, hitsByKind := map[string]int{}, map[string]int{}
	users := map[string]map[int32]int{}
	pages := map[int]int{}
	const samples = 100000
	for counter := 0; counter < samples; counter++ {
		draw := random.Intn(total)
		name := ""
		for _, w := range weights {
			if draw < w.weight {
				name = w.name
				break
			}
			draw -= w.weight
		}
		built, err := application.BuildOperation(api.Operation{Name: name, Target: gateway}, api.Sample{Counter: int64(counter), Random: random.Uint64()}, application)
		if err != nil {
			t.Fatal(err)
		}
		state := built.State.(*plan)
		if users[name] == nil {
			users[name] = map[int32]int{}
		}
		users[name][state.user]++
		for _, invocation := range built.Invocations {
			request := invocation.Payload.(api.HTTPRequestSpec)
			if request.Method != "GET" {
				continue
			}
			query := url.Values{}
			for key, value := range request.Query {
				query.Set(key, value)
			}
			if name != opCompose {
				pages[atoi(query.Get("start"))/PageSize]++
			}
			key := request.Path + "?" + query.Encode()
			reads++
			readsByKind[name]++
			if seen[key] {
				hits++
				hitsByKind[name]++
			}
			seen[key] = true
		}
		application.FinishOperation(built)
	}
	rate := float64(hits) / float64(reads)
	t.Logf("exact-request cache hit rate over %d samples: %.3f (%d of %d timeline requests); distinct requests %d", samples, rate, hits, reads, len(seen))
	for name, count := range readsByKind {
		t.Logf("  %s: %d requests, hit rate %.3f", name, count, float64(hitsByKind[name])/float64(count))
	}
	for name, counts := range users {
		top := 0
		for _, count := range counts {
			if count > top {
				top = count
			}
		}
		t.Logf("  %s: %d distinct users, top user %.1f%%", name, len(counts), 100*float64(top)/float64(sumValues(counts)))
	}
	t.Logf("  pages %v", pages)
	if rate < 0.3 || rate > 0.9 {
		t.Fatalf("hit rate %.3f outside the expected range", rate)
	}
}

func atoi(text string) int {
	value := 0
	for _, c := range text {
		value = value*10 + int(c-'0')
	}
	return value
}

func sumValues(counts map[int32]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}
