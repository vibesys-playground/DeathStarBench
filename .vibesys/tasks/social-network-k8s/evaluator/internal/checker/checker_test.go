package checker

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/fakeapp"
)

type fakeHook struct {
	app      *fakeapp.App
	restarts []string
	flushes  []string
}

func (h *fakeHook) Restart(_ context.Context, deployment string) error {
	h.restarts = append(h.restarts, deployment)
	h.app.Restart(deployment)
	return nil
}

func (h *fakeHook) Flush(_ context.Context, cache string) error {
	h.flushes = append(h.flushes, cache)
	h.app.Flush(cache)
	return nil
}

var testData = dataset.Generate(11, dataset.Config{Users: 500, FolloweeScale: 6, MaxFollowees: 100, Reciprocity: 0.2, PostScale: 4, MaxPosts: 60})

// The fake's faults are scaled to a 200 ms bound to keep the tests short.
const testBound = 200 * time.Millisecond

func runChecker(t *testing.T, options fakeapp.Options) ([]Result, *fakeHook) {
	t.Helper()
	app := fakeapp.New(testData, options)
	server := httptest.NewServer(app)
	t.Cleanup(server.Close)
	hook := &fakeHook{app: app}
	results, err := Run(context.Background(), Config{
		BaseURL: server.URL, Seed: 7, Hook: hook, Deployments: []string{"nginx-thrift", "home-timeline-service"},
		Caches: []string{"post-storage-memcached", "social-graph-redis"}, Dataset: testData, Bound: testBound,
		Replicas: 4, FollowAfterFlush: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return results, hook
}

func failed(results []Result) []Result {
	var out []Result
	for _, result := range results {
		if !result.Passed {
			out = append(out, result)
		}
	}
	return out
}

func TestCheckerPassesACorrectApplication(t *testing.T) {
	results, hook := runChecker(t, fakeapp.Options{FanoutDelay: testBound * 3 / 4})
	if bad := failed(results); len(bad) != 0 {
		t.Fatalf("failed checks: %+v", bad)
	}
	if len(results) != 14 || len(hook.restarts) != 2 || len(hook.flushes) != 2 {
		t.Fatalf("results %d, restarts %v, flushes %v", len(results), hook.restarts, hook.flushes)
	}
}

func TestCheckerRejectsFaultyApplications(t *testing.T) {
	tests := []struct {
		name    string
		options fakeapp.Options
		check   string // a check that must fail
	}{
		{"never-invalidating cache", fakeapp.Options{StaleCache: true}, "C3"},
		{"one stale replica", fakeapp.Options{StaleReplica: true}, "C"},
		{"fan-out beyond the bound", fakeapp.Options{FanoutDelay: testBound * 2}, "C7"},
		{"unfollow ignored", fakeapp.Options{IgnoreUnfollow: true}, "C8 "},
		{"home timelines lost on flush", fakeapp.Options{LoseHomeOnFlush: true}, "C13"},
		{"posts lost on restart", fakeapp.Options{LosePostsOnRestart: true}, "C13"},
		// The pinned social graph service's follower cache (see fakeapp).
		{"partial follower set after a flush", fakeapp.Options{FollowerCache: true}, "C13"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results, _ := runChecker(t, test.options)
			bad := failed(results)
			found := false
			for _, result := range bad {
				found = found || strings.HasPrefix(result.Name, test.check)
			}
			if !found {
				t.Fatalf("check %s passed; failures %+v", test.check, bad)
			}
			t.Logf("%d failed, e.g. %s: %s", len(bad), bad[0].Name, bad[0].Detail)
		})
	}
}

// Without the follow-after-flush step the pinned follower cache passes:
// --skip-follow-after-flush lets a base with that gap pass the rest.
func TestFollowAfterFlushIsTheOnlyFollowerCacheFailure(t *testing.T) {
	app := fakeapp.New(testData, fakeapp.Options{FollowerCache: true})
	server := httptest.NewServer(app)
	defer server.Close()
	results, err := Run(context.Background(), Config{
		BaseURL: server.URL, Seed: 8, Hook: &fakeHook{app: app}, Deployments: []string{"nginx-thrift"},
		Caches: []string{"social-graph-redis"}, Dataset: testData, Bound: testBound, Replicas: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bad := failed(results); len(bad) != 0 {
		t.Fatalf("failed checks: %+v", bad)
	}
}
