package oracle

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"vibesys/social-network-k8s-evaluator/internal/checker"
	"vibesys/social-network-k8s-evaluator/internal/fakeapp"
)

type earlierHook struct{ app *fakeapp.App }

func (h earlierHook) Restart(_ context.Context, deployment string) error {
	h.app.Restart(deployment)
	return nil
}

func (h earlierHook) Flush(_ context.Context, cache string) error {
	h.app.Flush(cache)
	return nil
}

func failing(names func(int) (string, bool), n int) []string {
	var out []string
	for i := 0; i < n; i++ {
		if name, passed := names(i); !passed {
			out = append(out, strings.Fields(name)[0])
		}
	}
	return out
}

// TestTheOracleCoversTheEarlierChecker runs the earlier scenario checker (internal/checker)
// and the oracle's checks against the same applications: both pass the correct one, and every
// fault the earlier checker caught the oracle catches too.
func TestTheOracleCoversTheEarlierChecker(t *testing.T) {
	cases := []struct {
		name    string
		options fakeapp.Options
	}{
		{"correct, fan-out within the bound", fakeapp.Options{FanoutDelay: testBound * 3 / 4}},
		{"never-invalidating cache", fakeapp.Options{StaleCache: true}},
		{"one stale replica", fakeapp.Options{StaleReplica: true}},
		{"fan-out beyond the bound", fakeapp.Options{FanoutDelay: testBound * 2}},
		{"unfollow ignored", fakeapp.Options{IgnoreUnfollow: true}},
		{"home timelines lost on flush", fakeapp.Options{LoseHomeOnFlush: true}},
		{"posts lost on restart", fakeapp.Options{LosePostsOnRestart: true}},
		{"partial follower set after a flush", fakeapp.Options{FollowerCache: true}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			app := fakeapp.New(testData, test.options)
			server := httptest.NewServer(app)
			defer server.Close()
			earlier, err := checker.Run(context.Background(), checker.Config{
				BaseURL: server.URL, Seed: 7, Hook: earlierHook{app}, Deployments: []string{"nginx-thrift", "home-timeline-service"},
				Caches: []string{"post-storage-memcached", "social-graph-redis"}, Dataset: testData, Bound: testBound,
				Replicas: 4, FollowAfterFlush: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			app2 := fakeapp.New(testData, test.options)
			server2 := httptest.NewServer(app2)
			defer server2.Close()
			now, err := RunChecks(context.Background(), Config{
				BaseURL: server2.URL, Seed: 7, Hook: earlierHook{app2}, Deployments: []string{"nginx-thrift", "home-timeline-service"},
				Caches: []string{"post-storage-memcached", "social-graph-redis"}, Dataset: testData, Bound: testBound,
				Replicas: 4, FollowAfterFlush: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			old := failing(func(i int) (string, bool) { return earlier[i].Name, earlier[i].Passed }, len(earlier))
			fresh := failing(func(i int) (string, bool) { return now[i].Name, now[i].Passed }, len(now))
			t.Logf("earlier checker fails %v; oracle fails %v", old, fresh)
			if (len(old) == 0) != (len(fresh) == 0) {
				t.Fatalf("the earlier checker fails %v, the oracle %v", old, fresh)
			}
		})
	}
}
