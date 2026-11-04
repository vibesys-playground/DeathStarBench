// Package checker is the task's accuracy check of a Social Network
// deployment, over the public API only: the intent of the old task's C1-C11
// checks, the freshness bound across replicas, the seeded dataset, and
// durability across restarts of stateless pods and cache flushes.
package checker

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// Hook restarts one pod of a deployment or flushes a cache.
type Hook interface {
	Restart(ctx context.Context, deployment string) error
	Flush(ctx context.Context, cache string) error
}

// Config configures a run.
type Config struct {
	BaseURL     string
	Seed        int64
	Hook        Hook
	Deployments []string
	Caches      []string
	Dataset     *dataset.Dataset // nil: skip the dataset checks
	Bound       time.Duration
	Replicas    int // reads per check, to reach every replica
	// FollowAfterFlush also follows a user right after a cache flush,
	// before anything reads the flushed follower set.
	FollowAfterFlush bool
	Transport        http.RoundTripper // tests
	Log              func(format string, args ...any)
}

// Result is one check's outcome.
type Result struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

// Default lists.
var (
	DefaultDeployments = []string{
		"nginx-thrift", "compose-post-service", "home-timeline-service", "media-service", "post-storage-service",
		"social-graph-service", "text-service", "unique-id-service", "url-shorten-service", "user-mention-service",
		"user-service", "user-timeline-service",
	}
	DefaultCaches = []string{
		"post-storage-memcached", "user-memcached", "url-shorten-memcached", "media-memcached",
		"user-timeline-redis", "social-graph-redis",
	}
)

type suite struct {
	config  Config
	w       *world
	results []Result
	// durability fixture: d[1], d[2] follow d[0]; d[3] mentions d[1].
	d []*user
}

// Run runs every check and returns the results; err is a setup failure.
func Run(ctx context.Context, config Config) ([]Result, error) {
	if config.Replicas <= 0 {
		config.Replicas = 12
	}
	if config.Log == nil {
		config.Log = func(string, ...any) {}
	}
	random := rand.New(rand.NewSource(config.Seed))
	// Ids and names far from the dataset's (1..20000) and unique per seed.
	base := int64(3_000_000_000) + (config.Seed&0x7fffffff%1_000_000)*1000
	s := &suite{config: config, w: &world{
		c: newClient(config.BaseURL, 30*time.Second, config.Transport), bound: config.Bound, replicas: config.Replicas,
		random: random, prefix: "chk" + strconv.FormatInt(base/1000%1_000_000, 36), nextID: base, posts: map[string]*timeline.Post{},
	}}
	if _, err := s.w.c.readRaw(ctx, false, "1", "0", "1"); err != nil {
		return nil, fmt.Errorf("gateway unreachable: %w", err)
	}
	checks := []struct {
		name string
		run  func(context.Context) error
	}{
		{"C1 user-timeline schema and content", s.userTimelineSchema},
		{"C2 home-timeline schema and content", s.homeTimelineSchema},
		{"C3 user timeline newest first", s.userTimelineOrder},
		{"C4 home timeline newest first", s.homeTimelineOrder},
		{"C5 home timeline holds followed and mentioned users only", s.visibility},
		{"C6 read your write on every replica", s.readYourWrite},
		{"C7 fan-out within the bound on every replica", s.fanout},
		{"C8 unfollow stops fan-out of later posts", s.unfollow},
		{"C8b follow starts fan-out of later posts", s.follow},
		{"C9 invalid requests do not change state", s.invalidRequests},
		{"C10 pages are disjoint windows of one timeline", s.pagination},
		{"C11 burst reads agree; URLs, mentions and media round trip", s.burstAndContent},
		{"C12 seeded dataset served exactly", s.datasetCheck},
		{"C13 durability across restarts and cache flushes", s.durability},
	}
	for _, check := range checks {
		config.Log("running %s", check.name)
		err := check.run(ctx)
		result := Result{Name: check.name, Passed: err == nil}
		if err != nil {
			result.Detail = err.Error()
		}
		s.results = append(s.results, result)
		if ctx.Err() != nil {
			return s.results, ctx.Err()
		}
	}
	return s.results, nil
}

func (s *suite) userTimelineSchema(ctx context.Context) error {
	a, err := s.w.newUser(ctx)
	if err != nil {
		return err
	}
	if _, _, err := s.w.compose(ctx, a, composeOptions{}); err != nil {
		return err
	}
	return s.w.expect(ctx, a, false)
}

func (s *suite) homeTimelineSchema(ctx context.Context) error {
	users, err := s.w.newUsers(ctx, 2)
	if err != nil {
		return err
	}
	a, b := users[0], users[1]
	if err := s.w.follow(ctx, a, b, true); err != nil {
		return err
	}
	_, acked, err := s.w.compose(ctx, b, composeOptions{})
	if err != nil {
		return err
	}
	if err := s.w.settle(ctx, acked); err != nil {
		return err
	}
	return s.w.expect(ctx, a, true)
}

func (s *suite) userTimelineOrder(ctx context.Context) error {
	a, err := s.w.newUser(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < 4; i++ {
		if _, _, err := s.w.compose(ctx, a, composeOptions{}); err != nil {
			return err
		}
	}
	return s.w.expect(ctx, a, false)
}

func (s *suite) homeTimelineOrder(ctx context.Context) error {
	users, err := s.w.newUsers(ctx, 3)
	if err != nil {
		return err
	}
	a := users[0]
	for _, v := range users[1:] {
		if err := s.w.follow(ctx, a, v, true); err != nil {
			return err
		}
	}
	var acked time.Time
	for i := 0; i < 4; i++ {
		if _, acked, err = s.w.compose(ctx, users[1+i%2], composeOptions{}); err != nil {
			return err
		}
	}
	if err := s.w.settle(ctx, acked); err != nil {
		return err
	}
	return s.w.expect(ctx, a, true)
}

func (s *suite) visibility(ctx context.Context) error {
	users, err := s.w.newUsers(ctx, 4)
	if err != nil {
		return err
	}
	a, b, c, mentioned := users[0], users[1], users[2], users[3]
	if err := s.w.follow(ctx, a, b, true); err != nil {
		return err
	}
	if _, _, err := s.w.compose(ctx, b, composeOptions{}); err != nil {
		return err
	}
	other, acked, err := s.w.compose(ctx, c, composeOptions{mentions: []*user{mentioned}})
	if err != nil {
		return err
	}
	if err := s.w.settle(ctx, acked); err != nil {
		return err
	}
	if err := s.w.expect(ctx, a, true); err != nil {
		return err
	}
	if err := s.w.expect(ctx, mentioned, true); err != nil {
		return err
	}
	return s.w.absent(ctx, b, other.PostID)
}

func (s *suite) readYourWrite(ctx context.Context) error {
	a, err := s.w.newUser(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < 3; i++ { // compose checks the first page on every replica
		if _, _, err := s.w.compose(ctx, a, composeOptions{}); err != nil {
			return err
		}
	}
	return s.w.expect(ctx, a, false)
}

// fanout reads every follower's home timeline right at the bound.
func (s *suite) fanout(ctx context.Context) error {
	users, err := s.w.newUsers(ctx, 4)
	if err != nil {
		return err
	}
	author := users[0]
	for _, v := range users[1:] {
		if err := s.w.follow(ctx, v, author, true); err != nil {
			return err
		}
	}
	for round := 0; round < 3; round++ {
		_, acked, err := s.w.compose(ctx, author, composeOptions{})
		if err != nil {
			return err
		}
		if err := s.w.settle(ctx, acked); err != nil {
			return err
		}
		for _, v := range users[1:] {
			if err := s.w.expect(ctx, v, true); err != nil {
				return fmt.Errorf("%v (read %v after the acknowledgement)", err, time.Since(acked).Round(time.Millisecond))
			}
		}
	}
	return nil
}

func (s *suite) unfollow(ctx context.Context) error {
	users, err := s.w.newUsers(ctx, 2)
	if err != nil {
		return err
	}
	a, b := users[0], users[1]
	if err := s.w.follow(ctx, a, b, true); err != nil {
		return err
	}
	_, acked, err := s.w.compose(ctx, b, composeOptions{})
	if err != nil {
		return err
	}
	if err := s.w.settle(ctx, acked); err != nil {
		return err
	}
	if err := s.w.expect(ctx, a, true); err != nil {
		return fmt.Errorf("before the unfollow: %w", err)
	}
	if err := s.w.follow(ctx, a, b, false); err != nil {
		return err
	}
	later, acked, err := s.w.compose(ctx, b, composeOptions{})
	if err != nil {
		return err
	}
	if err := s.w.settle(ctx, acked); err != nil {
		return err
	}
	return s.w.absent(ctx, a, later.PostID)
}

func (s *suite) follow(ctx context.Context) error {
	users, err := s.w.newUsers(ctx, 2)
	if err != nil {
		return err
	}
	a, b := users[0], users[1]
	if _, _, err := s.w.compose(ctx, b, composeOptions{}); err != nil {
		return err
	}
	if err := s.w.follow(ctx, a, b, true); err != nil {
		return err
	}
	later, acked, err := s.w.compose(ctx, b, composeOptions{})
	if err != nil {
		return err
	}
	if err := s.w.settle(ctx, acked); err != nil {
		return err
	}
	// The post before the follow may or may not be there; the later one must.
	for attempt := 0; attempt < s.w.replicas; attempt++ {
		page, err := s.w.c.read(ctx, true, a.id, 0, 10)
		if err != nil {
			return err
		}
		if len(page) == 0 || page[0].PostID != later.PostID || !timeline.Same(later, &page[0]) {
			return fmt.Errorf("home timeline of %s (read %d) is %s, want %s first", a.name, attempt+1, describeAll(page), timeline.Describe(later))
		}
	}
	return nil
}

func (s *suite) invalidRequests(ctx context.Context) error {
	a, err := s.w.newUser(ctx)
	if err != nil {
		return err
	}
	if _, _, err := s.w.compose(ctx, a, composeOptions{}); err != nil {
		return err
	}
	unknown := strconv.FormatInt(s.w.nextID+999, 10)
	id := strconv.FormatInt(a.id, 10)
	for _, args := range [][4]string{
		{"user", unknown, "0", "10"}, {"home", unknown, "0", "10"}, {"user", id, "5", "2"},
		{"home", id, "-1", "3"}, {"user", id, "x", "y"}, {"user", "", "0", "10"},
	} {
		reply, err := s.w.c.readRaw(ctx, args[0] == "home", args[1], args[2], args[3])
		if err != nil {
			return err
		}
		if reply.status == http.StatusOK {
			if posts, err := timeline.Parse(reply.body); err != nil || len(posts) != 0 {
				return fmt.Errorf("read %v answered %d posts (%v), want none", args, len(posts), err)
			}
		}
	}
	// A compose without text is rejected and leaves no post.
	if err := s.w.c.compose(ctx, a, "", nil); err == nil {
		return fmt.Errorf("compose without text was acknowledged")
	}
	if err := s.w.expect(ctx, a, false); err != nil {
		return err
	}
	return s.w.expect(ctx, a, true)
}

func (s *suite) pagination(ctx context.Context) error {
	a, err := s.w.newUser(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < 7; i++ {
		if _, _, err := s.w.compose(ctx, a, composeOptions{}); err != nil {
			return err
		}
	}
	for _, window := range [][2]int{{0, 3}, {3, 6}, {6, 9}, {2, 5}, {5, 7}, {7, 10}} {
		page, err := s.w.c.read(ctx, false, a.id, window[0], window[1])
		if err != nil {
			return err
		}
		end := min(window[1], len(a.posts))
		var want []string
		if window[0] < end {
			want = a.posts[window[0]:end]
		}
		if err := s.w.matches(page, want); err != nil {
			return fmt.Errorf("page %v: %v", window, err)
		}
	}
	return nil
}

func (s *suite) burstAndContent(ctx context.Context) error {
	users, err := s.w.newUsers(ctx, 3)
	if err != nil {
		return err
	}
	a, b, c := users[0], users[1], users[2]
	if err := s.w.follow(ctx, a, b, true); err != nil {
		return err
	}
	media := []timeline.Media{{ID: "12345678901234567", Type: "png"}, {ID: "22345678901234567", Type: "jpg"}}
	if _, _, err := s.w.compose(ctx, b, composeOptions{url: true}); err != nil {
		return err
	}
	if _, _, err := s.w.compose(ctx, b, composeOptions{mentions: []*user{c}, media: media}); err != nil {
		return err
	}
	_, acked, err := s.w.compose(ctx, b, composeOptions{mentions: []*user{a, c}, url: true})
	if err != nil {
		return err
	}
	if err := s.w.settle(ctx, acked); err != nil {
		return err
	}
	for _, u := range users {
		if err := s.w.expect(ctx, u, true); err != nil {
			return err
		}
	}
	return s.w.expect(ctx, b, false)
}

// datasetCheck reads seeded timelines of a sample of users.
func (s *suite) datasetCheck(ctx context.Context) error {
	data := s.config.Dataset
	if data == nil {
		return nil
	}
	sample := s.datasetSample(60)
	for _, u := range sample {
		for _, home := range []bool{false, true} {
			if err := s.expectSeeded(ctx, data, u, home, s.w.random.Intn(3)*10, 1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *suite) datasetSample(n int) []int {
	data := s.config.Dataset
	out := []int{int(data.ActivityRank[0]), int(data.PopularityRank[0])}
	for len(out) < n {
		out = append(out, s.w.random.Intn(len(data.Users)))
	}
	return out
}

func (s *suite) expectSeeded(ctx context.Context, data *dataset.Dataset, u int, home bool, start, reads int) error {
	row := data.UserPosts.Row(u)
	if home {
		row = data.Home.Row(u)
	}
	stop := start + 10
	var want []int32
	if start < len(row) {
		want = row[start:min(stop, len(row))]
	}
	for attempt := 0; attempt < reads; attempt++ {
		page, err := s.w.c.read(ctx, home, data.Users[u].ID, start, stop)
		if err != nil {
			return err
		}
		if len(page) != len(want) {
			return fmt.Errorf("seeded %s of user %d [%d, %d): %d posts %s, want %d", kindName(home), data.Users[u].ID, start, stop, len(page), describeAll(page), len(want))
		}
		for i, index := range want {
			expected := timeline.Seeded(data, index)
			if !timeline.Same(&expected, &page[i]) {
				return fmt.Errorf("seeded %s of user %d [%d, %d): post %d is %s, want %s", kindName(home), data.Users[u].ID, start, stop, i, timeline.Describe(&page[i]), timeline.Describe(&expected))
			}
		}
	}
	return nil
}
