package oracle

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// Config configures the accuracy check (RunChecks).
type Config struct {
	BaseURL     string
	Seed        int64
	Hook        Hook
	Deployments []string
	Caches      []string
	Dataset     *dataset.Dataset // nil: skip the dataset checks (the deployment was written to)
	Bound       time.Duration
	Replicas    int // reads per check, to reach every replica
	// FollowAfterFlush also follows a user right after a cache flush, before anything reads
	// the flushed follower set.
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

// suite generates the check programs: each a scenario over users of its own, run against the
// application and judged by the model (the verifier carries the states from check to check).
type suite struct {
	config   Config
	model    *Model
	verifier *Verifier
	system   *HTTPSystem
	random   *rand.Rand
	prefix   string
	nextID   int64
	sequence int
}

// user is a checker-owned user, as far as the programs need to know it: how many posts its
// timelines may hold (to size the reads) and who follows it.
type user struct {
	id        int64
	name      string
	posts     int
	home      int
	followers map[*user]bool
}

// RunChecks runs every check and returns the results; err is a setup failure.
func RunChecks(ctx context.Context, config Config) ([]Result, error) {
	if config.Replicas <= 0 {
		config.Replicas = 12
	}
	if config.Log == nil {
		config.Log = func(string, ...any) {}
	}
	// Ids and names far from the dataset's (1..20000) and unique per seed.
	base := int64(3_000_000_000) + (config.Seed&0x7fffffff%1_000_000)*1000
	model := NewModel(config.Dataset, config.Bound)
	s := &suite{
		config: config, model: model, verifier: NewVerifier(model, nil),
		system: NewHTTPSystem(config.BaseURL, 30*time.Second, config.Transport, config.Hook),
		random: rand.New(rand.NewSource(config.Seed)), prefix: "chk" + strconv.FormatInt(base/1000%1_000_000, 36), nextID: base,
	}
	if c := s.system.Invoke(ctx, RawRead(false, "1", "0", "1")); c.Err != "" {
		return nil, fmt.Errorf("gateway unreachable: %s", c.Err)
	}
	checks := []struct {
		name  string
		build func(*program)
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
		{"C14 concurrent writes and reads have a legal ordering", s.concurrency},
	}
	var results []Result
	for _, check := range checks {
		config.Log("running %s", check.name)
		p := &program{s: s, id: strings.Fields(check.name)[0]}
		check.build(p)
		err := p.err
		if err == nil {
			_, err = s.verifier.Run(ctx, Program{ID: p.id, Steps: p.steps}, s.system)
		}
		result := Result{Name: check.name, Passed: err == nil}
		if err != nil {
			result.Detail = err.Error()
			var mismatch *Mismatch
			if errors.As(err, &mismatch) {
				config.Log("%s failed at step %d; trace %s", p.id, mismatch.Step, clipTrace(mismatch.Trace()))
			}
		}
		results = append(results, result)
		if ctx.Err() != nil {
			return results, ctx.Err()
		}
	}
	return results, nil
}

func clipTrace(trace string) string {
	if len(trace) > 4000 {
		return "..." + trace[len(trace)-4000:]
	}
	return trace
}

// program builds one check's steps.
type program struct {
	s     *suite
	id    string
	steps []Step
	err   error
}

func (p *program) call(a Action)         { p.steps = append(p.steps, Step{Call: &a}) }
func (p *program) parallel(as ...Action) { p.steps = append(p.steps, Step{Parallel: as}) }
func (p *program) settle()               { p.steps = append(p.steps, Step{Settle: true}) }
func (p *program) replicas() int         { return p.s.config.Replicas }

func (p *program) newUser() *user {
	s := p.s
	s.nextID++
	u := &user{id: s.nextID, name: fmt.Sprintf("%su%d", s.prefix, s.nextID%100000), followers: map[*user]bool{}}
	p.call(Register(u.id, u.name))
	return u
}

func (p *program) newUsers(n int) []*user {
	out := make([]*user, n)
	for i := range out {
		out[i] = p.newUser()
	}
	return out
}

// follow changes u -> v and lets the change settle.
func (p *program) follow(u, v *user, on bool) {
	p.call(Follow(u.id, v.id, on))
	if on {
		v.followers[u] = true
		u.home += v.posts
	} else {
		delete(v.followers, u)
	}
	p.settle()
}

type composeOptions struct {
	mentions []*user
	url      bool
	media    []timeline.Media
}

// composeAction is a post of u: a few words, its mentions, a unique marker, maybe a URL.
func (p *program) composeAction(u *user, options composeOptions) Action {
	s := p.s
	s.sequence++
	marker := fmt.Sprintf("m%sp%d", s.prefix, s.sequence)
	words := wordList
	parts := []string{words[s.random.Intn(len(words))], words[s.random.Intn(len(words))]}
	a := Action{Kind: KindCompose, User: u.id, Name: u.name, Marker: marker, Media: options.media}
	for _, v := range options.mentions {
		parts = append(parts, "@"+v.name)
		a.Mentions = append(a.Mentions, v.id)
	}
	parts = append(parts, words[s.random.Intn(len(words))], "#"+marker)
	if options.url {
		a.URL = fmt.Sprintf("http://example%d.com", s.random.Intn(1000000))
		parts = append(parts, a.URL)
	}
	a.Text = strings.Join(parts, " ")
	return a
}

// posted counts a post of u in the timelines it may reach.
func posted(u *user, options composeOptions) {
	u.posts++
	recipients := map[*user]bool{}
	for v := range u.followers {
		recipients[v] = true
	}
	for _, v := range options.mentions {
		recipients[v] = true
	}
	for v := range recipients {
		v.home++
	}
}

// compose posts as u, then reads the first page of the author's timeline on every replica
// (read your write).
func (p *program) compose(u *user, options composeOptions) {
	p.call(p.composeAction(u, options))
	posted(u, options)
	for i := 0; i < p.replicas(); i++ {
		p.call(Read(false, u.id, 0, PageSize))
	}
}

// expect reads u's whole timeline on every replica.
func (p *program) expect(u *user, home bool) {
	for i := 0; i < p.replicas(); i++ {
		p.call(Read(home, u.id, 0, p.size(u, home)+5))
	}
}

func (p *program) size(u *user, home bool) int {
	if home {
		return u.home
	}
	return u.posts
}

var wordList = strings.Fields("time day road park river tree garden table book page music song game team ball water coffee bread apple lunch market train bike plan idea photo color light paper box walk read write cook paint build open start visit share watch learn help find")

func (s *suite) userTimelineSchema(p *program) {
	a := p.newUser()
	p.compose(a, composeOptions{})
	p.expect(a, false)
}

func (s *suite) homeTimelineSchema(p *program) {
	users := p.newUsers(2)
	a, b := users[0], users[1]
	p.follow(a, b, true)
	p.compose(b, composeOptions{})
	p.settle()
	p.expect(a, true)
}

func (s *suite) userTimelineOrder(p *program) {
	a := p.newUser()
	for i := 0; i < 4; i++ {
		p.compose(a, composeOptions{})
	}
	p.expect(a, false)
}

func (s *suite) homeTimelineOrder(p *program) {
	users := p.newUsers(3)
	a := users[0]
	for _, v := range users[1:] {
		p.follow(a, v, true)
	}
	for i := 0; i < 4; i++ {
		p.compose(users[1+i%2], composeOptions{})
	}
	p.settle()
	p.expect(a, true)
}

func (s *suite) visibility(p *program) {
	users := p.newUsers(4)
	a, b, c, mentioned := users[0], users[1], users[2], users[3]
	p.follow(a, b, true)
	p.compose(b, composeOptions{})
	p.compose(c, composeOptions{mentions: []*user{mentioned}})
	p.settle()
	p.expect(a, true)
	p.expect(mentioned, true)
	for i := 0; i < p.replicas(); i++ { // b follows nobody and is not mentioned
		p.call(Read(true, b.id, 0, 10))
	}
}

func (s *suite) readYourWrite(p *program) {
	a := p.newUser()
	for i := 0; i < 3; i++ {
		p.compose(a, composeOptions{})
	}
	p.expect(a, false)
}

// fanout reads every follower's home timeline right at the bound.
func (s *suite) fanout(p *program) {
	users := p.newUsers(4)
	author := users[0]
	for _, v := range users[1:] {
		p.follow(v, author, true)
	}
	for round := 0; round < 3; round++ {
		p.compose(author, composeOptions{})
		p.settle()
		for _, v := range users[1:] {
			p.expect(v, true)
		}
	}
}

func (s *suite) unfollow(p *program) {
	users := p.newUsers(2)
	a, b := users[0], users[1]
	p.follow(a, b, true)
	p.compose(b, composeOptions{})
	p.settle()
	p.expect(a, true)
	p.follow(a, b, false)
	p.compose(b, composeOptions{}) // must stay out of a's home timeline
	p.settle()
	for i := 0; i < p.replicas(); i++ {
		p.call(Read(true, a.id, 0, a.home+10))
	}
}

func (s *suite) follow(p *program) {
	users := p.newUsers(2)
	a, b := users[0], users[1]
	p.compose(b, composeOptions{}) // may or may not appear after the follow
	p.follow(a, b, true)
	p.compose(b, composeOptions{}) // must appear, first
	p.settle()
	for i := 0; i < p.replicas(); i++ {
		p.call(Read(true, a.id, 0, 10))
	}
}

func (s *suite) invalidRequests(p *program) {
	a := p.newUser()
	p.compose(a, composeOptions{})
	unknown := strconv.FormatInt(s.nextID+999, 10)
	id := strconv.FormatInt(a.id, 10)
	for _, args := range [][4]string{
		{"user", unknown, "0", "10"}, {"home", unknown, "0", "10"}, {"user", id, "5", "2"},
		{"home", id, "-1", "3"}, {"user", id, "x", "y"}, {"user", "", "0", "10"},
	} {
		p.call(RawRead(args[0] == "home", args[1], args[2], args[3]))
	}
	// A compose without text is refused and leaves no post.
	p.call(Action{Kind: KindCompose, User: a.id, Name: a.name})
	p.expect(a, false)
	p.expect(a, true)
}

func (s *suite) pagination(p *program) {
	a := p.newUser()
	for i := 0; i < 7; i++ {
		p.compose(a, composeOptions{})
	}
	for _, window := range [][2]int{{0, 3}, {3, 6}, {6, 9}, {2, 5}, {5, 7}, {7, 10}} {
		p.call(Read(false, a.id, window[0], window[1]))
	}
}

func (s *suite) burstAndContent(p *program) {
	users := p.newUsers(3)
	a, b, c := users[0], users[1], users[2]
	p.follow(a, b, true)
	media := []timeline.Media{{ID: "12345678901234567", Type: "png"}, {ID: "22345678901234567", Type: "jpg"}}
	p.compose(b, composeOptions{url: true})
	p.compose(b, composeOptions{mentions: []*user{c}, media: media})
	p.compose(b, composeOptions{mentions: []*user{a, c}, url: true})
	p.settle()
	for _, u := range users {
		p.expect(u, true)
	}
	p.expect(b, false)
}

// datasetCheck reads seeded timelines of a sample of users.
func (s *suite) datasetCheck(p *program) {
	if s.config.Dataset == nil {
		return
	}
	for _, u := range s.datasetSample(60) {
		for _, home := range []bool{false, true} {
			p.call(Read(home, s.config.Dataset.Users[u].ID, s.random.Intn(3)*10, s.random.Intn(3)*10+10))
		}
	}
}

func (s *suite) datasetSample(n int) []int {
	data := s.config.Dataset
	out := []int{int(data.ActivityRank[0]), int(data.PopularityRank[0])}
	for len(out) < n {
		out = append(out, s.random.Intn(len(data.Users)))
	}
	return out
}

// durability builds a small fixture (d1 and d2 follow d0, d3 mentions d1), then restarts one
// pod of every deployment and flushes every cache in turn. After each event every timeline of
// the fixture and of a sample of seeded users must be served again on every replica, and
// writes must still work: a new post of d0 reaches its followers within the bound (after a
// flush, a new user follows d0 first, before anything reads the flushed state).
func (s *suite) durability(p *program) {
	if s.config.Hook == nil {
		p.err = fmt.Errorf("no restart hook")
		return
	}
	d := p.newUsers(4)
	for _, follower := range d[1:3] {
		p.follow(follower, d[0], true)
	}
	for i := 0; i < 3; i++ {
		p.compose(d[0], composeOptions{url: i == 1})
		p.compose(d[3], composeOptions{mentions: []*user{d[1]}})
	}
	p.settle()
	verify := func() {
		for _, u := range d {
			p.expect(u, false)
			p.expect(u, true)
		}
		if data := s.config.Dataset; data != nil {
			for _, u := range s.datasetSample(8) {
				for _, home := range []bool{false, true} {
					for i := 0; i < 2; i++ {
						p.call(Read(home, data.Users[u].ID, 0, 10))
					}
				}
			}
		}
	}
	var newcomers []*user
	writeAfter := func(follow bool) {
		if follow {
			newcomer := p.newUser()
			p.follow(newcomer, d[0], true)
			newcomers = append(newcomers, newcomer)
		}
		p.compose(d[0], composeOptions{})
		p.settle()
		p.expect(d[1], true)
		p.expect(d[2], true)
		for _, newcomer := range newcomers { // the new post first; older posts of d0 may or may not appear
			for i := 0; i < p.replicas(); i++ {
				p.call(Read(true, newcomer.id, 0, 1))
			}
		}
	}
	verify()
	for _, deployment := range s.config.Deployments {
		p.steps = append(p.steps, Step{Restart: deployment})
		verify()
		writeAfter(false)
	}
	for _, cache := range s.config.Caches {
		p.steps = append(p.steps, Step{Flush: cache})
		if s.config.FollowAfterFlush {
			writeAfter(true)
		}
		verify()
		writeAfter(false)
	}
}

// concurrency sends writes and reads at once: every answer must be explained by some ordering
// of the calls that keeps their real-time order (a post in flight may or may not be shown, a
// follow change in flight may or may not apply to a post composed meanwhile).
func (s *suite) concurrency(p *program) {
	users := p.newUsers(5)
	a, b, c, d, e := users[0], users[1], users[2], users[3], users[4]
	p.follow(c, a, true)
	p.follow(c, b, true)
	p.follow(d, a, true)
	// Two authors compose while their followers' and their own timelines are read.
	p.parallel(
		p.composeAction(a, composeOptions{}), p.composeAction(b, composeOptions{mentions: []*user{e}}),
		Read(true, c.id, 0, 10), Read(true, d.id, 0, 10), Read(false, a.id, 0, 10),
		Read(false, b.id, 0, 10), Read(true, c.id, 0, 10), Read(true, e.id, 0, 10),
	)
	posted(a, composeOptions{})
	posted(b, composeOptions{mentions: []*user{e}})
	p.settle()
	for _, u := range []*user{c, d, e} {
		p.expect(u, true)
	}
	p.expect(a, false)
	p.expect(b, false)
	// A follow while the followee composes: that post may or may not reach the new follower.
	p.parallel(Follow(e.id, a.id, true), p.composeAction(a, composeOptions{}), Read(true, e.id, 0, 10), Read(false, a.id, 0, 10))
	posted(a, composeOptions{})
	e.home += a.posts
	a.followers[e] = true
	p.settle()
	p.compose(a, composeOptions{}) // after the follow settled: must reach e
	p.settle()
	p.expect(e, true)
	// An unfollow while the followee composes, then a post that must stay out.
	p.parallel(Follow(c.id, b.id, false), p.composeAction(b, composeOptions{}), Read(true, c.id, 0, 10), Read(true, c.id, 0, 10))
	posted(b, composeOptions{})
	delete(b.followers, c)
	p.settle()
	p.compose(b, composeOptions{})
	p.settle()
	p.expect(c, true)
}
