package checker

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// user is a checker-owned user. The checker knows its exact state: every
// change is given time to settle before anything depends on it, so expected
// timelines are exact lists.
type user struct {
	id        int64
	name      string
	posts     []string // post ids, newest first
	home      []string
	followers map[*user]bool
	// homeExact is false once an edge into this user's home changed after
	// the followee had posted (posts before a change may or may not stay).
	homeExact bool
}

// world is the checker's users and the posts they wrote.
type world struct {
	c        *client
	bound    time.Duration
	replicas int
	random   *rand.Rand
	prefix   string
	nextID   int64
	users    []*user
	posts    map[string]*timeline.Post
	sequence int
}

var words = strings.Fields("time day road park river tree garden table book page music song game team ball water coffee bread apple lunch market train bike plan idea photo color light paper box walk read write cook paint build open start visit share watch learn help find")

func (w *world) newUser(ctx context.Context) (*user, error) {
	w.nextID++
	u := &user{id: w.nextID, name: fmt.Sprintf("%su%d", w.prefix, w.nextID%100000), followers: map[*user]bool{}, homeExact: true}
	if err := w.c.register(ctx, u); err != nil {
		return nil, err
	}
	w.users = append(w.users, u)
	return u, nil
}

func (w *world) newUsers(ctx context.Context, n int) ([]*user, error) {
	out := make([]*user, n)
	for i := range out {
		u, err := w.newUser(ctx)
		if err != nil {
			return nil, err
		}
		out[i] = u
	}
	return out, nil
}

// follow changes u -> v and waits until the change has settled.
func (w *world) follow(ctx context.Context, u, v *user, follow bool) error {
	if err := w.c.follow(ctx, u, v, follow); err != nil {
		return err
	}
	if len(v.posts) > 0 {
		u.homeExact = false
	}
	if follow {
		v.followers[u] = true
	} else {
		delete(v.followers, u)
	}
	return w.settle(ctx, time.Now())
}

func (w *world) settle(ctx context.Context, acked time.Time) error {
	return sleepUntil(ctx, acked.Add(w.bound+20*time.Millisecond))
}

func sleepUntil(ctx context.Context, at time.Time) error {
	timer := time.NewTimer(time.Until(at))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type composeOptions struct {
	mentions []*user
	url      bool
	media    []timeline.Media
}

// compose posts as u, checks read-your-write on every replica and learns
// the post. It returns the post and the time of the acknowledgement.
func (w *world) compose(ctx context.Context, u *user, options composeOptions) (*timeline.Post, time.Time, error) {
	w.sequence++
	marker := fmt.Sprintf("#%sp%d", w.prefix, w.sequence)
	parts := []string{words[w.random.Intn(len(words))], words[w.random.Intn(len(words))]}
	for _, v := range options.mentions {
		parts = append(parts, "@"+v.name)
	}
	parts = append(parts, words[w.random.Intn(len(words))], marker)
	expanded := ""
	if options.url {
		expanded = fmt.Sprintf("http://example%d.com", w.random.Intn(1000000))
		parts = append(parts, expanded)
	}
	text := strings.Join(parts, " ")
	sent := time.Now()
	if err := w.c.compose(ctx, u, text, options.media); err != nil {
		return nil, time.Time{}, err
	}
	acked := time.Now()
	// Read your write, on every replica.
	var learned *timeline.Post
	for attempt := 0; attempt < w.replicas; attempt++ {
		page, err := w.c.read(ctx, false, u.id, 0, 1)
		if err != nil {
			return nil, acked, err
		}
		if len(page) != 1 || !strings.Contains(page[0].Text, marker) {
			return nil, acked, fmt.Errorf("read your write (read %d): the author's timeline starts with %v, not the post just composed", attempt+1, describeAll(page))
		}
		if learned == nil {
			post := page[0]
			if err := checkComposed(&post, u, text, expanded, options, sent, acked); err != nil {
				return nil, acked, err
			}
			learned = &post
		} else if !timeline.Same(learned, &page[0]) {
			return nil, acked, fmt.Errorf("read your write: %s served differently as %s", timeline.Describe(learned), timeline.Describe(&page[0]))
		}
	}
	w.posts[learned.PostID] = learned
	u.posts = append([]string{learned.PostID}, u.posts...)
	recipients := map[*user]bool{}
	for v := range u.followers {
		recipients[v] = true
	}
	for _, v := range options.mentions {
		recipients[v] = true
	}
	for v := range recipients {
		v.home = append([]string{learned.PostID}, v.home...)
	}
	return learned, acked, nil
}

func checkComposed(post *timeline.Post, u *user, text, expanded string, options composeOptions, sent, acked time.Time) error {
	if post.CreatorID != strconv.FormatInt(u.id, 10) || post.Creator != u.name || post.PostType != 0 {
		return fmt.Errorf("composed post has creator (%s, %s) type %d, want (%d, %s) type 0", post.CreatorID, post.Creator, post.PostType, u.id, u.name)
	}
	if expanded == "" {
		if post.Text != text || len(post.URLs) != 0 {
			return fmt.Errorf("composed post text %q urls %v, want %q", post.Text, post.URLs, text)
		}
	} else if len(post.URLs) != 1 || post.URLs[0].Expanded != expanded || post.URLs[0].Shortened == "" ||
		post.Text != strings.TrimSuffix(text, expanded)+post.URLs[0].Shortened {
		return fmt.Errorf("composed post text %q urls %v, want the sent text with %s shortened", post.Text, post.URLs, expanded)
	}
	var mentions []timeline.Mention
	for _, v := range options.mentions {
		mentions = append(mentions, timeline.Mention{UserID: strconv.FormatInt(v.id, 10), Username: v.name})
	}
	if !timeline.SameMentions(mentions, post.Mentions) {
		return fmt.Errorf("composed post mentions %v, want %v", post.Mentions, mentions)
	}
	if fmt.Sprint(post.Media) != fmt.Sprint(options.media) && !(len(post.Media) == 0 && len(options.media) == 0) {
		return fmt.Errorf("composed post media %v, want %v", post.Media, options.media)
	}
	if post.Timestamp < sent.Add(-5*time.Second).UnixMilli() || post.Timestamp > acked.Add(5*time.Second).UnixMilli() {
		return fmt.Errorf("composed post timestamp %d outside %d..%d", post.Timestamp, sent.UnixMilli(), acked.UnixMilli())
	}
	return nil
}

// expect reads u's whole timeline on every replica and requires exactly
// the expected posts, newest first, with their learned content.
func (w *world) expect(ctx context.Context, u *user, home bool) error {
	want := u.posts
	if home {
		want = u.home
	}
	for attempt := 0; attempt < w.replicas; attempt++ {
		page, err := w.c.read(ctx, home, u.id, 0, len(want)+5)
		if err != nil {
			return err
		}
		if err := w.matches(page, want); err != nil {
			return fmt.Errorf("%s of %s (read %d of %d): %v", kindName(home), u.name, attempt+1, w.replicas, err)
		}
	}
	return nil
}

func (w *world) matches(page []timeline.Post, want []string) error {
	if len(page) != len(want) {
		return fmt.Errorf("%d posts %v, want %d", len(page), describeAll(page), len(want))
	}
	for i := range page {
		expected := w.posts[want[i]]
		if page[i].PostID != want[i] {
			return fmt.Errorf("post %d is %s, want %s", i, timeline.Describe(&page[i]), timeline.Describe(expected))
		}
		if !timeline.Same(expected, &page[i]) {
			return fmt.Errorf("post %d %s differs from its first read %s", i, timeline.Describe(&page[i]), timeline.Describe(expected))
		}
	}
	return nil
}

// absent requires that a post is in none of u's home timeline reads.
func (w *world) absent(ctx context.Context, u *user, postID string) error {
	for attempt := 0; attempt < w.replicas; attempt++ {
		page, err := w.c.read(ctx, true, u.id, 0, len(u.home)+10)
		if err != nil {
			return err
		}
		for _, post := range page {
			if post.PostID == postID {
				return fmt.Errorf("home timeline of %s (read %d) holds %s", u.name, attempt+1, timeline.Describe(&post))
			}
		}
	}
	return nil
}

func describeAll(page []timeline.Post) string {
	parts := make([]string, len(page))
	for i := range page {
		parts[i] = timeline.Describe(&page[i])
	}
	return "[" + strings.Join(parts, "; ") + "]"
}
