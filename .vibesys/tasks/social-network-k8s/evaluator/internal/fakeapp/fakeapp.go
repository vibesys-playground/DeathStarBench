// Package fakeapp is an in-process fake of the Social Network's public API
// (the nginx /wrk2-api routes) with the pinned application's semantics:
// push fan-out to followers and mentioned users, URL shortening that drops
// the text after the last URL, cjson's {} for empty lists. Options inject
// the faults the evaluator must catch. It is test infrastructure.
package fakeapp

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
)

// Options select faults.
type Options struct {
	// FanoutDelay delays home-timeline delivery (asynchronous fan-out).
	FanoutDelay time.Duration
	// StaleCache serves every timeline read from a response cache that is
	// never invalidated (filled on first read).
	StaleCache bool
	// StaleReplica makes every other read come from a replica whose cache
	// is never invalidated (round robin over two instances).
	StaleReplica bool
	// LoseHomeOnFlush drops all home timelines when a cache is flushed.
	LoseHomeOnFlush bool
	// LosePostsOnRestart forgets posts composed since start on a restart.
	LosePostsOnRestart bool
	// IgnoreUnfollow acknowledges unfollows without applying them.
	IgnoreUnfollow bool
	// FollowerCache models the pinned social graph service's Redis cache of
	// follower sets: fan-out reads the cached set when it is non-empty and
	// fills it from the store otherwise, a follow adds to the cached set
	// whether or not it was cached, and a flush of social-graph-redis
	// empties the cache. A follow after a flush thus leaves a partial set.
	FollowerCache bool
}

type post struct {
	id, author int64
	reqID      int64
	timestamp  int64
	text       string
	mentions   []int64
	urls       [][2]string
	media      [][2]string
	seeded     bool
}

// App is the fake application.
type App struct {
	options       Options
	mu            sync.Mutex
	names         map[int64]string
	ids           map[string]int64
	follows       map[int64]map[int64]bool // follower -> followees
	posts         map[int64]*post
	user          map[int64][]int64 // newest first
	home          map[int64][]int64
	cache         map[string][]byte
	followerCache map[int64]map[int64]bool // author -> followers
	replica       int
	nextID        int64
	clock         int64
	random        *rand.Rand
	Requests      int
}

// New returns a fake holding data (nil: empty).
func New(data *dataset.Dataset, options Options) *App {
	app := &App{
		options: options, names: map[int64]string{}, ids: map[string]int64{}, follows: map[int64]map[int64]bool{},
		posts: map[int64]*post{}, user: map[int64][]int64{}, home: map[int64][]int64{}, cache: map[string][]byte{}, followerCache: map[int64]map[int64]bool{},
		nextID: 1 << 50, random: rand.New(rand.NewSource(1)),
	}
	if data == nil {
		return app
	}
	for _, u := range data.Users {
		app.names[u.ID] = u.Username
		app.ids[u.Username] = u.ID
	}
	for u := range data.Users {
		set := map[int64]bool{}
		for _, v := range data.Followees.Row(u) {
			set[data.Users[v].ID] = true
		}
		app.follows[data.Users[u].ID] = set
	}
	for index := range data.Posts {
		source := &data.Posts[index]
		p := &post{id: source.ID, author: data.Users[source.Author].ID, reqID: source.ReqID, timestamp: source.Timestamp, text: source.Text, seeded: true}
		for _, v := range source.Mentions {
			p.mentions = append(p.mentions, data.Users[v].ID)
		}
		for _, url := range source.URLs {
			p.urls = append(p.urls, [2]string{url.Shortened, url.Expanded})
		}
		for _, item := range source.Media {
			p.media = append(p.media, [2]string{strconv.FormatInt(item.ID, 10), item.Type})
		}
		app.posts[p.id] = p
	}
	for u := range data.Users {
		id := data.Users[u].ID
		for _, index := range data.UserPosts.Row(u) {
			app.user[id] = append(app.user[id], data.Posts[index].ID)
		}
		for _, index := range data.Home.Row(u) {
			app.home[id] = append(app.home[id], data.Posts[index].ID)
		}
	}
	return app
}

// Restart models a restart of a stateless pod.
func (a *App) Restart(string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cache = map[string][]byte{}
	if a.options.LosePostsOnRestart {
		for id, p := range a.posts {
			if !p.seeded {
				delete(a.posts, id)
			}
		}
		prune := func(lists map[int64][]int64) {
			for u, list := range lists {
				kept := list[:0]
				for _, id := range list {
					if a.posts[id] != nil {
						kept = append(kept, id)
					}
				}
				lists[u] = kept
			}
		}
		prune(a.user)
		prune(a.home)
	}
}

// Flush models a cache flush.
func (a *App) Flush(cache string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.options.LoseHomeOnFlush {
		a.home = map[int64][]int64{}
	}
	if cache == "social-graph-redis" {
		a.followerCache = map[int64]map[int64]bool{}
	}
}

func (a *App) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	a.mu.Lock()
	a.Requests++
	a.mu.Unlock()
	if err := request.ParseForm(); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	form := request.Form
	switch request.URL.Path {
	case "/wrk2-api/user/register":
		a.register(writer, form)
	case "/wrk2-api/user/follow", "/wrk2-api/user/unfollow":
		a.follow(writer, form, request.URL.Path == "/wrk2-api/user/follow")
	case "/wrk2-api/post/compose":
		a.compose(writer, form)
	case "/wrk2-api/user-timeline/read", "/wrk2-api/home-timeline/read":
		a.read(writer, request)
	default:
		http.NotFound(writer, request)
	}
}

func (a *App) register(writer http.ResponseWriter, form map[string][]string) {
	get := func(key string) string { return first(form[key]) }
	id, err := strconv.ParseInt(get("user_id"), 10, 64)
	if err != nil || get("username") == "" || get("first_name") == "" || get("last_name") == "" || get("password") == "" {
		http.Error(writer, "Incomplete arguments", http.StatusBadRequest)
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.ids[get("username")]; exists {
		http.Error(writer, "User registration failure: User "+get("username")+" already existed", http.StatusInternalServerError)
		return
	}
	a.names[id] = get("username")
	a.ids[get("username")] = id
	fmt.Fprintln(writer, "Success!")
}

func (a *App) follow(writer http.ResponseWriter, form map[string][]string, follow bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var u, v int64
	if first(form["user_id"]) != "" {
		u, _ = strconv.ParseInt(first(form["user_id"]), 10, 64)
		v, _ = strconv.ParseInt(first(form["followee_id"]), 10, 64)
	} else {
		u, v = a.ids[first(form["user_name"])], a.ids[first(form["followee_name"])]
	}
	if u == 0 || v == 0 {
		http.Error(writer, "Incomplete arguments", http.StatusBadRequest)
		return
	}
	if a.follows[u] == nil {
		a.follows[u] = map[int64]bool{}
	}
	if follow {
		a.follows[u][v] = true
		if a.options.FollowerCache {
			if a.followerCache[v] == nil {
				a.followerCache[v] = map[int64]bool{}
			}
			a.followerCache[v][u] = true
		}
	} else if !a.options.IgnoreUnfollow {
		delete(a.follows[u], v)
		delete(a.followerCache[v], u)
	}
	fmt.Fprintln(writer, "Success!")
}

var (
	mentionPattern = regexp.MustCompile(`@[a-zA-Z0-9-_]+`)
	urlPattern     = regexp.MustCompile(`(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)`)
)

func (a *App) compose(writer http.ResponseWriter, form map[string][]string) {
	author, err := strconv.ParseInt(first(form["user_id"]), 10, 64)
	text := first(form["text"])
	if err != nil || first(form["username"]) == "" || text == "" || first(form["post_type"]) == "" {
		http.Error(writer, "Incomplete arguments", http.StatusBadRequest)
		return
	}
	var mediaIDs, mediaTypes []string
	_ = json.Unmarshal([]byte(first(form["media_ids"])), &mediaIDs)
	_ = json.Unmarshal([]byte(first(form["media_types"])), &mediaTypes)
	a.mu.Lock()
	a.nextID++
	now := time.Now().UnixMilli()
	if now <= a.clock {
		now = a.clock + 1
	}
	a.clock = now
	p := &post{id: a.nextID, author: author, reqID: a.random.Int63n(1 << 52), timestamp: now}
	for _, name := range mentionPattern.FindAllString(text, -1) {
		if id, ok := a.ids[name[1:]]; ok {
			p.mentions = append(p.mentions, id)
		}
	}
	// The pinned text service: URLs shortened, text after the last dropped.
	p.text = text
	if matches := urlPattern.FindAllStringIndex(text, -1); len(matches) > 0 {
		updated, last := "", 0
		for _, match := range matches {
			letters := make([]byte, 10)
			for i := range letters {
				letters[i] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[a.random.Intn(62)]
			}
			short := "http://short-url/" + string(letters)
			p.urls = append(p.urls, [2]string{short, text[match[0]:match[1]]})
			updated += text[last:match[0]] + short
			last = match[1]
		}
		p.text = updated
	}
	for index := range mediaIDs {
		if index < len(mediaTypes) {
			p.media = append(p.media, [2]string{mediaIDs[index], mediaTypes[index]})
		}
	}
	a.posts[p.id] = p
	a.user[author] = insert(a.user[author], p.id, a.posts)
	var recipients []int64
	if cached := a.followerCache[author]; a.options.FollowerCache && len(cached) > 0 {
		for follower := range cached {
			recipients = append(recipients, follower)
		}
	} else {
		filled := map[int64]bool{}
		for follower, followees := range a.follows {
			if followees[author] {
				recipients = append(recipients, follower)
				filled[follower] = true
			}
		}
		if a.options.FollowerCache {
			a.followerCache[author] = filled
		}
	}
	recipients = append(recipients, p.mentions...)
	a.mu.Unlock()
	deliver := func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.posts[p.id] == nil {
			return
		}
		for _, r := range recipients {
			if !contains(a.home[r], p.id) {
				a.home[r] = insert(a.home[r], p.id, a.posts)
			}
		}
	}
	if a.options.FanoutDelay > 0 {
		time.AfterFunc(a.options.FanoutDelay, deliver)
	} else {
		deliver()
	}
	fmt.Fprintln(writer, "Successfully upload post")
}

func (a *App) read(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	id, err1 := strconv.ParseInt(query.Get("user_id"), 10, 64)
	start, err2 := strconv.Atoi(query.Get("start"))
	stop, err3 := strconv.Atoi(query.Get("stop"))
	if err1 != nil || err2 != nil || err3 != nil {
		http.Error(writer, "Incomplete arguments", http.StatusBadRequest)
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	key := request.URL.Path + "?" + request.URL.RawQuery
	stale := a.options.StaleCache
	if a.options.StaleReplica {
		a.replica++
		stale = a.replica%2 == 0
	}
	if cached, ok := a.cache[key]; ok && stale {
		writer.Header().Set("Content-Type", "application/json")
		writer.Write(cached)
		return
	}
	list := a.user[id]
	if strings.Contains(request.URL.Path, "home") {
		list = a.home[id]
	}
	var out []map[string]any
	if start >= 0 && stop > start {
		for index := start; index < stop && index < len(list); index++ {
			out = append(out, a.wire(a.posts[list[index]]))
		}
	}
	body, _ := json.Marshal(emptyAsObject(out))
	if a.options.StaleCache || a.options.StaleReplica {
		a.cache[key] = body
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(body)
}

func (a *App) wire(p *post) map[string]any {
	mentions := []any{}
	for _, id := range p.mentions {
		mentions = append(mentions, map[string]any{"user_id": strconv.FormatInt(id, 10), "username": a.names[id]})
	}
	media := []any{}
	for _, item := range p.media {
		media = append(media, map[string]any{"media_id": item[0], "media_type": item[1]})
	}
	urls := []any{}
	for _, url := range p.urls {
		urls = append(urls, map[string]any{"shortened_url": url[0], "expanded_url": url[1]})
	}
	return map[string]any{
		"post_id": strconv.FormatInt(p.id, 10), "req_id": strconv.FormatInt(p.reqID, 10), "text": p.text,
		"timestamp": strconv.FormatInt(p.timestamp, 10), "post_type": 0,
		"creator":       map[string]any{"user_id": strconv.FormatInt(p.author, 10), "username": a.names[p.author]},
		"user_mentions": emptyAsObject(mentions), "media": emptyAsObject(media), "urls": emptyAsObject(urls),
	}
}

// emptyAsObject mimics cjson, which encodes an empty Lua table as {}.
func emptyAsObject[T any](list []T) any {
	if len(list) == 0 {
		return map[string]any{}
	}
	return list
}

// insert keeps a list newest first.
func insert(list []int64, id int64, posts map[int64]*post) []int64 {
	at := sort.Search(len(list), func(i int) bool { return posts[list[i]].timestamp < posts[id].timestamp })
	list = append(list, 0)
	copy(list[at+1:], list[at:])
	list[at] = id
	return list
}

func contains(list []int64, id int64) bool {
	for _, value := range list {
		if value == id {
			return true
		}
	}
	return false
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
