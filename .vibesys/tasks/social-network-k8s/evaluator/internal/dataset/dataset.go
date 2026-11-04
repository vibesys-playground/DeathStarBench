// Package dataset generates the task's Social Network dataset from a seed:
// users, a directed follow graph with heavy-tailed follower and followee
// counts, and posts with a heavy-tailed number per user. The seed command
// writes it into the data tier; the load generator and the checker derive
// their expected answers from the same generation.
//
// All text is synthetic: words from a short fixed list, user names
// "user<id>", mentions "@user<id>", URLs "http://example<n>.com".
package dataset

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DefaultSeed is the task's dataset seed.
const DefaultSeed uint64 = 0x5eed50c1a1

// Config sizes the dataset.
type Config struct {
	Users int
	// FolloweeScale is the Pareto2 scale of the number of followees beyond
	// the first (mean about FolloweeScale + 1), capped at MaxFollowees.
	FolloweeScale float64
	MaxFollowees  int
	// Reciprocity is the probability that a followed user follows back.
	Reciprocity float64
	// PostScale is the Pareto2 scale of the posts per user (mean about
	// PostScale), capped at MaxPosts.
	PostScale float64
	MaxPosts  int
}

// DefaultConfig is the task's dataset size.
func DefaultConfig() Config {
	return Config{Users: 20000, FolloweeScale: 12, MaxFollowees: 2000, Reciprocity: 0.2, PostScale: 5, MaxPosts: 400}
}

const (
	// Posts are spread over the 180 days before WindowEnd (2026-06-01 UTC),
	// so they are always older than posts composed during a run.
	WindowEnd    int64 = 1780272000000
	WindowLength int64 = 180 * 24 * 3600 * 1000
	// FirstPostID: the service's unique ids are (machine, time, counter)
	// words far above this range, so seeded ids never collide with them.
	FirstPostID int64 = 1_000_000
	// ShortURLPrefix is the url-shorten service's prefix.
	ShortURLPrefix = "http://short-url/"
)

// User is user index i, with id i+1.
type User struct {
	ID        int64
	Username  string
	FirstName string
	LastName  string
	Password  string
	Salt      string
}

// URL is one shortened URL of a post.
type URL struct{ Shortened, Expanded string }

// Media is one media item of a post.
type Media struct {
	ID   int64
	Type string
}

// Post is one seeded post. Text is as stored: URLs already shortened.
type Post struct {
	ID        int64
	ReqID     int64
	Author    int32 // user index
	Timestamp int64 // ms since the epoch, unique across the dataset
	Text      string
	Mentions  []int32 // user indices
	URLs      []URL
	Media     []Media
}

// CSR is a list of int32 lists.
type CSR struct {
	Offsets []int32
	Values  []int32
}

// Row returns list i.
func (c *CSR) Row(i int) []int32 { return c.Values[c.Offsets[i]:c.Offsets[i+1]] }

// Dataset is the generated data.
type Dataset struct {
	Seed   uint64
	Config Config
	Users  []User
	// Followees and Followers are sorted ascending user indices.
	Followees CSR
	Followers CSR
	// Posts are in timestamp order; post index p has id FirstPostID + p.
	Posts []Post
	// UserPosts and Home list post indices, newest first.
	UserPosts CSR
	Home      CSR
	// PopularityRank lists user indices from the most to the least followed
	// (ties by index); ActivityRank from the most to the least posting.
	PopularityRank []int32
	ActivityRank   []int32

	digestOnce sync.Once
	digest     string
}

var (
	defaultOnce sync.Once
	defaultData *Dataset
)

// Default is the dataset of DefaultSeed and DefaultConfig, generated once.
func Default() *Dataset {
	defaultOnce.Do(func() { defaultData = Generate(DefaultSeed, DefaultConfig()) })
	return defaultData
}

// Generate builds the dataset of a seed.
func Generate(seed uint64, config Config) *Dataset {
	data := &Dataset{Seed: seed, Config: config}
	data.generateUsers()
	data.generateGraph()
	data.generatePosts()
	data.indexPosts()
	return data
}

// UserIndex returns the index of a user id, or -1.
func (d *Dataset) UserIndex(id int64) int {
	if id < 1 || id > int64(len(d.Users)) {
		return -1
	}
	return int(id - 1)
}

// Follows reports whether user u follows user v in the dataset.
func (d *Dataset) Follows(u, v int) bool {
	row := d.Followees.Row(u)
	at := sort.Search(len(row), func(i int) bool { return row[i] >= int32(v) })
	return at < len(row) && row[at] == int32(v)
}

// FollowTime is the timestamp the social graph stores for edge u -> v.
func (d *Dataset) FollowTime(u, v int) int64 {
	span := uint64(WindowLength + 365*24*3600*1000)
	return WindowEnd - WindowLength - 365*24*3600*1000 + int64(Mix(d.Seed^uint64(u)<<20, uint64(v))%span)
}

func (d *Dataset) generateUsers() {
	d.Users = make([]User, d.Config.Users)
	for index := range d.Users {
		r := NewRNG(d.Seed, "user", index)
		id := int64(index + 1)
		d.Users[index] = User{
			ID: id, Username: "user" + strconv.FormatInt(id, 10),
			FirstName: "first" + strconv.FormatInt(id, 10), LastName: "last" + strconv.FormatInt(id, 10),
			Password: "password" + strconv.FormatInt(id, 10), Salt: token(r, 32),
		}
	}
}

func (d *Dataset) generateGraph() {
	n := d.Config.Users
	// Followees are drawn by a Zipf over a random popularity order, so
	// follower counts are heavy-tailed.
	order := make([]int32, n)
	for index := range order {
		order[index] = int32(index)
	}
	shuffleRNG := NewRNG(d.Seed, "popularity", 0)
	for i := n - 1; i > 0; i-- {
		j := shuffleRNG.Intn(i + 1)
		order[i], order[j] = order[j], order[i]
	}
	zipf := NewZipf(n, ZipfThreeQuarters)
	followees := make([]map[int32]struct{}, n)
	for index := range followees {
		followees[index] = make(map[int32]struct{})
	}
	type edge struct{ u, v int32 }
	var edges []edge
	for u := 0; u < n; u++ {
		r := NewRNG(d.Seed, "followees", u)
		want := 1 + r.Pareto2(d.Config.FolloweeScale)
		if want > d.Config.MaxFollowees {
			want = d.Config.MaxFollowees
		}
		for attempt := 0; attempt < 4*want && len(followees[u]) < want; attempt++ {
			v := order[zipf.Rank(r.Float())]
			if int(v) == u {
				continue
			}
			if _, ok := followees[u][v]; ok {
				continue
			}
			followees[u][v] = struct{}{}
			edges = append(edges, edge{int32(u), v})
		}
	}
	for index, e := range edges {
		r := NewRNG(d.Seed, "reciprocity", index)
		if !r.Chance(d.Config.Reciprocity) {
			continue
		}
		if _, ok := followees[e.v][e.u]; !ok {
			followees[e.v][e.u] = struct{}{}
		}
	}
	followers := make([][]int32, n)
	rows := make([][]int32, n)
	for u := 0; u < n; u++ {
		row := make([]int32, 0, len(followees[u]))
		for v := range followees[u] {
			row = append(row, v)
		}
		sort.Slice(row, func(i, j int) bool { return row[i] < row[j] })
		rows[u] = row
		for _, v := range row {
			followers[v] = append(followers[v], int32(u))
		}
	}
	d.Followees = buildCSR(rows)
	d.Followers = buildCSR(followers) // ascending: u increases in the loop
	d.PopularityRank = rankBy(n, func(u int) int { return len(followers[u]) })
}

var words = strings.Fields(`
	time day way year week work home city road park river tree garden table
	chair window door book page letter music song film game team ball field
	water coffee tea bread apple lunch dinner market store train bus bike car
	plan idea story photo picture color light sound paper box bag shoe coat
	walk run read write cook paint build open close start finish visit share
	watch listen learn teach help find bring carry check try move wait call
	good new big small long short early late quiet happy busy warm cold green
	blue bright fresh simple today tomorrow again soon here there`)

func (d *Dataset) generatePosts() {
	n := d.Config.Users
	type draft struct {
		author    int32
		timestamp int64
		r         *RNG
	}
	var drafts []draft
	counts := make([]int, n)
	for u := 0; u < n; u++ {
		r := NewRNG(d.Seed, "posts", u)
		count := r.Pareto2(d.Config.PostScale)
		if count > d.Config.MaxPosts {
			count = d.Config.MaxPosts
		}
		counts[u] = count
		for k := 0; k < count; k++ {
			drafts = append(drafts, draft{
				author:    int32(u),
				timestamp: WindowEnd - WindowLength + int64(r.Next()%uint64(WindowLength)),
				r:         NewRNG(d.Seed, "post/"+strconv.Itoa(u), k),
			})
		}
	}
	sort.SliceStable(drafts, func(i, j int) bool { return drafts[i].timestamp < drafts[j].timestamp })
	// Make timestamps unique, so that the order of every timeline is defined.
	for index := 1; index < len(drafts); index++ {
		if drafts[index].timestamp <= drafts[index-1].timestamp {
			drafts[index].timestamp = drafts[index-1].timestamp + 1
		}
	}
	d.Posts = make([]Post, len(drafts))
	for index, item := range drafts {
		d.Posts[index] = d.composePost(index, item.author, item.timestamp, item.r)
	}
	d.ActivityRank = rankBy(n, func(u int) int { return counts[u] })
}

// composePost draws one post's text, mentions, URL and media.
func (d *Dataset) composePost(index int, author int32, timestamp int64, r *RNG) Post {
	post := Post{
		ID: FirstPostID + int64(index), ReqID: int64(r.Next() >> 12), Author: author, Timestamp: timestamp,
	}
	count := 3 + r.Pareto2(6)
	if count > 30 {
		count = 30
	}
	parts := make([]string, count)
	for i := range parts {
		parts[i] = words[r.Intn(len(words))]
	}
	// Mentions name followees of the author.
	followees := d.Followees.Row(int(author))
	mentions := 0
	if len(followees) > 0 && r.Chance(0.25) {
		mentions = 1
		if len(followees) > 1 && r.Chance(0.2) {
			mentions = 2
		}
	}
	for len(post.Mentions) < mentions {
		v := followees[r.Intn(len(followees))]
		if len(post.Mentions) == 1 && post.Mentions[0] == v {
			continue
		}
		post.Mentions = append(post.Mentions, v)
		at := r.Intn(len(parts) + 1)
		parts = append(parts[:at], append([]string{"@" + d.Users[v].Username}, parts[at:]...)...)
	}
	text := strings.Join(parts, " ")
	// A URL, if any, ends the text (the pinned text service drops what
	// follows the last URL).
	if r.Chance(0.15) {
		url := URL{
			Expanded:  fmt.Sprintf("http://example%d.com", r.Intn(1000000)),
			Shortened: ShortURLPrefix + token(r, 10),
		}
		post.URLs = append(post.URLs, url)
		text += " " + url.Shortened
	}
	post.Text = text
	if r.Chance(0.1) {
		items := 1 + r.Intn(3)
		for i := 0; i < items; i++ {
			mediaType := "png"
			if r.Chance(0.5) {
				mediaType = "jpg"
			}
			post.Media = append(post.Media, Media{ID: 1e16 + int64(r.Next()%9e16), Type: mediaType})
		}
	}
	return post
}

func (d *Dataset) indexPosts() {
	n := d.Config.Users
	userPosts := make([][]int32, n)
	home := make([][]int32, n)
	for index := len(d.Posts) - 1; index >= 0; index-- { // newest first
		post := &d.Posts[index]
		userPosts[post.Author] = append(userPosts[post.Author], int32(index))
		followers := d.Followers.Row(int(post.Author))
		for _, follower := range followers {
			home[follower] = append(home[follower], int32(index))
		}
		for _, mentioned := range post.Mentions {
			// Mentions name followees of the author, who follow the author
			// back only sometimes; the home timeline is a set.
			if !d.Follows(int(mentioned), int(post.Author)) {
				home[mentioned] = append(home[mentioned], int32(index))
			}
		}
	}
	d.UserPosts = buildCSR(userPosts)
	d.Home = buildCSR(home)
}

// Counts are the dataset's sizes.
type Counts struct {
	Users        int `json:"users"`
	Follows      int `json:"follows"`
	Posts        int `json:"posts"`
	HomeEntries  int `json:"home_entries"`
	MaxFollowers int `json:"max_followers"`
	MaxFollowees int `json:"max_followees"`
	MaxPosts     int `json:"max_posts"`
	MaxHome      int `json:"max_home"`
}

// Counts returns the dataset's sizes.
func (d *Dataset) Counts() Counts {
	maxRow := func(c *CSR) int {
		result := 0
		for i := 0; i+1 < len(c.Offsets); i++ {
			if size := int(c.Offsets[i+1] - c.Offsets[i]); size > result {
				result = size
			}
		}
		return result
	}
	return Counts{
		Users: len(d.Users), Follows: len(d.Followees.Values), Posts: len(d.Posts), HomeEntries: len(d.Home.Values),
		MaxFollowers: maxRow(&d.Followers), MaxFollowees: maxRow(&d.Followees),
		MaxPosts: maxRow(&d.UserPosts), MaxHome: maxRow(&d.Home),
	}
}

// Digest is the SHA-256 of a canonical encoding of the whole dataset.
func (d *Dataset) Digest() string {
	d.digestOnce.Do(func() {
		sum := sha256.New()
		buffer := make([]byte, 8)
		integer := func(value int64) {
			binary.BigEndian.PutUint64(buffer, uint64(value))
			sum.Write(buffer)
		}
		text := func(value string) {
			integer(int64(len(value)))
			sum.Write([]byte(value))
		}
		integer(int64(d.Seed))
		for _, user := range d.Users {
			integer(user.ID)
			for _, value := range []string{user.Username, user.FirstName, user.LastName, user.Password, user.Salt} {
				text(value)
			}
		}
		for _, csr := range []*CSR{&d.Followees, &d.Home} {
			integer(int64(len(csr.Offsets)))
			for _, value := range csr.Offsets {
				integer(int64(value))
			}
			for _, value := range csr.Values {
				integer(int64(value))
			}
		}
		for _, post := range d.Posts {
			integer(post.ID)
			integer(post.ReqID)
			integer(int64(post.Author))
			integer(post.Timestamp)
			text(post.Text)
			integer(int64(len(post.Mentions)))
			for _, value := range post.Mentions {
				integer(int64(value))
			}
			integer(int64(len(post.URLs)))
			for _, url := range post.URLs {
				text(url.Shortened)
				text(url.Expanded)
			}
			integer(int64(len(post.Media)))
			for _, media := range post.Media {
				integer(media.ID)
				text(media.Type)
			}
		}
		d.digest = hex.EncodeToString(sum.Sum(nil))
	})
	return d.digest
}

func buildCSR(rows [][]int32) CSR {
	offsets := make([]int32, len(rows)+1)
	total := 0
	for index, row := range rows {
		total += len(row)
		offsets[index+1] = int32(total)
	}
	values := make([]int32, 0, total)
	for _, row := range rows {
		values = append(values, row...)
	}
	return CSR{Offsets: offsets, Values: values}
}

// rankBy orders user indices by a key, descending, ties by index.
func rankBy(n int, key func(int) int) []int32 {
	order := make([]int32, n)
	for index := range order {
		order[index] = int32(index)
	}
	sort.SliceStable(order, func(i, j int) bool { return key(int(order[i])) > key(int(order[j])) })
	return order
}

const alphanumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func token(r *RNG, length int) string {
	out := make([]byte, length)
	for i := range out {
		out[i] = alphanumeric[r.Intn(len(alphanumeric))]
	}
	return string(out)
}
