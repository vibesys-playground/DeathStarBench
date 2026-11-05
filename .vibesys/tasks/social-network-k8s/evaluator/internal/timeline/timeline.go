// Package timeline parses the Social Network's timeline responses and checks
// a returned page against a model that knows which posts must, may or must
// not be in the timeline. It is shared by the benchmark adapter and the
// checker.
package timeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
)

// Post is one post as nginx serves it (wrk2-api read routes).
type Post struct {
	PostID    string
	CreatorID string
	Creator   string
	ReqID     string
	Text      string
	Timestamp int64
	PostType  int
	Mentions  []Mention
	Media     []Media
	URLs      []URL
}

type Mention struct{ UserID, Username string }
type Media struct{ ID, Type string }
type URL struct{ Shortened, Expanded string }

var postFields = []string{"creator", "media", "post_id", "post_type", "req_id", "text", "timestamp", "urls", "user_mentions"}

var decimal = regexp.MustCompile(`^-?[0-9]+$`)

// Parse decodes a timeline body strictly: a JSON array of posts with exactly
// the pinned fields and types. Empty lists, and an empty timeline, may be
// encoded as {} (cjson).
func Parse(body []byte) ([]Post, error) {
	if bytes.Equal(bytes.TrimSpace(body), []byte("{}")) {
		return nil, nil // cjson encodes an empty timeline as {}
	}
	var raw []map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("timeline must be a JSON array of objects: %v", err)
	}
	if raw == nil {
		return nil, fmt.Errorf("timeline must be a JSON array, got null")
	}
	posts := make([]Post, len(raw))
	for index, fields := range raw {
		keys := make([]string, 0, len(fields))
		for key := range fields {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if strings.Join(keys, ",") != strings.Join(postFields, ",") {
			return nil, fmt.Errorf("post %d fields %v, want %v", index, keys, postFields)
		}
		post := &posts[index]
		var err error
		str := func(name string, target *string, numeric bool) {
			if err != nil {
				return
			}
			if e := json.Unmarshal(fields[name], target); e != nil {
				err = fmt.Errorf("post %d %s must be a string", index, name)
			} else if numeric && !decimal.MatchString(*target) {
				err = fmt.Errorf("post %d %s %q must be a decimal integer", index, name, *target)
			}
		}
		var timestamp string
		str("post_id", &post.PostID, true)
		str("req_id", &post.ReqID, true)
		str("text", &post.Text, false)
		str("timestamp", &timestamp, true)
		if err != nil {
			return nil, err
		}
		post.Timestamp, _ = strconv.ParseInt(timestamp, 10, 64)
		var creator map[string]json.RawMessage
		if json.Unmarshal(fields["creator"], &creator) != nil || len(creator) != 2 ||
			json.Unmarshal(creator["user_id"], &post.CreatorID) != nil || json.Unmarshal(creator["username"], &post.Creator) != nil {
			return nil, fmt.Errorf("post %d creator must be {user_id, username} strings", index)
		}
		var postType float64
		if json.Unmarshal(fields["post_type"], &postType) != nil || math.Trunc(postType) != postType {
			return nil, fmt.Errorf("post %d post_type must be an integer", index)
		}
		post.PostType = int(postType)
		lists := []struct {
			name   string
			fields [2]string
			add    func(a, b string)
		}{
			{"user_mentions", [2]string{"user_id", "username"}, func(a, b string) { post.Mentions = append(post.Mentions, Mention{a, b}) }},
			{"media", [2]string{"media_id", "media_type"}, func(a, b string) { post.Media = append(post.Media, Media{a, b}) }},
			{"urls", [2]string{"shortened_url", "expanded_url"}, func(a, b string) { post.URLs = append(post.URLs, URL{a, b}) }},
		}
		for _, list := range lists {
			if err := parseList(fields[list.name], list.fields, list.add); err != nil {
				return nil, fmt.Errorf("post %d %s: %v", index, list.name, err)
			}
		}
	}
	return posts, nil
}

func parseList(raw json.RawMessage, names [2]string, add func(a, b string)) error {
	if trimmed := bytes.TrimSpace(raw); bytes.Equal(trimmed, []byte("{}")) {
		return nil
	}
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return fmt.Errorf("must be an array or {}")
	}
	for index, item := range items {
		var a, b string
		if len(item) != 2 || json.Unmarshal(item[names[0]], &a) != nil || json.Unmarshal(item[names[1]], &b) != nil || a == "" || b == "" {
			return fmt.Errorf("item %d must be {%s, %s} non-empty strings", index, names[0], names[1])
		}
		add(a, b)
	}
	return nil
}

// Same reports whether two posts are identical, mentions compared as sets.
func Same(a, b *Post) bool {
	if a.PostID != b.PostID || a.CreatorID != b.CreatorID || a.Creator != b.Creator || a.ReqID != b.ReqID ||
		a.Text != b.Text || a.Timestamp != b.Timestamp || a.PostType != b.PostType ||
		len(a.Mentions) != len(b.Mentions) || len(a.Media) != len(b.Media) || len(a.URLs) != len(b.URLs) {
		return false
	}
	for i := range a.Media {
		if a.Media[i] != b.Media[i] {
			return false
		}
	}
	for i := range a.URLs {
		if a.URLs[i] != b.URLs[i] {
			return false
		}
	}
	return SameMentions(a.Mentions, b.Mentions)
}

// SameMentions compares mention lists as sets.
func SameMentions(a, b []Mention) bool {
	if len(a) != len(b) {
		return false
	}
	key := func(list []Mention) []string {
		out := make([]string, len(list))
		for i, m := range list {
			out[i] = m.UserID + "/" + m.Username
		}
		sort.Strings(out)
		return out
	}
	ka, kb := key(a), key(b)
	for i := range ka {
		if ka[i] != kb[i] {
			return false
		}
	}
	return true
}

// Describe is a short form of a post for error messages.
func Describe(post *Post) string {
	text := post.Text
	if len(text) > 40 {
		text = text[:40] + "..."
	}
	return fmt.Sprintf("post %s by %s at %d %q", post.PostID, post.CreatorID, post.Timestamp, text)
}

// Candidate is one post the model allows in a timeline.
type Candidate struct {
	// Timestamp is the post's timestamp, or 0 while the model does not know
	// it (a post composed but not yet seen). An unknown timestamp lies in
	// [Lo, Hi] when Hi is set, and anywhere otherwise.
	Timestamp int64
	// Must: the post is certainly in the timeline; otherwise it may be.
	Must bool
	// Lo and Hi bound an unknown timestamp (Hi = 0: unbounded).
	Lo, Hi int64
}

// bounds is the range the candidate's timestamp lies in.
func (c Candidate) bounds() (lo, hi int64) {
	switch {
	case c.Timestamp != 0:
		return c.Timestamp, c.Timestamp
	case c.Hi != 0:
		return c.Lo, c.Hi
	}
	return math.MinInt64, math.MaxInt64
}

// Window checks that page is timeline[start:stop] for some timeline holding
// every Must candidate, any subset of the other candidates and nothing else,
// ordered newest first with ties in any order. matched[i] is the candidate
// index of page[i] (the caller matched posts to candidates and rejected
// posts that match none). describe names a candidate for messages.
func Window(candidates []Candidate, page []Post, matched []int, start, stop int, describe func(int) string) error {
	for i := 1; i < len(page); i++ {
		if page[i].Timestamp > page[i-1].Timestamp {
			return fmt.Errorf("order: %s follows older %s", Describe(&page[i]), Describe(&page[i-1]))
		}
	}
	inPage := make(map[int]bool, len(page))
	for i, index := range matched {
		if inPage[index] {
			return fmt.Errorf("duplicate: %s appears twice", Describe(&page[i]))
		}
		inPage[index] = true
	}
	want := stop - start
	if len(page) > want {
		return fmt.Errorf("page holds %d posts, asked for %d", len(page), want)
	}
	if len(page) == 0 {
		must := 0
		for _, candidate := range candidates {
			if candidate.Must {
				must++
			}
		}
		if must > start {
			return fmt.Errorf("page [%d, %d) is empty, but the timeline holds at least %d posts", start, stop, must)
		}
		return nil
	}
	// A short page ends the timeline: every required post not in it precedes it.
	hi, lo, short := page[0].Timestamp, page[len(page)-1].Timestamp, len(page) < want
	before, maybeBefore, mustOutside := 0, 0, 0
	for index, candidate := range candidates {
		if inPage[index] {
			continue
		}
		low, high := candidate.bounds()
		canBefore := high >= hi         // newer than the page's first post, or tied with it
		canAfter := !short && low <= lo // older than the page's last post, or tied with it
		if candidate.Must {
			mustOutside++
			if !canBefore && !canAfter {
				if short && high < lo {
					return fmt.Errorf("missing: %s is older than the short page's last post", describe(index))
				}
				if !short && low > lo {
					return fmt.Errorf("missing: %s lies within the page", describe(index))
				}
				return fmt.Errorf("missing: %s belongs in the page", describe(index))
			}
			if low > hi {
				before++
			}
		}
		if canBefore {
			maybeBefore++
		}
	}
	if short && mustOutside > before {
		before = mustOutside
	}
	if start < before {
		return fmt.Errorf("page [%d, %d) starts too new: %d newer posts must precede %s", start, stop, before, Describe(&page[0]))
	}
	if start > maybeBefore {
		return fmt.Errorf("page [%d, %d) starts too old: at most %d posts can precede %s", start, stop, maybeBefore, Describe(&page[0]))
	}
	return nil
}

// Seeded is a dataset post as the API serves it.
func Seeded(data *dataset.Dataset, index int32) Post {
	post := &data.Posts[index]
	author := &data.Users[post.Author]
	wire := Post{
		PostID: strconv.FormatInt(post.ID, 10), CreatorID: strconv.FormatInt(author.ID, 10), Creator: author.Username,
		ReqID: strconv.FormatInt(post.ReqID, 10), Text: post.Text, Timestamp: post.Timestamp,
	}
	for _, v := range post.Mentions {
		wire.Mentions = append(wire.Mentions, Mention{UserID: strconv.FormatInt(data.Users[v].ID, 10), Username: data.Users[v].Username})
	}
	for _, item := range post.Media {
		wire.Media = append(wire.Media, Media{ID: strconv.FormatInt(item.ID, 10), Type: item.Type})
	}
	for _, url := range post.URLs {
		wire.URLs = append(wire.URLs, URL{Shortened: url.Shortened, Expanded: url.Expanded})
	}
	return wire
}
