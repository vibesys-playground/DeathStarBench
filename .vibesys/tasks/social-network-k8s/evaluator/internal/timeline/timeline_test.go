package timeline

import (
	"strings"
	"testing"
)

func TestParseAcceptsThePinnedEncoding(t *testing.T) {
	body := `[{"post_id":"7","creator":{"user_id":"1","username":"user1"},"req_id":"9","text":"a b",
		"user_mentions":{},"media":[{"media_id":"5","media_type":"png"}],"urls":{},"timestamp":"100","post_type":0}]`
	posts, err := Parse([]byte(body))
	if err != nil || len(posts) != 1 || posts[0].Timestamp != 100 || len(posts[0].Media) != 1 {
		t.Fatalf("Parse = %+v, %v", posts, err)
	}
	if posts, err := Parse([]byte("{}\n")); err != nil || len(posts) != 0 {
		t.Fatalf("empty timeline {}: %v", err)
	}
	for _, bad := range []string{
		`null`, `[{"post_id":"7"}]`,
		strings.Replace(body, `"timestamp":"100"`, `"timestamp":100`, 1),
		strings.Replace(body, `"post_id":"7"`, `"post_id":"x7"`, 1),
		strings.Replace(body, `"post_type":0`, `"post_type":0.5`, 1),
		strings.Replace(body, `"urls":{}`, `"urls":{"a":1}`, 1),
		strings.Replace(body, `"media_type":"png"`, `"media_type":""`, 1),
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("Parse accepted %s", bad)
		}
	}
}

func TestWindow(t *testing.T) {
	describe := func(i int) string { return "candidate" }
	// Candidates 0..4 with timestamps 50, 40, 30, 20, 10, all must.
	musts := []Candidate{{Timestamp: 50, Must: true}, {Timestamp: 40, Must: true}, {Timestamp: 30, Must: true}, {Timestamp: 20, Must: true}, {Timestamp: 10, Must: true}}
	check := func(candidates []Candidate, start, stop int, indices []int, wantErr string) {
		t.Helper()
		posts := make([]Post, len(indices))
		for i, index := range indices {
			posts[i] = Post{PostID: "p", Timestamp: candidates[index].Timestamp}
			if candidates[index].Timestamp == 0 {
				posts[i].Timestamp = 60
			}
		}
		err := Window(candidates, posts, indices, start, stop, describe)
		if (err == nil) != (wantErr == "") || (err != nil && !strings.Contains(err.Error(), wantErr)) {
			t.Fatalf("Window(start %d, %v) = %v, want %q", start, indices, err, wantErr)
		}
	}
	check(musts, 0, 3, []int{0, 1, 2}, "")
	check(musts, 3, 6, []int{3, 4}, "")
	check(musts, 0, 3, []int{0, 2}, "missing")        // gap
	check(musts, 0, 3, []int{1, 2, 3}, "too new")     // skipped the newest
	check(musts, 2, 4, []int{0, 1}, "too old")        // window too new
	check(musts, 0, 3, []int{1, 0}, "order")          // not newest first
	check(musts, 0, 3, []int{0, 0}, "duplicate")      // repeated
	check(musts, 3, 6, []int{3}, "short page")        // ended early
	check(musts, 0, 2, []int{0, 1, 2}, "asked for 2") // too long
	check(musts, 5, 8, nil, "")
	check(musts, 4, 8, nil, "empty")
	// An optional post may be present or absent, and shifts the window.
	optional := append([]Candidate{{Timestamp: 55, Must: false}}, musts...)
	check(optional, 0, 2, []int{0, 1}, "")
	check(optional, 0, 2, []int{1, 2}, "")
	check(optional, 1, 3, []int{1, 2}, "")
	check(optional, 1, 3, []int{2, 3}, "")
	check(optional, 2, 3, []int{1}, "too old")
	// A post of unknown timestamp may sit anywhere.
	unknown := append([]Candidate{{Timestamp: 0, Must: true}}, musts...)
	check(unknown, 0, 2, []int{1, 2}, "")
	check(unknown, 1, 3, []int{1, 2}, "")
	// A post of unknown timestamp within known bounds.
	newer := append([]Candidate{{Lo: 51, Hi: 70, Must: true}}, musts...)
	check(newer, 0, 2, []int{1, 2}, "too new") // certainly newer than the page
	check(newer, 1, 3, []int{1, 2}, "")
	inside := append([]Candidate{{Lo: 31, Hi: 39, Must: true}}, musts...)
	check(inside, 0, 3, []int{1, 2, 3}, "missing") // certainly between 40 and 20
	older := append([]Candidate{{Lo: 1, Hi: 5, Must: true}}, musts...)
	check(older, 3, 6, []int{4, 5}, "short page")
	check(older, 3, 5, []int{4, 5}, "")
	vague := append([]Candidate{{Lo: 1, Hi: 45, Must: true}}, musts...)
	check(vague, 0, 2, []int{1, 2}, "") // may be anywhere below 45
}
