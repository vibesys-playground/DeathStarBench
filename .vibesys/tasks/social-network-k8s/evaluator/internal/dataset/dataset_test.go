package dataset

import (
	"regexp"
	"strings"
	"testing"
)

// defaultDigest pins the task's dataset: the seed Job, the load generator
// and the checker all generate it, so any change to the generator changes
// what is seeded and must be deliberate.
const defaultDigest = "af53262d9dc775b2214a481d8165fcfec987c13b311161923167488f07035348"

func TestDefaultDatasetIsPinned(t *testing.T) {
	data := Default()
	if got := data.Digest(); got != defaultDigest {
		t.Fatalf("dataset digest = %s, want %s (counts %+v)", got, defaultDigest, data.Counts())
	}
	if again := Generate(DefaultSeed, DefaultConfig()).Digest(); again != defaultDigest {
		t.Fatalf("second generation digest = %s", again)
	}
	counts := data.Counts()
	t.Logf("counts %+v", counts)
	if counts.Users != 20000 || counts.Posts < 80000 || counts.Follows < 250000 || counts.MaxFollowers < 1000 {
		t.Fatalf("unexpected shape %+v", counts)
	}
}

func TestDatasetInvariants(t *testing.T) {
	data := Generate(7, Config{Users: 2000, FolloweeScale: 10, MaxFollowees: 300, Reciprocity: 0.2, PostScale: 4, MaxPosts: 100})
	if data.Digest() == Generate(8, data.Config).Digest() {
		t.Fatal("different seeds gave the same dataset")
	}
	mention := regexp.MustCompile(`@[a-zA-Z0-9-_]+`)
	url := regexp.MustCompile(`(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)`)
	for index, post := range data.Posts {
		if post.ID != FirstPostID+int64(index) {
			t.Fatalf("post %d id %d", index, post.ID)
		}
		if index > 0 && post.Timestamp <= data.Posts[index-1].Timestamp {
			t.Fatalf("post %d timestamp not increasing", index)
		}
		if post.Timestamp >= WindowEnd || post.Timestamp < WindowEnd-WindowLength-int64(len(data.Posts)) {
			t.Fatalf("post %d timestamp %d outside the window", index, post.Timestamp)
		}
		// The text service's parsing must find exactly the post's mentions
		// and URLs, and the text must end with the last URL.
		names := mention.FindAllString(post.Text, -1)
		if len(names) != len(post.Mentions) {
			t.Fatalf("post %d mentions %v, want %d", index, names, len(post.Mentions))
		}
		for _, name := range names {
			found := false
			for _, v := range post.Mentions {
				found = found || (name[1:] == data.Users[v].Username && data.Follows(int(post.Author), int(v)))
			}
			if !found {
				t.Fatalf("post %d mention %s not in %v", index, name, post.Mentions)
			}
		}
		// Stored text holds the shortened URL in place of the expanded one.
		if len(post.URLs) > 1 || (len(post.URLs) == 1 && !strings.HasSuffix(post.Text, " "+post.URLs[0].Shortened)) ||
			(len(post.URLs) == 0 && strings.Contains(post.Text, "http")) {
			t.Fatalf("post %d text %q urls %+v", index, post.Text, post.URLs)
		}
		if len(post.URLs) == 1 && url.FindString(post.URLs[0].Expanded) != post.URLs[0].Expanded {
			t.Fatalf("post %d expanded url %q is not one URL token", index, post.URLs[0].Expanded)
		}
	}
	// Home timelines: posts of followees plus posts mentioning the user,
	// newest first, without duplicates.
	for u := 0; u < len(data.Users); u += 37 {
		var want []int32
		for index := len(data.Posts) - 1; index >= 0; index-- {
			post := &data.Posts[index]
			mentioned := false
			for _, v := range post.Mentions {
				mentioned = mentioned || int(v) == u
			}
			if data.Follows(u, int(post.Author)) || mentioned {
				want = append(want, int32(index))
			}
		}
		got := data.Home.Row(u)
		if len(got) != len(want) {
			t.Fatalf("user %d home %d entries, want %d", u, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("user %d home entry %d = %d, want %d", u, i, got[i], want[i])
			}
		}
		for _, p := range data.UserPosts.Row(u) {
			if int(data.Posts[p].Author) != u {
				t.Fatalf("user %d timeline holds post %d of %d", u, p, data.Posts[p].Author)
			}
		}
	}
	for u := 0; u < len(data.Users); u++ {
		for _, v := range data.Followees.Row(u) {
			if int(v) == u {
				t.Fatalf("user %d follows itself", u)
			}
			found := false
			for _, w := range data.Followers.Row(int(v)) {
				found = found || int(w) == u
			}
			if !found {
				t.Fatalf("edge %d -> %d missing from followers", u, v)
			}
		}
	}
}
