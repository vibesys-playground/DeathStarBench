package oracle

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/fakeapp"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// small is a dataset small enough for unit tests.
var small = dataset.Generate(5, dataset.Config{Users: 200, FolloweeScale: 4, MaxFollowees: 50, Reciprocity: 0, PostScale: 2, MaxPosts: 10})

var t0 = time.Unix(2_000_000_000, 0)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

// done is an action answered over [start, end] ms after t0.
func done(a Action, start, end int, status int, body []byte) Call {
	return Call{Action: a, Start: at(start), End: at(end), Status: status, Body: body}
}

func composeOK(a Action, start, end int) Call {
	return done(a, start, end, 200, []byte("Successfully upload post"))
}

func writeOK(a Action, start, end int) Call { return done(a, start, end, 200, []byte("Success!")) }

func post(author int64, name, marker string) Action {
	return Action{Kind: KindCompose, User: author, Name: name, Marker: marker, Text: "hello world #" + marker}
}

// served is a live post as the application would serve it.
func served(a Action, id int64, stamp int) timeline.Post {
	p := timeline.Post{
		PostID: strconv.FormatInt(id, 10), CreatorID: strconv.FormatInt(a.User, 10), Creator: a.Name,
		ReqID: "77", Text: a.Text, Timestamp: at(stamp).UnixMilli(), Media: a.Media,
	}
	return p
}

// wire encodes posts as nginx serves a timeline (cjson's {} for empty lists).
func wire(posts ...timeline.Post) []byte {
	if len(posts) == 0 {
		return []byte("{}")
	}
	list := make([]map[string]any, len(posts))
	for i, p := range posts {
		object := func(n int, item func(int) map[string]any) any {
			if n == 0 {
				return map[string]any{}
			}
			out := make([]any, n)
			for j := range out {
				out[j] = item(j)
			}
			return out
		}
		list[i] = map[string]any{
			"post_id": p.PostID, "req_id": p.ReqID, "text": p.Text, "timestamp": strconv.FormatInt(p.Timestamp, 10),
			"post_type": p.PostType, "creator": map[string]any{"user_id": p.CreatorID, "username": p.Creator},
			"user_mentions": object(len(p.Mentions), func(j int) map[string]any {
				return map[string]any{"user_id": p.Mentions[j].UserID, "username": p.Mentions[j].Username}
			}),
			"media": object(len(p.Media), func(j int) map[string]any {
				return map[string]any{"media_id": p.Media[j].ID, "media_type": p.Media[j].Type}
			}),
			"urls": object(len(p.URLs), func(j int) map[string]any {
				return map[string]any{"shortened_url": p.URLs[j].Shortened, "expanded_url": p.URLs[j].Expanded}
			}),
		}
	}
	encoded, err := json.Marshal(list)
	if err != nil {
		panic(err)
	}
	return encoded
}

func read(home bool, user int64, start, stop int, from, to int, posts ...timeline.Post) Call {
	return done(Read(home, user, start, stop), from, to, 200, wire(posts...))
}

// apply applies calls in order, failing the test on the first rejected one.
func apply(t *testing.T, m *Model, s *State, calls ...Call) {
	t.Helper()
	for _, c := range calls {
		if err := m.Apply(s, c); err != nil {
			t.Fatalf("%s rejected: %v", c.Action, err)
		}
	}
}

// rejects says why the call is not allowed in s (copied), failing if it is.
func rejects(t *testing.T, m *Model, s *State, c Call, want string) {
	t.Helper()
	_, err := m.Step(s, c)
	if err == nil {
		t.Fatalf("%s accepted, want an error with %q", c.Action, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("%s rejected with %v, want %q", c.Action, err, want)
	}
}

func accepts(t *testing.T, m *Model, s *State, c Call) *State {
	t.Helper()
	next, err := m.Step(s, c)
	if err != nil {
		t.Fatalf("%s rejected: %v", c.Action, err)
	}
	return next
}

// handlerSystem runs actions on an in-process fake without a network.
type handlerSystem struct {
	app *fakeapp.App
	// mutate, when set, may change a call's answer (a faulty application).
	mutate func(*Call)
}

func (h *handlerSystem) Invoke(_ context.Context, a Action) Call {
	method, path, values := Request(a)
	var request *http.Request
	if method == http.MethodGet {
		request = httptest.NewRequest(method, path+"?"+values.Encode(), nil)
	} else {
		request = httptest.NewRequest(method, path, strings.NewReader(values.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	recorder := httptest.NewRecorder()
	c := Call{Action: a, Start: time.Now()}
	h.app.ServeHTTP(recorder, request)
	c.End = time.Now()
	c.Status, c.Body = recorder.Code, recorder.Body.Bytes()
	if h.mutate != nil {
		h.mutate(&c)
	}
	return c
}

func (h *handlerSystem) Restart(_ context.Context, deployment string) error {
	h.app.Restart(deployment)
	return nil
}

func (h *handlerSystem) Flush(_ context.Context, cache string) error {
	h.app.Flush(cache)
	return nil
}
