package checker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// client talks to nginx's public routes. Every request uses a new
// connection, so repeated reads spread over the replicas behind a Service.
type client struct {
	base string
	http *http.Client
}

func newClient(base string, timeout time.Duration, transport http.RoundTripper) *client {
	if transport == nil {
		transport = &http.Transport{DisableKeepAlives: true}
	}
	return &client{base: strings.TrimRight(base, "/"), http: &http.Client{Timeout: timeout, Transport: transport}}
}

type response struct {
	status int
	body   []byte
}

func (c *client) do(ctx context.Context, method, path string, values url.Values) (response, error) {
	var request *http.Request
	var err error
	if method == http.MethodGet {
		request, err = http.NewRequestWithContext(ctx, method, c.base+path+"?"+values.Encode(), nil)
	} else {
		request, err = http.NewRequestWithContext(ctx, method, c.base+path, strings.NewReader(values.Encode()))
		if err == nil {
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}
	if err != nil {
		return response{}, err
	}
	reply, err := c.http.Do(request)
	if err != nil {
		return response{}, err
	}
	defer reply.Body.Close()
	body, err := io.ReadAll(reply.Body)
	return response{status: reply.StatusCode, body: body}, err
}

func (c *client) post(ctx context.Context, path string, values url.Values, want string) error {
	reply, err := c.do(ctx, http.MethodPost, path, values)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if reply.status != http.StatusOK || !strings.Contains(string(reply.body), want) {
		return fmt.Errorf("%s: HTTP %d %q, want 200 %q", path, reply.status, clip(reply.body), want)
	}
	return nil
}

func (c *client) register(ctx context.Context, u *user) error {
	return c.post(ctx, "/wrk2-api/user/register", url.Values{
		"user_id": {strconv.FormatInt(u.id, 10)}, "username": {u.name},
		"first_name": {"first" + u.name}, "last_name": {"last" + u.name}, "password": {"password" + u.name},
	}, "Success")
}

func (c *client) follow(ctx context.Context, u, v *user, follow bool) error {
	path := "/wrk2-api/user/unfollow"
	if follow {
		path = "/wrk2-api/user/follow"
	}
	return c.post(ctx, path, url.Values{"user_id": {strconv.FormatInt(u.id, 10)}, "followee_id": {strconv.FormatInt(v.id, 10)}}, "Success")
}

func (c *client) compose(ctx context.Context, u *user, text string, media []timeline.Media) error {
	ids, types := make([]string, len(media)), make([]string, len(media))
	for i, item := range media {
		ids[i], types[i] = strconv.Quote(item.ID), strconv.Quote(item.Type)
	}
	return c.post(ctx, "/wrk2-api/post/compose", url.Values{
		"username": {u.name}, "user_id": {strconv.FormatInt(u.id, 10)}, "text": {text},
		"media_ids": {"[" + strings.Join(ids, ",") + "]"}, "media_types": {"[" + strings.Join(types, ",") + "]"},
		"post_type": {"0"},
	}, "Successfully upload post")
}

// read returns a timeline page.
func (c *client) read(ctx context.Context, home bool, id int64, start, stop int) ([]timeline.Post, error) {
	reply, err := c.readRaw(ctx, home, strconv.FormatInt(id, 10), strconv.Itoa(start), strconv.Itoa(stop))
	if err != nil {
		return nil, err
	}
	if reply.status != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d %q", kindName(home), reply.status, clip(reply.body))
	}
	posts, err := timeline.Parse(reply.body)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kindName(home), err)
	}
	return posts, nil
}

func (c *client) readRaw(ctx context.Context, home bool, id, start, stop string) (response, error) {
	path := "/wrk2-api/user-timeline/read"
	if home {
		path = "/wrk2-api/home-timeline/read"
	}
	return c.do(ctx, http.MethodGet, path, url.Values{"user_id": {id}, "start": {start}, "stop": {stop}})
}

func kindName(home bool) string {
	if home {
		return "home timeline"
	}
	return "user timeline"
}

func clip(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > 120 {
		text = text[:120]
	}
	return text
}
