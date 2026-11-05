package oracle

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Request is the HTTP request of an action on nginx's public routes: method, path and the
// query (GET) or form body (POST).
func Request(a Action) (method, path string, values url.Values) {
	switch a.Kind {
	case KindRegister:
		return http.MethodPost, "/wrk2-api/user/register", url.Values{
			"user_id": {strconv.FormatInt(a.User, 10)}, "username": {a.Name},
			"first_name": {"first" + a.Name}, "last_name": {"last" + a.Name}, "password": {"password" + a.Name},
		}
	case KindFollow:
		path = "/wrk2-api/user/unfollow"
		if a.On {
			path = "/wrk2-api/user/follow"
		}
		return http.MethodPost, path, url.Values{
			"user_id": {strconv.FormatInt(a.User, 10)}, "followee_id": {strconv.FormatInt(a.Followee, 10)},
		}
	case KindCompose:
		ids, types := make([]string, len(a.Media)), make([]string, len(a.Media))
		for i, item := range a.Media {
			ids[i], types[i] = strconv.Quote(item.ID), strconv.Quote(item.Type)
		}
		return http.MethodPost, "/wrk2-api/post/compose", url.Values{
			"username": {a.Name}, "user_id": {strconv.FormatInt(a.User, 10)}, "text": {a.Text},
			"media_ids": {"[" + strings.Join(ids, ",") + "]"}, "media_types": {"[" + strings.Join(types, ",") + "]"},
			"post_type": {"0"},
		}
	}
	path = "/wrk2-api/user-timeline/read"
	if a.Home {
		path = "/wrk2-api/home-timeline/read"
	}
	return http.MethodGet, path, url.Values{"user_id": {a.UserID}, "start": {a.Start}, "stop": {a.Stop}}
}

// Hook restarts one pod of a deployment or flushes a cache tier (the task's restart hook).
type Hook interface {
	Restart(ctx context.Context, deployment string) error
	Flush(ctx context.Context, cache string) error
}

// HTTPSystem is the application behind nginx-thrift's public API. Every request uses a new
// connection, so repeated reads spread over the replicas behind a Service.
type HTTPSystem struct {
	base string
	http *http.Client
	hook Hook
	// GatewayTimeout bounds the wait for the API to answer again after a restart.
	GatewayTimeout time.Duration
}

// NewHTTPSystem talks to base (nginx-thrift's URL); hook may be nil when no program restarts
// or flushes; transport nil is a fresh connection per request.
func NewHTTPSystem(base string, timeout time.Duration, transport http.RoundTripper, hook Hook) *HTTPSystem {
	if transport == nil {
		transport = &http.Transport{DisableKeepAlives: true}
	}
	return &HTTPSystem{
		base: strings.TrimRight(base, "/"), http: &http.Client{Timeout: timeout, Transport: transport},
		hook: hook, GatewayTimeout: 2 * time.Minute,
	}
}

// Invoke sends the action and records its times and answer.
func (h *HTTPSystem) Invoke(ctx context.Context, a Action) Call {
	method, path, values := Request(a)
	c := Call{Action: a}
	var request *http.Request
	var err error
	if method == http.MethodGet {
		request, err = http.NewRequestWithContext(ctx, method, h.base+path+"?"+values.Encode(), nil)
	} else {
		request, err = http.NewRequestWithContext(ctx, method, h.base+path, strings.NewReader(values.Encode()))
		if err == nil {
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}
	c.Start = time.Now()
	if err == nil {
		var reply *http.Response
		if reply, err = h.http.Do(request); err == nil {
			c.Status = reply.StatusCode
			c.Body, err = io.ReadAll(reply.Body)
			_ = reply.Body.Close()
		}
	}
	c.End = time.Now()
	if err != nil {
		c.Err = fmt.Sprintf("%s %s: %v", method, path, err)
	}
	return c
}

// Restart runs the hook, then waits until the API answers a read again.
func (h *HTTPSystem) Restart(ctx context.Context, deployment string) error {
	if h.hook == nil {
		return fmt.Errorf("no restart hook")
	}
	if err := h.hook.Restart(ctx, deployment); err != nil {
		return fmt.Errorf("restart hook for %s: %w", deployment, err)
	}
	return h.waitGateway(ctx)
}

// Flush runs the hook.
func (h *HTTPSystem) Flush(ctx context.Context, cache string) error {
	if h.hook == nil {
		return fmt.Errorf("no restart hook")
	}
	if err := h.hook.Flush(ctx, cache); err != nil {
		return fmt.Errorf("flush hook for %s: %w", cache, err)
	}
	return nil
}

// Ping is a read of a user outside every timeline: whether nginx answers.
func (h *HTTPSystem) Ping(ctx context.Context) error {
	c := h.Invoke(ctx, RawRead(false, "1", "0", "1"))
	if c.Err != "" {
		return fmt.Errorf("%s", c.Err)
	}
	if c.Status != http.StatusOK {
		return fmt.Errorf("HTTP %d %s", c.Status, clip(c.Body))
	}
	return nil
}

func (h *HTTPSystem) waitGateway(ctx context.Context) error {
	deadline := time.Now().Add(h.GatewayTimeout)
	for {
		err := h.Ping(ctx)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gateway not answering after the restart: %v", err)
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
