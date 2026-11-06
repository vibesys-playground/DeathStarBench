package snapi

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

// HTTPSystem is the application behind nginx-thrift's public API. Every request uses a new
// connection, so repeated reads spread over the replicas behind a Service.
type HTTPSystem struct {
	base string
	http *http.Client
}

// NewHTTPSystem talks to base (nginx-thrift's URL); transport nil is a fresh connection per
// request.
func NewHTTPSystem(base string, timeout time.Duration, transport http.RoundTripper) *HTTPSystem {
	if transport == nil {
		transport = &http.Transport{DisableKeepAlives: true}
	}
	return &HTTPSystem{base: strings.TrimRight(base, "/"), http: &http.Client{Timeout: timeout, Transport: transport}}
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
