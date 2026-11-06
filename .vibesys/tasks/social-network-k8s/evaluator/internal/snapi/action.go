// Package snapi is Social Network's public API as the task's workloads and oracle see it: an
// Action (what one call sends, never an expected answer), its HTTP request on nginx-thrift's
// routes, and a Call (an action as executed, with its times and answer).
package snapi

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// Kind is what a call does.
type Kind string

const (
	KindRegister Kind = "register" // POST /wrk2-api/user/register
	KindFollow   Kind = "follow"   // POST /wrk2-api/user/follow or /unfollow
	KindCompose  Kind = "compose"  // POST /wrk2-api/post/compose
	KindRead     Kind = "read"     // GET /wrk2-api/user-timeline/read or /home-timeline/read
)

// PageSize is the posts of one timeline page the benchmark reads.
const PageSize = 10

// Action is what one call sends: a stimulus, never an expected answer.
type Action struct {
	Kind Kind `json:"kind"`
	// User is the new user (register), the follower (follow), the author (compose) or, for a
	// read whose UserID is a number, that user.
	User int64 `json:"user,omitempty"`
	// Name is the username sent (register, compose).
	Name string `json:"name,omitempty"`
	// Followee and On: follow (true) or unfollow of User -> Followee.
	Followee int64 `json:"followee,omitempty"`
	On       bool  `json:"on,omitempty"`
	// Compose: Text as sent, with Marker (its unique "#m..." token, without "#"), the users
	// it mentions, URL (the expanded URL ending the text, or "") and Media.
	Text     string           `json:"text,omitempty"`
	Marker   string           `json:"marker,omitempty"`
	Mentions []int64          `json:"mentions,omitempty"`
	URL      string           `json:"url,omitempty"`
	Media    []timeline.Media `json:"media,omitempty"`
	// Read: the home (or user) timeline, with the query values as sent (any text, so that
	// invalid requests can be sent too).
	Home                bool   `json:"home,omitempty"`
	UserID, Start, Stop string `json:",omitempty"`
	// Holds are markers of posts composed earlier in the same operation, acknowledged before
	// this read started (the benchmark's compose then read your write).
	Holds []string `json:"holds,omitempty"`
}

// Register is the registration of user id with username name.
func Register(id int64, name string) Action {
	return Action{Kind: KindRegister, User: id, Name: name}
}

// Follow is a follow (on) or unfollow of u -> v.
func Follow(u, v int64, on bool) Action {
	return Action{Kind: KindFollow, User: u, Followee: v, On: on}
}

// Read is a read of user's timeline [start, stop).
func Read(home bool, user int64, start, stop int) Action {
	return Action{
		Kind: KindRead, Home: home, User: user,
		UserID: strconv.FormatInt(user, 10), Start: strconv.Itoa(start), Stop: strconv.Itoa(stop),
	}
}

// RawRead is a read with arbitrary query values.
func RawRead(home bool, userID, start, stop string) Action {
	a := Action{Kind: KindRead, Home: home, UserID: userID, Start: start, Stop: stop}
	if id, err := strconv.ParseInt(userID, 10, 64); err == nil {
		a.User = id
	}
	return a
}

func (a Action) followName() string {
	if a.On {
		return "follow"
	}
	return "unfollow"
}

func (a Action) timelineName() string {
	if a.Home {
		return "home timeline"
	}
	return "user timeline"
}

// String is a short form for messages.
func (a Action) String() string {
	switch a.Kind {
	case KindRegister:
		return fmt.Sprintf("register %d (%s)", a.User, a.Name)
	case KindFollow:
		return fmt.Sprintf("%s %d -> %d", a.followName(), a.User, a.Followee)
	case KindCompose:
		if a.Text == "" {
			return fmt.Sprintf("compose by %d without text", a.User)
		}
		return fmt.Sprintf("compose #%s by %d", a.Marker, a.User)
	case KindRead:
		return fmt.Sprintf("%s of %s [%s, %s)", a.timelineName(), a.UserID, a.Start, a.Stop)
	}
	return string(a.Kind)
}

// Call is one action as executed: when it was sent and answered, and the answer.
type Call struct {
	Action Action `json:"action"`
	// Start is when the request was sent, End when its answer was complete (or it failed).
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// Status and Body are the HTTP answer (Status 0: none); Err a transport failure.
	Status int    `json:"status"`
	Body   []byte `json:"body,omitempty"`
	Err    string `json:"error,omitempty"`
}

func (c Call) answer() string {
	if c.Err != "" {
		return c.Err
	}
	return fmt.Sprintf("HTTP %d %q", c.Status, clip(c.Body))
}

func clip(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > 120 {
		text = text[:120]
	}
	return text
}

// MarkerOf returns the marker token ("m...") of a live post's text, or "".
func MarkerOf(text string) string {
	at := strings.Index(text, "#m")
	if at < 0 {
		return ""
	}
	end := at + 1
	for end < len(text) && text[end] != ' ' {
		end++
	}
	return text[at+1 : end]
}
