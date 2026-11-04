package socialnetwork

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"vibesys/microservice-evaluator/api"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

const composeSuccessBody = "Successfully upload post"

// timestampSlack bounds the difference between the application's and the
// load generator's clocks when checking a live post's timestamp.
const timestampSlack = 5 * time.Second

func (a *Application) ValidateOperation(operation api.Operation, operationPlan api.OperationPlan, results []api.ProtocolResult) api.ValidationResult {
	state, ok := operationPlan.State.(*plan)
	if !ok || state == nil {
		return invalid("invalid_plan", fmt.Sprintf("unexpected plan state %T", operationPlan.State))
	}
	if len(results) != len(operationPlan.Invocations) {
		return invalid("result_count", fmt.Sprintf("got %d protocol results, want %d", len(results), len(operationPlan.Invocations)))
	}
	switch state.kind {
	case opUserTimeline, opHomeTimeline:
		body, failure := okBody(results[0])
		if failure != nil {
			return *failure
		}
		if err := a.checkTimeline(state.kind, state.user, state.start, body, state.built, nil); err != nil {
			return invalid("response_value", fmt.Sprintf("%s of user %d [%d, %d): %v", state.kind, a.data.Users[state.user].ID, state.start, state.start+PageSize, err))
		}
	case opCompose:
		body, failure := okBody(results[0])
		accepted := failure == nil && strings.TrimSpace(string(body)) == composeSuccessBody
		a.model.mu.Lock()
		a.model.endPost(state.post, accepted, time.Now())
		a.model.mu.Unlock()
		if failure != nil {
			return *failure
		}
		if !accepted {
			return invalid("response_value", fmt.Sprintf("compose response %q, want %q", clip(body), composeSuccessBody))
		}
		body, failure = okBody(results[1])
		if failure != nil {
			failure.ErrorCategory = "read_your_write"
			return *failure
		}
		if err := a.checkTimeline(opUserTimeline, state.user, 0, body, state.built, state.post); err != nil {
			return invalid("read_your_write", fmt.Sprintf("user timeline of author %d after compose: %v", a.data.Users[state.user].ID, err))
		}
	case opFollow, opUnfollow:
		body, failure := okBody(results[0])
		accepted := failure == nil && strings.Contains(string(body), "Success")
		a.model.mu.Lock()
		a.model.endChange(state.edge, accepted, time.Now())
		a.model.mu.Unlock()
		if failure != nil {
			return *failure
		}
		if !accepted {
			return invalid("response_value", fmt.Sprintf("%s response %q, want Success", state.kind, clip(body)))
		}
	default:
		return invalid("invalid_plan", "unknown plan kind "+state.kind)
	}
	return api.ValidationResult{Success: true}
}

func okBody(result api.ProtocolResult) ([]byte, *api.ValidationResult) {
	if !result.TransportSuccess {
		failure := invalid(result.ErrorCategory, result.ErrorMessage)
		return nil, &failure
	}
	response, ok := result.Payload.(api.HTTPResponse)
	if !ok {
		failure := invalid("invalid_response", fmt.Sprintf("expected HTTP response, got %T", result.Payload))
		return nil, &failure
	}
	if response.StatusCode != http.StatusOK {
		failure := invalid("http_status", fmt.Sprintf("HTTP %d, want 200: %s", response.StatusCode, clip(response.Body)))
		return nil, &failure
	}
	return response.Body, nil
}

// checkTimeline checks a page of user's timeline read at readStart. force,
// if set, is a live post that must be in it (read your write).
func (a *Application) checkTimeline(kind string, user int32, start int, body []byte, readStart time.Time, force *livePost) error {
	posts, err := timeline.Parse(body)
	if err != nil {
		return err
	}
	m := a.model
	m.mu.Lock()
	defer m.mu.Unlock()
	var candidates []candidate
	if kind == opHomeTimeline {
		candidates = m.homeCandidates(user, readStart)
	} else {
		candidates = m.userCandidates(user, readStart)
	}
	seededAt := make(map[int32]int, len(candidates))
	liveAt := map[*livePost]int{}
	for index, c := range candidates {
		if c.seeded >= 0 {
			seededAt[c.seeded] = index
		} else {
			if c.live == force {
				candidates[index].must = true
			}
			liveAt[c.live] = index
		}
	}
	now := time.Now()
	matched := make([]int, len(posts))
	for i := range posts {
		post := &posts[i]
		if seeded, ok := m.seededIDs[post.PostID]; ok {
			index, allowed := seededAt[seeded]
			if !allowed {
				return fmt.Errorf("unexpected: %s does not belong in this timeline", timeline.Describe(post))
			}
			expected := timeline.Seeded(a.data, seeded)
			if !timeline.Same(&expected, post) {
				return fmt.Errorf("content: %s differs from the stored %s", timeline.Describe(post), timeline.Describe(&expected))
			}
			matched[i] = index
			continue
		}
		p := m.byPostID[post.PostID]
		if p == nil {
			p = m.byMarker[markerOf(post.Text)]
			if p == nil {
				return fmt.Errorf("unexpected: %s was never composed", timeline.Describe(post))
			}
			if p.learned != nil {
				return fmt.Errorf("content: %s was first served as post %s", timeline.Describe(post), p.learned.PostID)
			}
			if err := a.checkLive(p, post, now); err != nil {
				return fmt.Errorf("content: %s: %v", timeline.Describe(post), err)
			}
			m.learn(p, post)
		} else if !timeline.Same(p.learned, post) {
			return fmt.Errorf("content: %s differs from its first read %s", timeline.Describe(post), timeline.Describe(p.learned))
		}
		index, allowed := liveAt[p]
		if !allowed {
			return fmt.Errorf("unexpected: %s does not belong in this timeline", timeline.Describe(post))
		}
		matched[i] = index
	}
	window := make([]timeline.Candidate, len(candidates))
	for index, c := range candidates {
		window[index] = timeline.Candidate{Timestamp: m.timestampOf(c), Must: c.must}
	}
	return timeline.Window(window, posts, matched, start, start+PageSize, func(index int) string {
		c := candidates[index]
		if c.seeded >= 0 {
			wire := timeline.Seeded(a.data, c.seeded)
			return timeline.Describe(&wire)
		}
		if c.live.learned != nil {
			return timeline.Describe(c.live.learned)
		}
		return fmt.Sprintf("post #%s by %d (acknowledged %s)", c.live.marker, a.data.Users[c.live.author].ID, c.live.ackedAt.Format(time.StampMilli))
	})
}

// checkLive checks a live post's first appearance against what was sent.
func (a *Application) checkLive(p *livePost, post *timeline.Post, now time.Time) error {
	author := &a.data.Users[p.author]
	if post.CreatorID != strconv.FormatInt(author.ID, 10) || post.Creator != author.Username {
		return fmt.Errorf("creator (%s, %s), want (%d, %s)", post.CreatorID, post.Creator, author.ID, author.Username)
	}
	if post.PostType != 0 {
		return fmt.Errorf("post_type %d, want 0", post.PostType)
	}
	if p.url == "" {
		if post.Text != p.text || len(post.URLs) != 0 {
			return fmt.Errorf("text %q urls %v, want %q", post.Text, post.URLs, p.text)
		}
	} else {
		if len(post.URLs) != 1 || post.URLs[0].Expanded != p.url || post.URLs[0].Shortened == "" ||
			post.Text != strings.TrimSuffix(p.text, p.url)+post.URLs[0].Shortened {
			return fmt.Errorf("text %q urls %v, want the text sent with %s shortened", post.Text, post.URLs, p.url)
		}
	}
	var mentions []timeline.Mention
	for _, v := range p.mentions {
		mentions = append(mentions, timeline.Mention{UserID: strconv.FormatInt(a.data.Users[v].ID, 10), Username: a.data.Users[v].Username})
	}
	if !timeline.SameMentions(mentions, post.Mentions) {
		return fmt.Errorf("mentions %v, want %v", post.Mentions, mentions)
	}
	if len(post.Media) != len(p.media) {
		return fmt.Errorf("media %v, want %v", post.Media, p.media)
	}
	for i := range p.media {
		if post.Media[i] != p.media[i] {
			return fmt.Errorf("media %v, want %v", post.Media, p.media)
		}
	}
	if post.Timestamp < p.sentAt.Add(-timestampSlack).UnixMilli() || post.Timestamp > now.Add(timestampSlack).UnixMilli() {
		return fmt.Errorf("timestamp %d outside the compose's time %d..%d (ms)", post.Timestamp, p.sentAt.UnixMilli(), now.UnixMilli())
	}
	return nil
}

func clip(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > 120 {
		text = text[:120]
	}
	return text
}

func invalid(category, message string) api.ValidationResult {
	return api.ValidationResult{ErrorCategory: category, ErrorMessage: message}
}
