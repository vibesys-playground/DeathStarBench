package socialnetwork

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"vibesys/microservice-evaluator/api"
	"vibesys/social-network-k8s-evaluator/internal/dataset"
)

// Streams of dataset.Mix(sample.Random, stream) per choice of a sample.
const (
	streamUser = iota
	streamPage
	streamOther
	streamRetry = 1000
)

// plan is an operation's state between BuildOperation and validation.
type plan struct {
	kind  string
	user  int32 // reader, author or follower (user index)
	start int
	built time.Time
	post  *livePost
	edge  *change
}

// Page draws a timeline page start: the first page 80% of the time, the
// second 12%, the third 5%, pages 4-7 3%.
func Page(random uint64) int {
	u := dataset.Unit(random, streamPage)
	switch {
	case u < 0.80:
		return 0
	case u < 0.92:
		return PageSize
	case u < 0.97:
		return 2 * PageSize
	default:
		return PageSize * (3 + int(dataset.Mix(random, streamPage+50)%4))
	}
}

func (a *Application) activeUser(random, stream uint64) int32 {
	return a.data.ActivityRank[a.active.Rank(dataset.Unit(random, stream))]
}

func (a *Application) popularUser(random, stream uint64) int32 {
	return a.data.PopularityRank[a.popular.Rank(dataset.Unit(random, stream))]
}

// BuildOperation draws the request of one sample: user-timeline reads of
// popular users (Zipf 1 over follower count), home-timeline reads and
// composes of active users (Zipf 0.75 over post count), follows of popular
// users by active users, unfollows of a random followee.
func (a *Application) BuildOperation(operation api.Operation, sample api.Sample, _ any) (api.OperationPlan, error) {
	random := sample.Random
	switch operation.Name {
	case opUserTimeline:
		return a.timelinePlan(opUserTimeline, a.popularUser(random, streamUser), Page(random)), nil
	case opHomeTimeline:
		return a.timelinePlan(opHomeTimeline, a.activeUser(random, streamUser), Page(random)), nil
	case opCompose:
		return a.composePlan(a.activeUser(random, streamUser), random), nil
	case opFollow, opUnfollow:
		return a.edgePlan(operation.Name == opFollow, random)
	}
	return api.OperationPlan{}, fmt.Errorf("unknown Social Network operation %q", operation.Name)
}

func timelineInvocation(kind string, data *dataset.Dataset, user int32, start int) api.Invocation {
	path := "/wrk2-api/user-timeline/read"
	if kind == opHomeTimeline {
		path = "/wrk2-api/home-timeline/read"
	}
	return api.Invocation{Target: gateway, Operation: kind, Payload: api.HTTPRequestSpec{
		Method: http.MethodGet, Path: path,
		Query: map[string]string{
			"user_id": strconv.FormatInt(data.Users[user].ID, 10),
			"start":   strconv.Itoa(start), "stop": strconv.Itoa(start + PageSize),
		},
	}}
}

func (a *Application) timelinePlan(kind string, user int32, start int) api.OperationPlan {
	return api.OperationPlan{
		Invocations: []api.Invocation{timelineInvocation(kind, a.data, user, start)},
		State:       &plan{kind: kind, user: user, start: start, built: time.Now()},
	}
}

func (a *Application) composePlan(author int32, random uint64) api.OperationPlan {
	seq := a.seq.Add(1)
	p := newLivePost(a.data, author, random, seq, a.namespace+strconv.FormatInt(seq, 36))
	now := time.Now()
	p.sentAt = now
	a.model.mu.Lock()
	a.model.beginPost(p)
	a.model.mu.Unlock()
	return api.OperationPlan{
		Invocations: []api.Invocation{
			{Target: gateway, Operation: opCompose, Payload: api.HTTPRequestSpec{
				Method: http.MethodPost, Path: "/wrk2-api/post/compose", Form: composeForm(a.data, p),
			}},
			timelineInvocation(opUserTimeline, a.data, author, 0),
		},
		State: &plan{kind: opCompose, user: author, built: now, post: p},
	}
}

// edgePlan draws a follow of a popular user not yet followed, or an
// unfollow of a current followee, on an edge with no change in flight.
func (a *Application) edgePlan(follow bool, random uint64) (api.OperationPlan, error) {
	m := a.model
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(a.data.Users)
	var u, v int32 = -1, -1
	for attempt := uint64(0); attempt < 64 && v < 0; attempt++ {
		stream := streamRetry + 3*attempt
		candidate := a.activeUser(random, stream)
		var target int32
		if follow {
			target = a.popularUser(random, stream+1)
		} else {
			row := a.data.Followees.Row(int(candidate))
			if len(row) == 0 {
				continue
			}
			target = row[dataset.Mix(random, stream+1)%uint64(len(row))]
		}
		if target != candidate && m.follows(candidate, target) != follow && !m.busy(candidate, target) {
			u, v = candidate, target
		}
	}
	// Unlikely fallback: scan for any eligible edge.
	for scan := 0; v < 0 && scan < n*4; scan++ {
		candidate := int32((int(random%uint64(n)) + scan) % n)
		row := a.data.Followees.Row(int(candidate))
		if len(row) == 0 {
			continue
		}
		target := row[scan%len(row)]
		if !follow {
			if m.follows(candidate, target) && !m.busy(candidate, target) {
				u, v = candidate, target
			}
		} else if other := int32((int(target) + 1) % n); other != candidate && !m.follows(candidate, other) && !m.busy(candidate, other) {
			u, v = candidate, other
		}
	}
	if v < 0 {
		return api.OperationPlan{}, fmt.Errorf("no edge to change")
	}
	now := time.Now()
	c := m.beginChange(u, v, follow, now)
	path := "/wrk2-api/user/unfollow"
	kind := opUnfollow
	if follow {
		path, kind = "/wrk2-api/user/follow", opFollow
	}
	return api.OperationPlan{
		Invocations: []api.Invocation{{Target: gateway, Operation: kind, Payload: api.HTTPRequestSpec{
			Method: http.MethodPost, Path: path,
			Form: map[string]string{
				"user_id":     strconv.FormatInt(a.data.Users[u].ID, 10),
				"followee_id": strconv.FormatInt(a.data.Users[v].ID, 10),
			},
		}}},
		State: &plan{kind: kind, user: u, built: now, edge: c},
	}, nil
}

// FinishOperation marks a write whose outcome validation never saw as
// failed (it may or may not have taken effect).
func (a *Application) FinishOperation(operationPlan api.OperationPlan) {
	state, ok := operationPlan.State.(*plan)
	if !ok {
		return
	}
	a.model.mu.Lock()
	defer a.model.mu.Unlock()
	now := time.Now()
	if state.post != nil {
		a.model.endPost(state.post, false, now)
	}
	if state.edge != nil {
		a.model.endChange(state.edge, false, now)
	}
}
