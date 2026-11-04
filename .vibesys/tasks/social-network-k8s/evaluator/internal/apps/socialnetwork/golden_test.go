package socialnetwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"math/rand"
	"net/http"
	"testing"

	"vibesys/microservice-evaluator/api"
)

// goldenDigest is the digest goldenStream computes for the Social Network
// benchmark adapter at vibesys commit de037943c (resources/evaluators/
// microservice/apps/socialnetwork), from which this package was copied. The
// task-local adapter must reproduce it: the same setup requests, operation
// plans and validation decisions for the same seeds.
const goldenDigest = "92593a6cb1825568242ba8db8c8251bae985111bafa3ca6f98b84a5846025eca"

const goldenSamplesPerTrial = 3000

var goldenOperations = []string{userTimelineRead, homeTimelineRead, composeUserTimeline, legacyComposePost}

var goldenTrials = []api.TrialContext{
	{Index: 0, Seed: 48, FixtureSeed: 48},
	{Index: 0, Seed: 48, FixtureSeed: 49},
}

func TestGoldenOperationStream(t *testing.T) {
	successes := make(map[string]int)
	if digest := goldenStream(t, successes); digest != goldenDigest {
		t.Fatalf("operation stream digest = %s, want %s", digest, goldenDigest)
	}
	// The fake answers correctly, so every real answer passes, and each kind
	// of corruption is rejected at least sometimes.
	if got, want := successes["real"], goldenSamplesPerTrial*len(goldenTrials); got != want {
		t.Fatalf("real answers accepted %d times, want %d", got, want)
	}
	for _, name := range []string{"status", "drop", "swap", "text", "identity", "short"} {
		if successes[name] == goldenSamplesPerTrial*len(goldenTrials) {
			t.Errorf("corruption %s was never rejected", name)
		}
	}
}

func goldenWorkload() api.Workload {
	workload := socialNetworkWorkload(userTimelineRead)
	workload.Operations = nil
	for _, name := range goldenOperations {
		workload.Operations = append(workload.Operations, api.Operation{Name: name, Target: "gateway", Weight: 1})
	}
	workload.ApplicationConfig = map[string]any{
		"users": int64(40), "seed_posts_per_user": int64(3), "timeline_limit": int64(5), "setup_delay_seconds": float64(0),
	}
	return workload
}

// goldenStream prepares the adapter against the fake Social Network for each
// trial, then runs seeded samples of randomly interleaved operations through
// it, validating the fake's answers and deterministic corruptions of them.
// The digest covers every setup request, plan and validation decision.
func goldenStream(t *testing.T, successes map[string]int) string {
	t.Helper()
	sum := sha256.New()
	for _, trial := range goldenTrials {
		applicationValue, err := New(goldenWorkload())
		if err != nil {
			t.Fatal(err)
		}
		application := applicationValue.(*Application)
		runtime := &handlerRuntime{handler: newFakeSocialNetwork(t)}
		prepared, err := application.Prepare(context.Background(), runtime, trial)
		if err != nil {
			t.Fatal(err)
		}
		for _, request := range runtime.requests {
			fmt.Fprintf(sum, "setup %s\n", mustJSON(request))
		}
		random := rand.New(rand.NewSource(trial.FixtureSeed))
		for counter := 0; counter < goldenSamplesPerTrial; counter++ {
			operation := api.Operation{Name: goldenOperations[random.Intn(len(goldenOperations))], Target: "gateway"}
			sample := api.Sample{Counter: int64(counter), Random: random.Uint64()}
			goldenSample(t, sum, successes, application, prepared, runtime, operation, sample)
		}
	}
	return hex.EncodeToString(sum.Sum(nil))
}

func goldenSample(t *testing.T, sum hash.Hash, successes map[string]int, application *Application, prepared any, runtime *handlerRuntime,
	operation api.Operation, sample api.Sample) {
	t.Helper()
	plan, err := application.BuildOperation(operation, sample, prepared)
	if err != nil {
		t.Fatalf("%s sample %+v: %v", operation.Name, sample, err)
	}
	defer application.FinishOperation(plan)
	fmt.Fprintf(sum, "plan %s %d %d %s\n", operation.Name, sample.Counter, sample.Random, encodePlan(t, plan))
	results := make([]api.ProtocolResult, len(plan.Invocations))
	for index, invocation := range plan.Invocations {
		results[index] = runtime.Invoke(context.Background(), invocation)
	}
	random := rand.New(rand.NewSource(int64(sample.Random >> 1)))
	// The real answer first, so that what it teaches the model takes effect;
	// the corruptions are then validated against the same plan.
	for _, variant := range append([]resultVariant{{"real", results}}, corruptions(results, random)...) {
		decision := application.ValidateOperation(operation, plan, variant.results)
		fmt.Fprintf(sum, "validate %s %v %s %s\n", variant.name, decision.Success, decision.ErrorCategory, decision.ErrorMessage)
		if decision.Success {
			successes[variant.name]++
		}
	}
}

func encodePlan(t *testing.T, plan api.OperationPlan) []byte {
	t.Helper()
	state := plan.State.(*operationState)
	texts := make([]string, len(state.expected))
	for index, post := range state.expected {
		texts[index] = post.text
	}
	encoded, err := json.Marshal(struct {
		Invocations []api.Invocation
		Kind        string
		User        int
		Marker      string
		Size        int
		Expected    []string
		Release     bool
	}{plan.Invocations, state.kind, state.user.id, state.marker, state.timelineSize, texts, state.release != nil})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

type resultVariant struct {
	name    string
	results []api.ProtocolResult
}

func corruptions(results []api.ProtocolResult, random *rand.Rand) []resultVariant {
	mutateTimeline := func(name string, change func([]map[string]any) []map[string]any) resultVariant {
		index := random.Intn(len(results))
		changed := append([]api.ProtocolResult(nil), results...)
		response, _ := changed[index].Payload.(api.HTTPResponse)
		var posts []map[string]any
		if json.Unmarshal(response.Body, &posts) == nil {
			response.Body = mustJSON(change(posts))
		} else {
			response.Body = append([]byte("x"), response.Body...)
		}
		changed[index].Payload = response
		return resultVariant{name: name, results: changed}
	}
	status := append([]api.ProtocolResult(nil), results...)
	statusIndex := random.Intn(len(results))
	status[statusIndex] = protocolResult(http.StatusInternalServerError, []byte("ZADD failed"))
	return []resultVariant{
		{name: "status", results: status},
		mutateTimeline("drop", func(posts []map[string]any) []map[string]any {
			if len(posts) == 0 {
				return posts
			}
			at := random.Intn(len(posts))
			return append(posts[:at:at], posts[at+1:]...)
		}),
		mutateTimeline("swap", func(posts []map[string]any) []map[string]any {
			if len(posts) > 1 {
				posts[0], posts[1] = posts[1], posts[0]
			}
			return posts
		}),
		mutateTimeline("text", func(posts []map[string]any) []map[string]any {
			if len(posts) > 0 {
				posts[random.Intn(len(posts))]["text"] = "corrupted"
			}
			return posts
		}),
		mutateTimeline("identity", func(posts []map[string]any) []map[string]any {
			if len(posts) > 0 {
				posts[random.Intn(len(posts))]["post_id"] = "post-0"
			}
			return posts
		}),
		{name: "short", results: results[:len(results)-1]},
	}
}
