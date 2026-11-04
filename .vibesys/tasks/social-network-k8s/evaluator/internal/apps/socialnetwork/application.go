// Package socialnetwork is the task's benchmark adapter for DeathStarBench's
// Social Network: requests drawn over the task's dataset with skewed
// popularity, and exact validation of every answer against a model of the
// seeded data and the run's own writes (see Model for the freshness rules).
package socialnetwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"vibesys/microservice-evaluator/api"
	"vibesys/social-network-k8s-evaluator/internal/dataset"
)

const (
	opUserTimeline = "user_timeline_read"
	opHomeTimeline = "home_timeline_read"
	opCompose      = "compose_post"
	opFollow       = "follow"
	opUnfollow     = "unfollow"

	gateway = "gateway"
	// PageSize is the number of posts per timeline page.
	PageSize = 10
)

var knownOperations = map[string]bool{opUserTimeline: true, opHomeTimeline: true, opCompose: true, opFollow: true, opUnfollow: true}

// Application is the adapter. One instance serves every trial of a run; its
// model carries over between trials.
type Application struct {
	data     *dataset.Dataset
	model    *Model
	workload string

	popular *dataset.Zipf // user-timeline targets, by PopularityRank
	active  *dataset.Zipf // readers, authors, followers, by ActivityRank

	seq       atomic.Int64
	namespace string
	verified  bool
}

var _ api.Application = (*Application)(nil)

// New builds the adapter for a workload.
func New(workload api.Workload) (api.Application, error) {
	return NewWithJournal(workload, nil)
}

// NewWithJournal builds the adapter and replays an earlier phase's journal
// (WriteJournal) into its model.
func NewWithJournal(workload api.Workload, journal []byte) (*Application, error) {
	gatewayFound := false
	for _, target := range workload.Targets {
		if target.Name == gateway {
			gatewayFound = true
			if target.Protocol != "http" {
				return nil, fmt.Errorf("Social Network gateway target must use HTTP, got %q", target.Protocol)
			}
		}
	}
	if !gatewayFound {
		return nil, fmt.Errorf("Social Network requires a target named %s", gateway)
	}
	for _, operation := range workload.Operations {
		if !knownOperations[operation.Name] {
			return nil, fmt.Errorf("unknown Social Network operation %q", operation.Name)
		}
		if operation.Target != gateway {
			return nil, fmt.Errorf("Social Network operation %q must target %s", operation.Name, gateway)
		}
		if operation.HTTP != nil || len(operation.CaptureHeaders) != 0 {
			return nil, fmt.Errorf("Social Network operation %q must not declare http or capture_headers", operation.Name)
		}
	}
	seed, bound := dataset.DefaultSeed, time.Second
	for key, value := range workload.ApplicationConfig {
		switch key {
		case "dataset_seed":
			number, ok := integer(value)
			if !ok || number < 0 {
				return nil, fmt.Errorf("application_config.dataset_seed must be a non-negative integer")
			}
			seed = uint64(number)
		case "freshness_ms":
			number, ok := integer(value)
			if !ok || number < 0 {
				return nil, fmt.Errorf("application_config.freshness_ms must be a non-negative integer")
			}
			bound = time.Duration(number) * time.Millisecond
		default:
			return nil, fmt.Errorf("unknown application_config field %q", key)
		}
	}
	data := dataset.Default()
	if seed != dataset.DefaultSeed {
		data = dataset.Generate(seed, dataset.DefaultConfig())
	}
	application := &Application{
		data: data, model: newModel(data, bound), workload: workload.Name,
		popular: dataset.NewZipf(len(data.Users), dataset.ZipfOne),
		active:  dataset.NewZipf(len(data.Users), dataset.ZipfThreeQuarters),
	}
	if journal != nil {
		if err := application.replay(journal); err != nil {
			return nil, fmt.Errorf("replay journal: %w", err)
		}
	}
	return application, nil
}

func (a *Application) Name() string { return "social-network" }

func (a *Application) SupportsSkipPrepare() bool { return false }

func (a *Application) Reset(context.Context, api.Runtime, api.TrialContext) error { return nil }

// Prepare names the trial's live posts and, once per run, checks that the
// deployment serves this dataset.
func (a *Application) Prepare(ctx context.Context, runtime api.Runtime, trial api.TrialContext) (any, error) {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%d", a.workload, trial.FixtureSeed, trial.Index)))
	a.namespace = "m" + hex.EncodeToString(digest[:5])
	if !a.verified {
		if err := a.verify(ctx, runtime); err != nil {
			return nil, err
		}
		a.verified = true
	}
	return a, nil
}

// verify reads the first page of the most active users' timelines.
func (a *Application) verify(ctx context.Context, runtime api.Runtime) error {
	for _, user := range a.data.ActivityRank[:4] {
		for _, kind := range []string{opUserTimeline, opHomeTimeline} {
			plan := a.timelinePlan(kind, user, 0)
			result := runtime.Invoke(ctx, plan.Invocations[0])
			if validation := a.ValidateOperation(api.Operation{Name: kind}, plan, []api.ProtocolResult{result}); !validation.Success {
				return fmt.Errorf("the deployment does not serve dataset %s (seed %d): %s of user %d: %s",
					a.data.Digest()[:16], a.data.Seed, kind, a.data.Users[user].ID, validation.ErrorMessage)
			}
		}
	}
	return nil
}

func integer(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case float64:
		if math.Trunc(typed) == typed && math.Abs(typed) < 1<<53 {
			return int64(typed), true
		}
	case string:
		number, err := strconv.ParseInt(typed, 0, 64)
		return number, err == nil
	}
	return 0, false
}
