package socialnetwork

import (
	"context"
	"net/http/httptest"
	"strings"

	"testing"
	"time"

	"vibesys/microservice-evaluator/api"
	"vibesys/microservice-evaluator/drivers/httpdriver"
	"vibesys/microservice-evaluator/engine"
	"vibesys/microservice-evaluator/registry"
	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/fakeapp"
)

func testWorkload(address string, seconds float64, concurrency int) api.Workload {
	one := 1.0
	operations := []api.Operation{
		{Name: opUserTimeline, Target: gateway, Weight: 500},
		{Name: opHomeTimeline, Target: gateway, Weight: 400},
		{Name: opCompose, Target: gateway, Weight: 100},
		{Name: opFollow, Target: gateway, Weight: 5},
		{Name: opUnfollow, Target: gateway, Weight: 5},
	}
	return api.Workload{
		Version: 1, Name: "social-network-test", Application: "social-network",
		Load: api.Load{Model: "closed_loop", DurationSeconds: seconds, Concurrency: concurrency, TimeoutSeconds: 5,
			Repetitions: 1, Seed: 42, FixtureSeed: 42, MinOfferedRateRatio: 0},
		Targets:     []api.Target{{Name: gateway, Protocol: "http", Address: address, SessionPolicy: "reuse"}},
		Operations:  operations,
		Objective:   api.Objective{Name: "ops", Metric: "operations_per_second", Direction: "maximize", Unit: "operations/s"},
		Constraints: api.Constraints{MinSuccessRate: &one},
	}
}

func runEngine(t *testing.T, workload api.Workload, factory func(api.Workload) (api.Application, error)) engine.RunResult {
	t.Helper()
	result, err := tryEngine(workload, factory)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func tryEngine(workload api.Workload, factory func(api.Workload) (api.Application, error)) (engine.RunResult, error) {
	components := registry.New()
	if err := components.RegisterDriver(httpdriver.New()); err != nil {
		return engine.RunResult{}, err
	}
	if err := components.RegisterApplication("social-network", factory); err != nil {
		return engine.RunResult{}, err
	}
	return engine.New(components, engine.Options{EngineVersion: "test", WorkloadHash: "test"}).Run(context.Background(), workload)
}

func failures(result engine.RunResult) (map[string]int, map[string]int, string) {
	counts, failed := map[string]int{}, map[string]int{}
	example := ""
	for _, observation := range result.Observations {
		counts[observation.Operation]++
		if !observation.ApplicationSuccess {
			failed[observation.ErrorCategory]++
			if example == "" {
				example = observation.ErrorMessage
			}
		}
	}
	return counts, failed, example
}

func TestBenchmarkPassesACorrectApplication(t *testing.T) {
	// Asynchronous fan-out within the bound (1 s) is allowed.
	app := fakeapp.New(dataset.Default(), fakeapp.Options{FanoutDelay: 900 * time.Millisecond})
	server := httptest.NewServer(app)
	defer server.Close()
	workload := testWorkload(server.URL, 3, 16)
	workload.Load.Repetitions = 2
	result := runEngine(t, workload, New)
	counts, failed, example := failures(result)
	t.Logf("operations %v", counts)
	if len(failed) != 0 || !result.Summary.Valid {
		t.Fatalf("failures %v, e.g. %s", failed, example)
	}
	for _, name := range []string{opUserTimeline, opHomeTimeline, opCompose} {
		if counts[name] < 20 {
			t.Fatalf("only %d %s operations", counts[name], name)
		}
	}
}

func TestBenchmarkRejectsFaultyApplications(t *testing.T) {
	tests := []struct {
		name    string
		options fakeapp.Options
		want    string
	}{
		{"never-invalidated cache", fakeapp.Options{StaleCache: true}, ""},
		{"stale replica", fakeapp.Options{StaleReplica: true}, ""},
		{"fan-out beyond the bound", fakeapp.Options{FanoutDelay: 1500 * time.Millisecond}, "home_timeline_read"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(fakeapp.New(dataset.Default(), test.options))
			defer server.Close()
			result := runEngine(t, testWorkload(server.URL, 4, 16), New)
			_, failed, example := failures(result)
			if len(failed) == 0 || result.Summary.Valid {
				t.Fatal("faulty application passed")
			}
			if !strings.Contains(example, test.want) {
				t.Fatalf("first failure %q, want %q", example, test.want)
			}
			t.Logf("failures %v, e.g. %s", failed, example)
		})
	}
}

func TestJournalCarriesWritesToTheNextPhase(t *testing.T) {
	server := httptest.NewServer(fakeapp.New(dataset.Default(), fakeapp.Options{}))
	defer server.Close()
	var first *Application
	runEngine(t, testWorkload(server.URL, 2, 8), func(workload api.Workload) (api.Application, error) {
		created, err := NewWithJournal(workload, nil)
		first = created
		return created, err
	})
	journal, err := first.WriteJournal()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("journal %d bytes for %d events", len(journal), len(first.model.journal))
	second := testWorkload(server.URL, 2, 8)
	second.Name = "social-network-test-2"
	result := runEngine(t, second, func(workload api.Workload) (api.Application, error) {
		return NewWithJournal(workload, journal)
	})
	if _, failed, example := failures(result); len(failed) != 0 {
		t.Fatalf("phase two with the journal failed: %v, e.g. %s", failed, example)
	}
	// Replaying gives the same model.
	replayed, err := NewWithJournal(second, journal)
	if err != nil {
		t.Fatal(err)
	}
	again, err := replayed.WriteJournal()
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(journal) {
		t.Fatal("replayed journal differs")
	}
	// Without the journal the next phase sees posts it does not know: its
	// preparation (a read of the most active users' timelines) fails.
	if _, err := tryEngine(second, New); err == nil || !strings.Contains(err.Error(), "never composed") {
		t.Fatalf("phase two without the journal: %v", err)
	}
}
