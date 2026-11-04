package socialnetwork

import (
	"testing"

	"vibesys/microservice-evaluator/config"
	"vibesys/social-network-k8s-evaluator/internal/dataset"
)

// The task's workload files load, validate and build the adapter for the
// default dataset.
func TestTaskWorkloads(t *testing.T) {
	for _, name := range []string{"closed", "open"} {
		workload, err := config.Load("../../../../workload/"+name+".toml", "")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		application, err := NewWithJournal(workload, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if application.data != dataset.Default() || application.model.Bound.Milliseconds() != 1000 {
			t.Fatalf("%s: dataset seed %d, bound %v", name, application.data.Seed, application.model.Bound)
		}
	}
}
