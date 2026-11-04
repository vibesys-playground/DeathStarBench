package hotel

import (
	"testing"

	"vibesys/microservice-evaluator/config"
)

// The task's workloads load, the adapter accepts them, and admin writes are
// about 1% of the mix with every kind present.
func TestTaskWorkloads(t *testing.T) {
	for _, name := range []string{"closed", "open"} {
		workload, err := config.Load("../../../../workload/"+name+".toml", "")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, err := New(workload); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		total, writes := 0, 0
		kinds := map[string]bool{}
		for _, operation := range workload.Operations {
			total += operation.Weight
			switch operation.Name {
			case operationUpdateRate, operationUpdateProfile, operationSetCapacity, operationAddHotel:
				writes += operation.Weight
				kinds[operation.Name] = true
			}
		}
		if share := float64(writes) / float64(total); share < 0.008 || share > 0.012 || len(kinds) != 4 {
			t.Fatalf("%s: writes are %.2f%% of the mix, kinds %v", name, 100*share, kinds)
		}
	}
}
