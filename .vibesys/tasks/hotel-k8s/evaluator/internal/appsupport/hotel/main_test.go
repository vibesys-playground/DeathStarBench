package hotel

import (
	"fmt"
	"os"
	"testing"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// TestMain needs the task's dataset file, which dataset.py writes:
//
//	python -c 'import dataset, vibesys_evaluator...' (see tests/test_oracle.py)
//	HOTEL_DATASET=/path/dataset.json go test ./...
//
// Without it the package's tests are skipped.
func TestMain(m *testing.M) {
	if _, err := dataset.Load(); err != nil {
		fmt.Printf("skipping %s: %v\n", "internal/appsupport/hotel", err)
		os.Exit(0)
	}
	os.Exit(m.Run())
}
