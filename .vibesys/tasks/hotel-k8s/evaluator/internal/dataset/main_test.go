package dataset

import (
	"fmt"
	"os"
	"testing"

)

// TestMain needs the task's dataset file, which dataset.py writes:
//
//	python -c 'import dataset, vibesys_evaluator...' (see tests/test_oracle.py)
//	HOTEL_DATASET=/path/dataset.json go test ./...
//
// Without it the package's tests are skipped.
func TestMain(m *testing.M) {
	if _, err := Load(); err != nil {
		fmt.Printf("skipping %s: %v\n", "internal/dataset", err)
		os.Exit(0)
	}
	os.Exit(m.Run())
}
