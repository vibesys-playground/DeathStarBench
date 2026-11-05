package hotel

import (
	"os"
	"testing"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// TestMain generates the tests' dataset: dataset.TestSpec (the file $HOTEL_DATASET names, else
// a small dataset of the production shape).
func TestMain(m *testing.M) {
	dataset.UseTestDataset()
	os.Exit(m.Run())
}
