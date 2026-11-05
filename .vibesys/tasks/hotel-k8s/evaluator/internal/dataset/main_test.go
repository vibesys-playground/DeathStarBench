package dataset

import (
	"os"
	"testing"
)

// TestMain generates the tests' dataset: TestSpec (the file $HOTEL_DATASET names, else a small
// dataset of the production shape).
func TestMain(m *testing.M) {
	UseTestDataset()
	os.Exit(m.Run())
}
