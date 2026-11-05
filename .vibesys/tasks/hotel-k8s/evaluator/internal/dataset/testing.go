package dataset

import (
	"fmt"
	"os"
	"sync"
)

// TestSpec is a small dataset of the production shape, for tests: the dataset of
// $HOTEL_DATASET when it names one, else 20,000 hotels and 2,000 users.
func TestSpec() Spec {
	if file := os.Getenv(PathEnv); file != "" {
		spec, err := ReadSpec(file)
		if err != nil {
			panic(err)
		}
		return spec
	}
	return Spec{Format: Format, Seed: 0x686F74656C2D7633, Hotels: 20_000, Users: 2_000}
}

var (
	testOnce sync.Once
	testData *Dataset
)

// UseTestDataset makes Default return the dataset of TestSpec (generated once per process).
func UseTestDataset() {
	testOnce.Do(func() {
		data, err := Generate(TestSpec())
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		testData = data
	})
	UseDataset(testData)
}
