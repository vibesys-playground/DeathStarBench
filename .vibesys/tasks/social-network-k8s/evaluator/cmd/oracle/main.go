// Command oracle is the social-network-k8s task's accuracy oracle on vibesys-evaluator's
// checker (go/check): it checks a captured history of the accuracy workload against the
// contract in internal/snspec (OBJECTIVE.md and internal/snapi) with two specs, per author and
// per reader, whose verdicts are ANDed.
//
//	oracle --history DIR [--out /vibesys/artifacts/accuracy] [budget flags]
//
// DIR is the history directory the accuracy capture wrote (vibesys-evaluator's spec.md,
// "Accuracy history"), with the dataset file the workload read ({"seed": N, "digest": "..."},
// bench.py's dataset). Each call's Info is the JSON of its snapi.Action and its one step is the
// request that action makes. The exit status is 0 for PASS, 1 for FAIL and 2 for UNKNOWN, with
// the reason on stderr.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/snspec"
)

// datasetFile is the dataset file: the seed and digest of the task's generated dataset.
type datasetFile struct {
	Seed   uint64 `json:"seed"`
	Digest string `json:"digest"`
}

// loadDataset generates the dataset a dataset file names and checks its digest.
func loadDataset(path string) (*dataset.Dataset, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var file datasetFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	data := dataset.Default()
	if file.Seed != dataset.DefaultSeed {
		data = dataset.Generate(file.Seed, dataset.DefaultConfig())
	}
	if got := data.Digest(); got != file.Digest {
		return nil, fmt.Errorf("the dataset of seed %d has digest %s, the dataset file says %s", file.Seed, got, file.Digest)
	}
	return data, nil
}

// specs is Main's oracle: the author and reader specs over the history's dataset.
func specs(h *check.History) ([]check.AnySpec, error) {
	if h.Dataset == "" {
		return nil, errors.New("the history has no dataset file")
	}
	data, err := loadDataset(h.Dataset)
	if err != nil {
		return nil, err
	}
	return []check.AnySpec{snspec.DefaultAuthorSpec(data), snspec.DefaultReaderSpec(data)}, nil
}

func main() { check.Main(specs) }
