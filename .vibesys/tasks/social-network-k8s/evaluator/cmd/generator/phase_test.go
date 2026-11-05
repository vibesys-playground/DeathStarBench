package main

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/gen/vibesys/evaluator/v1"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/fakeapp"
	"vibesys/social-network-k8s-evaluator/internal/snworkload"
)

// mainEnv makes the test binary the generator (TestMain runs main), so the tests run the
// generator command as the phase pod does.
const mainEnv = "SN_GENERATOR_TEST_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(mainEnv) == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

var (
	loadgenOnce sync.Once
	loadgenPath string
	loadgenErr  error
)

// loadgen is $LOADGEN, or the SDK's loadgen built from the module the generator's SDK comes
// from (the same commit), in that module's own directory.
func loadgen(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs loadgen")
	}
	if path := os.Getenv("LOADGEN"); path != "" {
		return path
	}
	loadgenOnce.Do(func() {
		const module = "github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen"
		dir, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", module).Output()
		if err != nil {
			loadgenErr = fmt.Errorf("go list -m %s: %w", module, err)
			return
		}
		out, err := os.MkdirTemp("", "sn-loadgen-")
		if err != nil {
			loadgenErr = err
			return
		}
		loadgenPath = filepath.Join(out, "loadgen")
		build := exec.Command("go", "build", "-buildvcs=false", "-o", loadgenPath, "./cmd/loadgen")
		build.Dir = strings.TrimSpace(string(dir))
		build.Env = append(os.Environ(), "GOWORK=off")
		if output, err := build.CombinedOutput(); err != nil {
			loadgenErr = fmt.Errorf("go build loadgen in %s: %w\n%s", build.Dir, err, output)
		}
	})
	if loadgenErr != nil {
		t.Fatal(loadgenErr)
	}
	return loadgenPath
}

// datasetFile writes the dataset file bench.py's dataset() writes.
func datasetFile(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "dataset.json")
	encoded, err := json.Marshal(snworkload.DatasetFile{Seed: dataset.DefaultSeed, Digest: dataset.Default().Digest()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// runPhase runs one closed-loop phase as the phase pod does: loadgen with the generator under
// it, then the generator's check; it returns the PhaseOutput.
func runPhase(t *testing.T, target string, input *pb.PhaseInput) *pb.PhaseOutput {
	t.Helper()
	dir := t.TempDir()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	phase := filepath.Join(dir, "phase.binpb")
	if err := os.WriteFile(phase, encoded, 0o644); err != nil {
		t.Fatal(err)
	}
	data := datasetFile(t, dir)
	state := filepath.Join(dir, "state")
	if err := os.Mkdir(state, 0o755); err != nil {
		t.Fatal(err)
	}
	results, summary, output := filepath.Join(dir, "results.binpb"), filepath.Join(dir, "summary.binpb"), filepath.Join(dir, "output.binpb")
	shared := []string{"--phase-input", phase, "--dataset", data, "--state", state}
	run := exec.Command(loadgen(t), append([]string{"closed", "--target", target, "--clients", "16",
		"--secs", "3", "--warmup", "1", "--timeout", "5", "--results", results, "--summary", summary, "--", self, "generate"}, shared...)...)
	run.Env = append(os.Environ(), mainEnv+"=1")
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("loadgen: %v\n%s", err, out)
	}
	check := exec.Command(self, append(append([]string{"check"}, shared...), "--results", results, "--summary", summary, "--output", output)...)
	check.Env = run.Env
	if out, err := check.CombinedOutput(); err != nil {
		t.Fatalf("check: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	out := &pb.PhaseOutput{}
	if err := proto.Unmarshal(raw, out); err != nil {
		t.Fatal(err)
	}
	return out
}

func closedPhase(seed uint64, index uint32, history []*pb.HistoryEntry) *pb.PhaseInput {
	return &pb.PhaseInput{SampleSeed: seed, PhaseIndex: index, Model: pb.LoadModel_LOAD_MODEL_CLOSED,
		Clients: 16, Secs: 3, WarmupSecs: 1, History: history}
}

func violations(out *pb.PhaseOutput) string {
	var parts []string
	for _, v := range out.GetViolations() {
		parts = append(parts, v.GetOperation()+": "+v.GetMessage())
	}
	return strings.Join(parts, "\n")
}

// TestPhasesPassACorrectApplication: two phases of one sample against a correct application
// (asynchronous fan-out within the bound): every sampled response passes, and the second phase,
// which replays the first one's writes, validates the posts the first composed.
func TestPhasesPassACorrectApplication(t *testing.T) {
	app := fakeapp.New(dataset.Default(), fakeapp.Options{FanoutDelay: 900 * time.Millisecond})
	server := httptest.NewServer(app)
	defer server.Close()
	first := runPhase(t, server.URL, closedPhase(11, 0, nil))
	if first.GetInvalid() != 0 || first.GetChecked() == 0 {
		t.Fatalf("phase 0: %d of %d checks failed:\n%s", first.GetInvalid(), first.GetChecked(), violations(first))
	}
	writes := map[string]int{}
	for _, w := range first.GetWrites() {
		writes[w.GetOperation()]++
	}
	if writes["compose_post"] < 20 {
		t.Fatalf("phase 0 wrote %v", writes)
	}
	second := runPhase(t, server.URL, closedPhase(11, 1, first.GetWrites()))
	if second.GetInvalid() != 0 || second.GetChecked() == 0 {
		t.Fatalf("phase 1: %d of %d checks failed:\n%s", second.GetInvalid(), second.GetChecked(), violations(second))
	}
	t.Logf("phase 0: %d checked, writes %v; phase 1: %d checked", first.GetChecked(), writes, second.GetChecked())
	// Without the history the next phase meets posts it does not know.
	blind := runPhase(t, server.URL, closedPhase(11, 2, nil))
	if blind.GetInvalid() == 0 || !strings.Contains(violations(blind), "never composed") {
		t.Fatalf("a phase without the history passed: %s", violations(blind))
	}
}

// TestPhasesRejectFaultyApplications: the benchmark's checks fail a faulty application.
func TestPhasesRejectFaultyApplications(t *testing.T) {
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
			out := runPhase(t, server.URL, closedPhase(12, 0, nil))
			if out.GetInvalid() == 0 {
				t.Fatalf("a faulty application passed %d checks", out.GetChecked())
			}
			if !strings.Contains(violations(out), test.want) {
				t.Fatalf("violations %s, want %q", violations(out), test.want)
			}
			t.Logf("%d of %d checks failed, e.g. %s", out.GetInvalid(), out.GetChecked(), out.GetViolations()[0].GetMessage())
		})
	}
}
