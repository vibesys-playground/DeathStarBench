package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/gen/vibesys/evaluator/v1"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/snworkload"
)

// mainEnv makes the test binary the generator (TestMain runs main), so the test runs the
// generator command as the phase pod does: a process whose stream is closed when it has enough.
const mainEnv = "SN_GENERATOR_TEST_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(mainEnv) == "1" {
		main()
		return
	}
	os.Exit(m.Run())
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

// recordEnv rewrites testdata/ from the current generator instead of checking it.
const recordEnv = "SN_STREAM_RECORD"

// golden is testdata/stream.json.
type golden struct {
	About  string `json:"about"`
	Phases []struct {
		PhaseInput   string `json:"phase_input"`
		Operations   int    `json:"operations"`
		StreamSHA256 string `json:"stream_sha256"`
	} `json:"phases"`
}

// frame reads one length-delimited message.
func frame(in *bufio.Reader) ([]byte, error) {
	length, err := binary.ReadUvarint(in)
	if err != nil {
		return nil, err
	}
	out := make([]byte, length)
	_, err = io.ReadFull(in, out)
	return out, err
}

// sum is the SHA-256 over the frames, each as its 4-byte little-endian length and its bytes.
func sum(frames [][]byte) string {
	h := sha256.New()
	for _, f := range frames {
		var size [4]byte
		binary.LittleEndian.PutUint32(size[:], uint32(len(f)))
		h.Write(size[:])
		h.Write(f)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// generateStream runs `generate` as loadgen does and reads n operations; it returns the frames
// after the FileHeader (the RequestsHeader, then the operations).
func generateStream(t *testing.T, phaseInput string, n int) [][]byte {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(self, "generate", "--phase-input", phaseInput, "--dataset", datasetFile(t, t.TempDir()), "--state", t.TempDir())
	command.Env = append(os.Environ(), mainEnv+"=1")
	command.Stderr = os.Stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	in := bufio.NewReaderSize(stdout, 1<<16)
	var frames [][]byte
	for len(frames) < n+2 { // the FileHeader, the RequestsHeader, the operations
		f, err := frame(in)
		if err != nil {
			t.Fatalf("frame %d: %v", len(frames), err)
		}
		frames = append(frames, f)
	}
	_ = stdout.Close() // the end of the phase, as loadgen ends it
	if err := command.Wait(); err != nil {
		t.Fatalf("generate: %v", err)
	}
	return frames[1:]
}

// TestTheStreamIsPinned: for the same phases, the generator writes the stream recorded in
// testdata/. The requests of each operation are the earlier servicebench adapter's, word for
// word (internal/snworkload's TestRequestsAreTheEarlierAdapters at the commit that first
// recorded them); a change here is a change of the task's requests.
func TestTheStreamIsPinned(t *testing.T) {
	if os.Getenv(recordEnv) == "1" {
		record(t)
	}
	text, err := os.ReadFile(filepath.Join("testdata", "stream.json"))
	if err != nil {
		t.Fatal(err)
	}
	var want golden
	if err := json.Unmarshal(text, &want); err != nil {
		t.Fatal(err)
	}
	for _, phase := range want.Phases {
		t.Run(phase.PhaseInput, func(t *testing.T) {
			stream := generateStream(t, filepath.Join("testdata", phase.PhaseInput), phase.Operations)
			if got := sum(stream); got != phase.StreamSHA256 {
				t.Errorf("the stream of %d operations has digest %s, want %s", phase.Operations, got, phase.StreamSHA256)
			}
			// The mix: 500/400/100/5/5.
			header := &pb.RequestsHeader{}
			if err := proto.Unmarshal(stream[0], header); err != nil {
				t.Fatal(err)
			}
			counts := make([]int, len(header.GetOperationNames()))
			for _, f := range stream[1:] {
				op := &pb.PlannedOperation{}
				if err := proto.Unmarshal(f, op); err != nil {
					t.Fatal(err)
				}
				counts[op.GetOperation()]++
			}
			for i, weight := range []float64{500, 400, 100, 5, 5} {
				share, want := float64(counts[i])/float64(phase.Operations), weight/1010
				if share < want*0.9-0.001 || share > want*1.1+0.001 {
					t.Errorf("%s: %.4f of the stream, want %.4f", header.GetOperationNames()[i], share, want)
				}
			}
		})
	}
}

// record writes testdata/: a closed phase 0 and an open phase 1 of the same sample.
func record(t *testing.T) {
	t.Helper()
	write := func(name string, input *pb.PhaseInput) string {
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join("testdata", name)
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	out := golden{About: "the generator's stream for these phases: SHA-256 over its frames (4-byte little-endian length, then the frame) after the FileHeader: the RequestsHeader and the first operations. Re-recorded by TestTheStreamIsPinned with " + recordEnv + "=1 when benchmarks stopped checking (no kept records or history; per-phase compose markers; follow edges from the phase's own changes)."}
	add := func(name string, input *pb.PhaseInput, n int) {
		out.Phases = append(out.Phases, struct {
			PhaseInput   string `json:"phase_input"`
			Operations   int    `json:"operations"`
			StreamSHA256 string `json:"stream_sha256"`
		}{name, n, sum(generateStream(t, write(name, input), n))})
	}
	add("phase0.binpb", &pb.PhaseInput{SampleSeed: 48, PhaseIndex: 0, Model: pb.LoadModel_LOAD_MODEL_CLOSED, Clients: 32, Secs: 20, WarmupSecs: 10}, 30000)
	add("phase1.binpb", &pb.PhaseInput{SampleSeed: 48, PhaseIndex: 1, Model: pb.LoadModel_LOAD_MODEL_OPEN, Rate: 1500,
		Arrivals: pb.Arrivals_ARRIVALS_UNIFORM, Secs: 60, WarmupSecs: 10}, 60000)
	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("testdata", "stream.json"), append(encoded, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
