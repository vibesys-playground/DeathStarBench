package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/gen/vibesys/evaluator/v1"
)

// recordEnv rewrites testdata/ from the current generator instead of checking it.
const recordEnv = "SN_STREAM_RECORD"

// golden is testdata/stream.json.
type golden struct {
	About  string `json:"about"`
	Phases []struct {
		PhaseInput   string `json:"phase_input"`
		Operations   int    `json:"operations"`
		StreamSHA256 string `json:"stream_sha256"`
		KeptSHA256   string `json:"kept_sha256"`
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
// after the FileHeader (the RequestsHeader, then the operations) and the kept records of those
// operations.
func generateStream(t *testing.T, phaseInput string, n int) (stream, kept [][]byte) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	command := exec.Command(self, "generate", "--phase-input", phaseInput, "--dataset", datasetFile(t, t.TempDir()), "--state", state)
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
	file, err := os.Open(filepath.Join(state, "kept.binpb"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	keptIn := bufio.NewReader(file)
	for {
		f, err := frame(keptIn)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		record := &pb.KeptOperation{}
		if err := proto.Unmarshal(f, record); err != nil {
			t.Fatal(err)
		}
		if record.GetIndex() >= uint64(n) { // records are in stream order
			break
		}
		kept = append(kept, f)
	}
	return frames[1:], kept
}

// TestTheStreamIsPinned: for the same phases, the generator writes the stream (and keeps the
// records) recorded in testdata/. The requests of each operation were the earlier
// servicebench adapter's, word for word (internal/snworkload's TestRequestsAreTheEarlierAdapters
// at the commit that recorded them); a change here is a change of the task's requests.
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
			stream, kept := generateStream(t, filepath.Join("testdata", phase.PhaseInput), phase.Operations)
			if got := sum(stream); got != phase.StreamSHA256 {
				t.Errorf("the stream of %d operations has digest %s, want %s", phase.Operations, got, phase.StreamSHA256)
			}
			if got := sum(kept); got != phase.KeptSHA256 {
				t.Errorf("the %d kept records have digest %s, want %s", len(kept), got, phase.KeptSHA256)
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

// record writes testdata/: a closed phase, then an open one whose history is the writes of
// the closed phase's first 5,000 operations, acknowledged.
func record(t *testing.T) {
	t.Helper()
	closed := &pb.PhaseInput{SampleSeed: 48, PhaseIndex: 0, Model: pb.LoadModel_LOAD_MODEL_CLOSED, Clients: 32, Secs: 20, WarmupSecs: 10}
	write := func(name string, input *pb.PhaseInput) string {
		encoded, err := proto.Marshal(input)
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
	out := golden{About: "the generator's stream for these phases: SHA-256 over its frames (4-byte little-endian length, then the frame) after the FileHeader: the RequestsHeader and the first operations; and over its kept records of those operations. Recorded by TestTheStreamIsPinned with " + recordEnv + "=1."}
	add := func(name string, input *pb.PhaseInput, n int) [][]byte {
		stream, kept := generateStream(t, write(name, input), n)
		out.Phases = append(out.Phases, struct {
			PhaseInput   string `json:"phase_input"`
			Operations   int    `json:"operations"`
			StreamSHA256 string `json:"stream_sha256"`
			KeptSHA256   string `json:"kept_sha256"`
		}{name, n, sum(stream), sum(kept)})
		return kept
	}
	kept := add("phase0.binpb", closed, 30000)
	names := []string{"user_timeline_read", "home_timeline_read", "compose_post", "follow", "unfollow"}
	var history []*pb.HistoryEntry
	for i, f := range kept {
		record := &pb.KeptOperation{}
		if err := proto.Unmarshal(f, record); err != nil {
			t.Fatal(err)
		}
		name := names[record.GetOperation()]
		if record.GetIndex() >= 5000 {
			break
		}
		if name != "compose_post" && name != "follow" && name != "unfollow" {
			continue
		}
		start := int64(1_800_000_000_000_000_000) + int64(i)*1_000_000
		answer := func(body string) *pb.StepResult {
			return &pb.StepResult{Status: 200, Response: &pb.StepResult_Body{Body: []byte(body)}}
		}
		steps := []*pb.StepResult{answer("Success")}
		if name == "compose_post" {
			steps = []*pb.StepResult{answer("Successfully upload post"), answer("{}")}
		}
		history = append(history, &pb.HistoryEntry{Operation: name, InfoJson: record.GetInfoJson(),
			StartUnixNanos: start, EndUnixNanos: start + 500_000, Ok: true, Steps: steps})
	}
	add("phase1.binpb", &pb.PhaseInput{SampleSeed: 48, PhaseIndex: 1, Model: pb.LoadModel_LOAD_MODEL_OPEN, Rate: 1500,
		Arrivals: pb.Arrivals_ARRIVALS_UNIFORM, Secs: 60, WarmupSecs: 10, History: history}, 60000)
	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("testdata", "stream.json"), append(encoded, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("recorded testdata/ (%d history entries in phase 1)", len(history))
}
