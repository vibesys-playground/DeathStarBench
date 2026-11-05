package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	pb "github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/gen/vibesys/evaluator/v1"
	"google.golang.org/protobuf/proto"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// mainEnv makes the test binary the generator (TestMain runs main), so the test runs the
// generator command as the phase pod does: a process whose stream is closed when it has enough.
const mainEnv = "HOTEL_GENERATOR_TEST_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(mainEnv) == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

// update re-records testdata/stream.json from this generator (a deliberate change of the task's
// requests, such as a new dataset): go test ./cmd/generator -run TestTheStream -args -update.
// It also rewrites the history of every later phase input from the first phase's writes
// (historyOf), so the history is one the dataset admits.
var update = flag.Bool("update", false, "re-record testdata/stream.json and the later phases' histories")

// writes are the operations a phase's history holds.
var writes = map[string]bool{
	"add_hotel": true, "reserve_capacity": true, "set_capacity": true, "update_profile": true, "update_rate": true,
}

// historyOf is a history of the first phase's kept writes as a later phase receives it: the
// first historyLimit of them, 30 seconds of calls from a fixed instant, every seventh failed (a
// failed write's effect is unknown to the model, as a timeout's is).
func historyOf(names []string, kept [][]byte) ([]*pb.HistoryEntry, error) {
	const historyLimit = 422
	var out []*pb.HistoryEntry
	for _, record := range kept {
		var op pb.KeptOperation
		if err := proto.Unmarshal(record, &op); err != nil {
			return nil, err
		}
		if name := names[op.GetOperation()]; writes[name] && len(out) < historyLimit {
			out = append(out, &pb.HistoryEntry{Operation: name, InfoJson: op.GetInfoJson()})
		}
	}
	const start, span, latency = int64(1_800_000_000_000_000_000), int64(30_000_000_000), int64(3_000_000)
	for k, entry := range out {
		entry.StartUnixNanos = start + int64(k)*span/int64(max(len(out), 1))
		entry.EndUnixNanos = entry.StartUnixNanos + latency
		entry.Ok = k%7 != 3
	}
	return out, nil
}

// rewriteHistory replaces the history of the phase input at path.
func rewriteHistory(path string, history []*pb.HistoryEntry) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var phase pb.PhaseInput
	if err := proto.Unmarshal(raw, &phase); err != nil {
		return err
	}
	phase.History = history
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(&phase)
	if err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0o644)
}

// golden is testdata/stream.json: the stream the generator writes for two phases over the
// task's dataset (dataset.py's SPEC). It was first recorded from the task's Python workload
// (bench.py before the Go port), which the Go generator equalled message for message, and
// re-recorded from the Go generator when the dataset grew to the snapshot-seeded one.
type golden struct {
	About         string `json:"about"`
	DatasetSHA256 string `json:"dataset_sha256"`
	Phases        []struct {
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

// keptIndex is a KeptOperation's index: field 1, a varint written first, absent when 0.
func keptIndex(record []byte) uint64 {
	if len(record) == 0 || record[0] != 0x08 {
		return 0
	}
	index, _ := binary.Uvarint(record[1:])
	return index
}

// TestTheStream: for the same phases, the generator writes the stream (and keeps the records)
// recorded in testdata/stream.json. A change here is a change of the task's requests.
func TestTheStream(t *testing.T) {
	path := os.Getenv(dataset.PathEnv)
	if path == "" {
		t.Skipf("$%s names no dataset file (python dataset.py FILE writes it)", dataset.PathEnv)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var want golden
	text, err := os.ReadFile(filepath.Join("testdata", "stream.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(text, &want); err != nil {
		t.Fatal(err)
	}
	if *update {
		sum := sha256.Sum256(raw)
		want.DatasetSHA256 = hex.EncodeToString(sum[:])
	}
	if got := sha256.Sum256(raw); hex.EncodeToString(got[:]) != want.DatasetSHA256 {
		t.Fatalf("%s is not the dataset the stream was recorded with (dataset.py changed?)", path)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var history []*pb.HistoryEntry
	for n := range want.Phases {
		phase := &want.Phases[n]
		if *update && n > 0 {
			if err := rewriteHistory(filepath.Join("testdata", phase.PhaseInput), history); err != nil {
				t.Fatal(err)
			}
		}
		t.Run(phase.PhaseInput, func(t *testing.T) {
			state := t.TempDir()
			command := exec.Command(self, "generate", "--phase-input", filepath.Join("testdata", phase.PhaseInput),
				"--dataset", path, "--state", state)
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
			for len(frames) < phase.Operations+2 { // the FileHeader, the RequestsHeader, the operations
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
			if *update {
				phase.StreamSHA256 = sum(frames[1:])
			}
			if got := sum(frames[1:]); got != phase.StreamSHA256 {
				t.Errorf("the stream of %d operations has digest %s, want %s", phase.Operations, got, phase.StreamSHA256)
			}
			keptFile, err := os.Open(filepath.Join(state, "kept.binpb"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = keptFile.Close() }()
			keptIn := bufio.NewReader(keptFile)
			var kept [][]byte
			for {
				f, err := frame(keptIn)
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if keptIndex(f) >= uint64(phase.Operations) { // records are in stream order
					break
				}
				kept = append(kept, f)
			}
			if *update {
				phase.KeptSHA256 = sum(kept)
				if n == 0 {
					var header pb.RequestsHeader
					if err := proto.Unmarshal(frames[1], &header); err != nil {
						t.Fatal(err)
					}
					if history, err = historyOf(header.GetOperationNames(), kept); err != nil {
						t.Fatal(err)
					}
				}
			}
			if got := sum(kept); got != phase.KeptSHA256 {
				t.Errorf("the %d kept records have digest %s, want %s", len(kept), got, phase.KeptSHA256)
			}
		})
	}
	if *update {
		encoded, err := json.MarshalIndent(want, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join("testdata", "stream.json"), append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
