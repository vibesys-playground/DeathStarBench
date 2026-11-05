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

// golden is testdata/stream.json: the stream the task's Python workload (bench.py before the Go
// port) wrote for two phases, recorded by tests/test_generator_stream.py of that commit after it
// had compared every message and kept info of the Python and the Go generator.
type golden struct {
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

// TestTheStreamIsThePythonWorkloads: for the same phases, the generator writes the stream (and
// keeps the records) the Python workload wrote. A change here is a change of the task's requests.
func TestTheStreamIsThePythonWorkloads(t *testing.T) {
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
	if got := sha256.Sum256(raw); hex.EncodeToString(got[:]) != want.DatasetSHA256 {
		t.Fatalf("%s is not the dataset the stream was recorded with (dataset.py changed?)", path)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range want.Phases {
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
			if got := sum(kept); got != phase.KeptSHA256 {
				t.Errorf("the %d kept records have digest %s, want %s", len(kept), got, phase.KeptSHA256)
			}
		})
	}
}
