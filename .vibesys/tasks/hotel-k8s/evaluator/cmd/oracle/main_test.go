package main

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"
	pb "github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/gen/vibesys/evaluator/v1"
)

// TestCaptures checks the three real captures of the oracle spike offline, as the accuracy
// command will check a history: the application as it is (base1, base2) passes and the base
// with an injected reservation race (mutant1) fails. It reads $HOTEL_ORACLE_CAPTURES, a
// directory of <name>/run (phase-<i>/{results,summary}.binpb, state/kept.binpb, faults.tsv) and
// <name>/dataset.json, and is skipped without it (each check holds the 1.5 million hotel
// dataset and up to 2.5 GB). Every capture is converted to the history directory format with a
// written history.binpb that includes its faults.
func TestCaptures(t *testing.T) {
	root := os.Getenv("HOTEL_ORACLE_CAPTURES")
	if root == "" {
		t.Skip("set HOTEL_ORACLE_CAPTURES to the spike's harness/runs directory")
	}
	for _, c := range []struct {
		name string
		want check.Verdict
	}{{"base1", check.Pass}, {"base2", check.Pass}, {"mutant1", check.Fail}} {
		t.Run(c.name, func(t *testing.T) {
			dir := historyDir(t, filepath.Join(root, c.name))
			began := time.Now()
			h, err := check.OpenHistory(dir)
			if err != nil {
				t.Fatal(err)
			}
			sp, err := specs(h)
			if err != nil {
				t.Fatal(err)
			}
			loaded := time.Since(began)
			r := check.Check(h, sp, check.DefaultOptions)
			t.Logf("%s: %s; %d phases, %d faults; dataset and catalog %v, check %v, total %v; checker peak heap %d MB; process peak RSS %s",
				c.name, r.Verdict, h.Phases, len(h.Faults), loaded.Round(time.Millisecond), r.Elapsed.Round(time.Millisecond),
				time.Since(began).Round(time.Millisecond), r.PeakHeapBytes>>20, peakRSS())
			if r.Verdict != c.want {
				t.Fatalf("%s, want %s\n%s", r.Verdict, c.want, r.Text(10))
			}
			if r.Verdict == check.Fail {
				t.Logf("first failure:\n%s", r.Text(2))
			}
			runtime.GC()
		})
	}
}

// historyDir writes the manifest of a capture into a new directory that links its phases and
// dataset: the faults of faults.tsv (kind, target, unix nanos of the fault's start; each
// follows the phase of its position) with the restart or flush as a point in time, since the
// spike's runs recorded no end.
func historyDir(t *testing.T, run string) string {
	t.Helper()
	dir := t.TempDir()
	phases := 0
	for {
		if _, err := os.Stat(filepath.Join(run, "run", "phase-"+strconv.Itoa(phases))); err != nil {
			break
		}
		if err := os.Symlink(filepath.Join(run, "run", "phase-"+strconv.Itoa(phases)), filepath.Join(dir, "phase-"+strconv.Itoa(phases))); err != nil {
			t.Fatal(err)
		}
		phases++
	}
	if err := os.Symlink(filepath.Join(run, "dataset.json"), filepath.Join(dir, "dataset.json")); err != nil {
		t.Fatal(err)
	}
	m := &pb.HistoryManifest{Phases: uint32(phases), Dataset: "dataset.json"}
	f, err := os.Open(filepath.Join(run, "run", "faults.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		parts := strings.Fields(s.Text())
		if len(parts) != 3 {
			continue
		}
		at, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		kind := strings.ReplaceAll(strings.Replace(parts[0], "-one", "", 1), "-", "_") // restart-one: restart
		m.Faults = append(m.Faults, &pb.FaultEvent{Kind: kind, Target: parts[1], StartUnixNanos: at, EndUnixNanos: at, AfterPhase: uint32(len(m.Faults))})
	}
	raw, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "history.binpb"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func peakRSS() string {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return "?"
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:"))
		}
	}
	return "?"
}
