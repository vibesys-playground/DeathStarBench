package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"

	"vibesys/social-network-k8s-evaluator/internal/oracle"
	"vibesys/social-network-k8s-evaluator/internal/snspec"
)

// TestRealCapture checks the oracle spike's real capture offline (a deployment under 16
// closed-loop clients for five 20 s phases, a restart or a cache flush between them: 171,000
// calls, 2 restarts and 2 flushes): it must pass with both specs. It reads
// $SN_ORACLE_CAPTURE, the spike's sn/runs/real1/hist directory (phase<i>/history.jsonl, the
// oracle.Calls of each phase, faults.json and dataset.json), converts it to a history with
// check.NewHistory (each call's Info is its action, see snspec.FromOracleCall) and is skipped
// without it.
func TestRealCapture(t *testing.T) {
	dir := os.Getenv("SN_ORACLE_CAPTURE")
	if dir == "" {
		t.Skip("set SN_ORACLE_CAPTURE to the spike's sn/runs/real1/hist directory")
	}
	began := time.Now()
	h := loadCapture(t, dir)
	converted := time.Since(began)
	sp, err := specs(h)
	if err != nil {
		t.Fatal(err)
	}
	prepared := time.Since(began)
	r := check.Check(h, sp, check.DefaultOptions)
	t.Logf("real1: %s; %d phases, %d faults; conversion %v, dataset %v, check %v (%s); total %v; checker peak heap %d MB; process peak RSS %s",
		r.Verdict, h.Phases, len(h.Faults), converted.Round(time.Millisecond), (prepared - converted).Round(time.Millisecond), r.Elapsed.Round(time.Millisecond),
		perSpec(r), time.Since(began).Round(time.Millisecond), r.PeakHeapBytes>>20, peakRSS())
	if r.Verdict != check.Pass {
		t.Fatalf("%s, want PASS\n%s", r.Verdict, r.Text(10))
	}
}

func perSpec(r *check.Report) string {
	var parts []string
	for _, s := range r.Specs {
		parts = append(parts, fmt.Sprintf("%s: %s, %d calls -> %d pieces, %d partitions, most in flight %d", s.Name, s.Verdict, s.Coverage.Calls, s.Coverage.Pieces, len(s.Partitions), s.MaxInFlight))
	}
	return strings.Join(parts, "; ")
}

// loadCapture converts the capture: one phase per phase<i>/history.jsonl (a write appears in
// the files of the phases that replayed it: only its first appearance counts), the faults of
// faults.json (the i-th follows phase i) and the dataset file.
func loadCapture(t *testing.T, dir string) *check.History {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "phase*", "history.jsonl"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no phase*/history.jsonl in %s", dir)
	}
	slices.Sort(files)
	seen := map[string]bool{}
	var phases [][]check.Call
	for ph, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		var calls []check.Call
		sc := bufio.NewScanner(fh)
		sc.Buffer(make([]byte, 1<<20), 1<<28)
		for sc.Scan() {
			var rec struct {
				Src  string
				Call oracle.Call
			}
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				t.Fatal(err)
			}
			c := rec.Call
			if c.Start.IsZero() {
				continue
			}
			if rec.Src == "write" {
				k := fmt.Sprint(c.Action.Kind, c.Action.Marker, c.Action.User, c.Action.Followee, c.Action.On, c.Start.UnixNano())
				if seen[k] {
					continue
				}
				seen[k] = true
			}
			calls = append(calls, snspec.FromOracleCall(c, ph, uint64(len(calls))))
		}
		fh.Close()
		if err := sc.Err(); err != nil {
			t.Fatal(err)
		}
		slices.SortStableFunc(calls, func(a, b check.Call) int { return int(a.Start - b.Start) })
		phases = append(phases, calls)
	}
	var faults []check.Fault
	if b, err := os.ReadFile(filepath.Join(dir, "faults.json")); err == nil {
		var fl []struct {
			Kind   string `json:"kind"`
			Target string `json:"target"`
			Start  int64  `json:"start_ns"`
			End    int64  `json:"end_ns"`
		}
		if err := json.Unmarshal(b, &fl); err != nil {
			t.Fatal(err)
		}
		for i, f := range fl {
			faults = append(faults, check.Fault{Kind: f.Kind, Target: f.Target, Start: f.Start, End: f.End, AfterPhase: i})
		}
	}
	h := check.NewHistory(phases, faults)
	h.Dataset = filepath.Join(dir, "dataset.json")
	return h
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
