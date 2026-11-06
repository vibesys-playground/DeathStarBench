package main

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"
)

// TestRealCaptureMutated shows the real capture's PASS is not vacuous: with the newest post
// dropped from five full home timeline pages of the last phase, the check fails.
func TestRealCaptureMutated(t *testing.T) {
	dir := os.Getenv("SN_ORACLE_CAPTURE")
	if dir == "" {
		t.Skip()
	}
	h := loadCapture(t, dir)
	calls, _ := h.Phase(h.Phases - 1)
	n := 0
	for i := range calls {
		c := &calls[i]
		if c.Op != "home-timeline" || len(c.Steps) != 1 || c.Steps[0].Status != 200 {
			continue
		}
		var posts []json.RawMessage
		if json.Unmarshal(c.Steps[0].Body, &posts) != nil || len(posts) < 10 {
			continue
		}
		c.Steps[0].Body, _ = json.Marshal(posts[1:])
		if n++; n == 5 {
			break
		}
	}
	t.Logf("mutated %d reads", n)
	sp, err := specs(h)
	if err != nil {
		t.Fatal(err)
	}
	r := check.Check(h, sp, check.DefaultOptions)
	t.Logf("mutated: %s in %v", r.Verdict, r.Elapsed.Round(time.Millisecond))
	if r.Verdict != check.Fail {
		t.Fatalf("%s, want FAIL\n%s", r.Verdict, r.Text(4))
	}
}
