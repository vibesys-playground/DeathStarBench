package snspec

import (
	"encoding/json"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"

	"vibesys/social-network-k8s-evaluator/internal/snapi"
)

// FromOracleCall is the history's form of an snapi.Call: one operation of one step whose Info
// is the JSON of its action, as the accuracy workload must record it. Times are Unix
// nanoseconds; a call with an error has status 0.
func FromOracleCall(c snapi.Call, phase int, index uint64) check.Call {
	info, err := json.Marshal(c.Action)
	if err != nil {
		panic(err)
	}
	start, end := c.Start.UnixNano(), c.End.UnixNano()
	end = max(end, start)
	op := string(c.Action.Kind)
	if c.Action.Kind == snapi.KindRead {
		op = "user-timeline"
		if c.Action.Home {
			op = "home-timeline"
		}
	}
	step := check.Step{Start: start, End: end, Status: c.Status, Body: c.Body, Error: c.Err}
	if c.Err != "" {
		step.Status = 0
	}
	return check.Call{Phase: phase, Index: index, Client: -1, Op: op, Info: info, Start: start, End: end, Steps: []check.Step{step}}
}
