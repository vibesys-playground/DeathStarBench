// Command accuracy-generator is the social-network-k8s accuracy check's request generator: the
// workload of internal/snaccuracy on the vibesys-evaluator Go SDK. bench.py names it
// (sn-accuracy-generator); the evaluator's accuracy run runs it under loadgen with every answer
// captured, for the oracle (cmd/oracle).
package main

import (
	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/workload"

	"vibesys/social-network-k8s-evaluator/internal/snaccuracy"
)

func main() { workload.Main(snaccuracy.Workload()) }
