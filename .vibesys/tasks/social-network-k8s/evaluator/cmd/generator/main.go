// Command generator is the social-network-k8s benchmark's request generator: the workload of
// internal/snworkload on the vibesys-evaluator Go SDK. The phase pod runs it under loadgen as
// `sn-generator generate ...` and after the phase as `sn-generator check ...` (the generator
// command line of the SDK's docs/spec.md); bench.py names it.
package main

import (
	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/workload"

	"vibesys/social-network-k8s-evaluator/internal/snworkload"
)

func main() { workload.Main(snworkload.Workload()) }
