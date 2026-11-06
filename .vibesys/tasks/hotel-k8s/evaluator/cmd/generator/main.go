// Command generator is the hotel-k8s benchmark's request generator: the workload of
// internal/hotelworkload on the vibesys-evaluator Go SDK. The phase pod runs it under loadgen as
// `hotel-generator generate ...` (the generator command line of the SDK's docs/spec.md); bench.py
// names it.
package main

import (
	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/workload"

	"vibesys/hotel-k8s-evaluator/internal/hotelworkload"
)

func main() { workload.Main(hotelworkload.Workload()) }
