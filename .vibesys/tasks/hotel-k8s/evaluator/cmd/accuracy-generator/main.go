// Command accuracy-generator is the hotel-k8s accuracy check's request generator: the workload of
// internal/hotelaccuracy on the vibesys-evaluator Go SDK. The accuracy run's phase pods run it
// under loadgen as `hotel-accuracy-generator generate ...`; bench.py names it.
package main

import (
	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/workload"

	"vibesys/hotel-k8s-evaluator/internal/hotelaccuracy"
)

func main() { workload.Main(hotelaccuracy.Workload()) }
