// Command loadgen is the task's load generator: the vibesys servicebench
// engine and CLI with the task-owned Hotel benchmark adapter registered as
// application "hotel". Flags and output are those of servicebench.
package main

import (
	"fmt"
	"os"

	"vibesys/hotel-k8s-evaluator/internal/apps/hotel"
	"vibesys/microservice-evaluator/composition"
	"vibesys/microservice-evaluator/drivers/httpdriver"
	"vibesys/microservice-evaluator/servicebenchcli"
)

var version = "dev"

func main() {
	err := servicebenchcli.Run(
		os.Args[1:],
		version,
		composition.Driver(httpdriver.New()),
		composition.Application("hotel", hotel.New),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}
