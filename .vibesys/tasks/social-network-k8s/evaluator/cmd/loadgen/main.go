// Command loadgen is the task's load generator: the vibesys servicebench
// engine and CLI with the task-owned Social Network benchmark adapter
// registered as application "social-network". Flags and output are those of
// servicebench.
package main

import (
	"fmt"
	"os"

	"vibesys/microservice-evaluator/composition"
	"vibesys/microservice-evaluator/drivers/httpdriver"
	"vibesys/microservice-evaluator/servicebenchcli"
	"vibesys/social-network-k8s-evaluator/internal/apps/socialnetwork"
)

var version = "dev"

func main() {
	err := servicebenchcli.Run(
		os.Args[1:],
		version,
		composition.Driver(httpdriver.New()),
		composition.Application("social-network", socialnetwork.New),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}
