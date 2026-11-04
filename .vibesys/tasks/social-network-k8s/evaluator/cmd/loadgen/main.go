// Command loadgen is the task's load generator: the vibesys servicebench
// engine and CLI with the task-owned Social Network benchmark adapter
// registered as application "social-network". Flags and output are those of
// servicebench, plus two task flags removed before servicebench sees them:
//
//	--state-out PATH  write the adapter's write journal to PATH on exit
//	--state-in PATH   replay an earlier phase's journal before the first trial
package main

import (
	"fmt"
	"os"
	"strings"

	"vibesys/microservice-evaluator/api"
	"vibesys/microservice-evaluator/composition"
	"vibesys/microservice-evaluator/drivers/httpdriver"
	"vibesys/microservice-evaluator/servicebenchcli"
	"vibesys/social-network-k8s-evaluator/internal/apps/socialnetwork"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	args, stateIn, stateOut, err := stateFlags(args)
	if err != nil {
		return err
	}
	var journal []byte
	if stateIn != "" {
		if journal, err = os.ReadFile(stateIn); err != nil {
			return err
		}
	}
	var application *socialnetwork.Application
	factory := func(workload api.Workload) (api.Application, error) {
		created, err := socialnetwork.NewWithJournal(workload, journal)
		if err == nil {
			application = created
		}
		return created, err
	}
	runErr := servicebenchcli.Run(args, version, composition.Driver(httpdriver.New()), composition.Application("social-network", factory))
	if stateOut != "" && application != nil {
		encoded, err := application.WriteJournal()
		if err == nil {
			err = os.WriteFile(stateOut, encoded, 0o644)
		}
		if err != nil && runErr == nil {
			runErr = fmt.Errorf("write --state-out: %w", err)
		}
	}
	return runErr
}

// stateFlags removes --state-in/--state-out (as "--flag value" or
// "--flag=value") from args.
func stateFlags(args []string) (rest []string, stateIn, stateOut string, err error) {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		name, value, inline := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if !strings.HasPrefix(arg, "-") || (name != "state-in" && name != "state-out") {
			rest = append(rest, arg)
			continue
		}
		if !inline {
			if index+1 >= len(args) {
				return nil, "", "", fmt.Errorf("%s needs a path", arg)
			}
			index++
			value = args[index]
		}
		if name == "state-in" {
			stateIn = value
		} else {
			stateOut = value
		}
	}
	return rest, stateIn, stateOut, nil
}
