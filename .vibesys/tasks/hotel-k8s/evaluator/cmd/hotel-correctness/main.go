// Command hotel-correctness is the task's semantic oracle: servicebench in
// accuracy mode with the Hotel accuracy adapter. It takes servicebench's flags
// plus one of its own:
//
//	--restart-command-json '["podctl", ..., "restart"]'
//
// the restart hook (see hotel.CommandRestarter): the oracle runs the command
// with "one" or "all" appended to restart one or every instance of the
// stateless services and the reservation cache. Without it the lifecycle
// checks use servicebench's --run-command-json/--stop-command-json, if given.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"vibesys/hotel-k8s-evaluator/internal/accuracyapps/hotel"
	"vibesys/microservice-evaluator/composition"
	"vibesys/microservice-evaluator/drivers/httpdriver"
	"vibesys/microservice-evaluator/servicebenchcli"
)

var version = "dev"

const restartFlag = "--restart-command-json"

// splitRestartFlag removes the task's flag from args and returns its value.
func splitRestartFlag(args []string) (rest []string, command []string, err error) {
	var value string
	found := false
	for index := 0; index < len(args); index++ {
		argument := args[index]
		switch {
		case argument == restartFlag || argument == "-"+strings.TrimPrefix(restartFlag, "--"):
			if index+1 >= len(args) {
				return nil, nil, fmt.Errorf("%s needs a value", restartFlag)
			}
			value, found = args[index+1], true
			index++
		case strings.HasPrefix(argument, restartFlag+"="):
			value, found = strings.TrimPrefix(argument, restartFlag+"="), true
		default:
			rest = append(rest, argument)
		}
	}
	if !found {
		return rest, nil, nil
	}
	if err := json.Unmarshal([]byte(value), &command); err != nil {
		return nil, nil, fmt.Errorf("%s must be a JSON string array: %w", restartFlag, err)
	}
	return rest, command, nil
}

func main() {
	args, command, err := splitRestartFlag(os.Args[1:])
	if err == nil {
		factory := hotel.New
		if command != nil {
			var restart hotel.Restarter
			restart, err = hotel.CommandRestarter(command, os.Stderr)
			factory = hotel.NewWithRestarter(restart)
		}
		if err == nil {
			err = servicebenchcli.Run(
				args,
				version,
				composition.Driver(httpdriver.New()),
				composition.AccuracyApplication("hotel", factory),
			)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "hotel-correctness:", err)
		os.Exit(1)
	}
}
