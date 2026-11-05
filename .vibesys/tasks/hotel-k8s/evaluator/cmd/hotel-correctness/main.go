// Command hotel-correctness is the task's semantic oracle: servicebench in
// accuracy mode with the Hotel accuracy adapter. It takes servicebench's flags
// plus two of its own:
//
//	--dataset PATH        the task's dataset file, which dataset.py writes
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
	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/composition"
	"vibesys/microservice-evaluator/drivers/httpdriver"
	"vibesys/microservice-evaluator/servicebenchcli"
)

var version = "dev"

const (
	restartFlag = "--restart-command-json"
	datasetFlag = "--dataset"
)

// splitFlag removes one of the task's flags from args and returns its value.
func splitFlag(args []string, flag string) (rest []string, value string, found bool, err error) {
	for index := 0; index < len(args); index++ {
		argument := args[index]
		switch {
		case argument == flag || argument == "-"+strings.TrimPrefix(flag, "--"):
			if index+1 >= len(args) {
				return nil, "", false, fmt.Errorf("%s needs a value", flag)
			}
			value, found = args[index+1], true
			index++
		case strings.HasPrefix(argument, flag+"="):
			value, found = strings.TrimPrefix(argument, flag+"="), true
		default:
			rest = append(rest, argument)
		}
	}
	return rest, value, found, nil
}

// splitRestartFlag removes the restart flag from args and returns its command.
func splitRestartFlag(args []string) (rest []string, command []string, err error) {
	rest, value, found, err := splitFlag(args, restartFlag)
	if err != nil || !found {
		return rest, nil, err
	}
	if err := json.Unmarshal([]byte(value), &command); err != nil {
		return nil, nil, fmt.Errorf("%s must be a JSON string array: %w", restartFlag, err)
	}
	return rest, command, nil
}

func main() {
	args, file, found, err := splitFlag(os.Args[1:], datasetFlag)
	if err == nil && found {
		dataset.Use(file)
	}
	if err == nil {
		_, err = dataset.Load()
	}
	var command []string
	if err == nil {
		args, command, err = splitRestartFlag(args)
	}
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
