package hotel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"vibesys/microservice-evaluator/api"
)

// RestartMode says which instances a restart replaces.
type RestartMode string

const (
	// RestartOne replaces one instance of each restarted service while the
	// others keep serving: a restarted instance must not serve older data
	// than its peers.
	RestartOne RestartMode = "one"
	// RestartAll replaces every instance at once: no acknowledged write may
	// live only in the memory of running instances.
	RestartAll RestartMode = "all"
)

// Restarter restarts the stateless services and the reservation cache in
// the given mode and returns once every replacement serves.
//
// Checks and modes: crash_recovery restarts all instances; durable_capacity
// restarts one instance of each service; durable_writes restarts one, reads
// every write back, then restarts all and reads them again.
type Restarter func(context.Context, RestartMode) error

// CommandRestarter runs command with the mode appended as its last
// argument (the task's hook protocol: exit status 0 once the replacements
// serve). The command's output goes to log.
func CommandRestarter(command []string, log io.Writer) (Restarter, error) {
	if len(command) == 0 {
		return nil, fmt.Errorf("restart command is empty")
	}
	for index, argument := range command {
		if argument == "" {
			return nil, fmt.Errorf("restart command argument %d is empty", index)
		}
	}
	command = append([]string(nil), command...)
	return func(ctx context.Context, mode RestartMode) error {
		var output bytes.Buffer
		process := exec.CommandContext(ctx, command[0], append(command[1:], string(mode))...)
		process.Stdout, process.Stderr = &output, &output
		err := process.Run()
		if log != nil {
			fmt.Fprintf(log, "hotel-correctness: restart %s:\n%s", mode, output.String())
		}
		if err != nil {
			return fmt.Errorf("restart %s: %w: %s", mode, err, lastLines(output.String(), 10))
		}
		return nil
	}, nil
}

func lastLines(text string, n int) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// NewWithRestarter is New with restart hooks owned by the task instead of the
// engine's managed lifecycle (which can only stop and start a whole
// candidate).
func NewWithRestarter(restart Restarter) func(api.Workload) (api.AccuracyApplication, error) {
	return func(workload api.Workload) (api.AccuracyApplication, error) {
		application, err := New(workload)
		if err != nil {
			return nil, err
		}
		application.(*Application).restart = restart
		return application, nil
	}
}

// restartHooks are a program's crash and start events for one restart.
type restartHooks struct {
	crash, start func(context.Context) error
}

// hooks returns the restart hooks for mode: the task's Restarter (crash
// replaces the instances and waits for them; start has nothing left to do),
// or else the engine's whole-candidate crash and start.
func (a *Application) hooks(check api.AccuracyContext, mode RestartMode) restartHooks {
	if a.restart != nil {
		return restartHooks{
			crash: func(ctx context.Context) error { return a.restart(ctx, mode) },
			start: func(context.Context) error { return nil },
		}
	}
	return restartHooks{crash: check.Crash, start: check.Start}
}

// hasLifecycle reports whether restarts are available to the checks.
func (a *Application) hasLifecycle(check api.AccuracyContext) bool {
	return a.restart != nil || (check.Crash != nil && check.Start != nil)
}

// durableWriteRestarts are the restarts durable_writes reads its writes
// after: one instance, then all, with the task's hooks; the engine's single
// crash and start otherwise.
func (a *Application) durableWriteRestarts(check api.AccuracyContext) []restartHooks {
	if a.restart != nil {
		return []restartHooks{a.hooks(check, RestartOne), a.hooks(check, RestartAll)}
	}
	return []restartHooks{a.hooks(check, RestartAll)}
}
