// Command podctl waits for and restarts the application's pods from inside
// the job's namespace (see internal/podctl). run.py runs it as the Job
// "ready" after deploying, and passes it to the oracle as the restart hook:
//
//	podctl [flags] ready             every Deployment serves with all replicas
//	podctl [flags] restart one|all   replace one / every pod of each Deployment,
//	                                 then wait as ready does
//
// The oracle appends the mode to the hook command ["podctl", flags..., "restart"].
// Exit status 0 means done; anything else is a failure, explained on stderr.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"vibesys/hotel-k8s-evaluator/internal/podctl"
)

type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }

func (l *listFlag) Set(value string) error {
	*l = append(*l, value)
	return nil
}

func splitList(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "podctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("podctl", flag.ContinueOnError)
	namespace := flags.String("namespace", "", "namespace (default: the pod's own)")
	deployments := flags.String("deployments", "", "comma-separated Deployments to act on")
	registered := flags.String("registered", "", "comma-separated Deployments whose pods register in consul")
	consul := flags.String("consul", "http://consul:8500", "consul agent URL")
	probed := flags.String("probed", "", "comma-separated Deployments whose pods are probed over HTTP")
	probePort := flags.Int("probe-port", 5000, "port of the probed pods")
	var probePaths listFlag
	flags.Var(&probePaths, "probe-path", "path (with query) that must answer 200 on every probed pod; repeatable")
	grace := flags.Int("grace-seconds", 1, "termination grace period of the deleted pods")
	timeout := flags.Duration("timeout", 300*time.Second, "bound of the whole command")
	if err := flags.Parse(args); err != nil {
		return err
	}
	rest := flags.Args()
	if len(rest) == 0 {
		return fmt.Errorf("want ready or restart one|all")
	}
	kube, err := podctl.InCluster(*namespace)
	if err != nil {
		return err
	}
	controller := &podctl.Controller{
		Kube: kube, Deployments: splitList(*deployments), Registered: splitList(*registered),
		ConsulURL: *consul, Probed: splitList(*probed), ProbePort: *probePort, ProbePaths: probePaths,
		Grace: *grace, Timeout: *timeout, Log: os.Stderr,
	}
	if len(controller.Deployments) == 0 {
		return fmt.Errorf("--deployments is empty")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch {
	case rest[0] == "ready" && len(rest) == 1:
		return controller.Ready(ctx)
	case rest[0] == "restart" && len(rest) == 2:
		mode, err := podctl.ParseMode(rest[1])
		if err != nil {
			return err
		}
		return controller.Restart(ctx, mode)
	}
	return fmt.Errorf("want ready or restart one|all, got %q", strings.Join(rest, " "))
}
