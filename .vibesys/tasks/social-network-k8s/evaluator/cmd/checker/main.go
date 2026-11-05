// Command checker is the task's accuracy check of a Social Network
// deployment: the oracle's check programs (internal/oracle), run against the
// public API and judged by the oracle's model (see evaluator/INTERFACE.md):
//
//	checker --base-url URL --seed N|random --restart-command-json '["...", ...]'
//	        [--deployments a,b] [--caches a,b] [--dataset-seed N] [--skip-dataset]
//	        [--freshness-ms 1000] [--replica-reads 12] [--skip-follow-after-flush]
//
// The restart hook is run as ARGV... restart DEPLOYMENT and ARGV... flush
// CACHE. Exit 0: all checks passed; 1: a check failed; 2: setup error.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/oracle"
)

type commandHook struct{ argv []string }

func (h commandHook) run(ctx context.Context, args ...string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, h.argv[0], append(append([]string(nil), h.argv[1:]...), args...)...)
	command.Stdout, command.Stderr = os.Stderr, os.Stderr
	return command.Run()
}

func (h commandHook) Restart(ctx context.Context, deployment string) error {
	return h.run(ctx, "restart", deployment)
}

func (h commandHook) Flush(ctx context.Context, cache string) error {
	return h.run(ctx, "flush", cache)
}

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	flags := flag.NewFlagSet("checker", flag.ContinueOnError)
	baseURL := flags.String("base-url", "", "nginx-thrift URL")
	seedText := flags.String("seed", "random", "seed (integer or random)")
	hookJSON := flags.String("restart-command-json", "", "restart hook argv as a JSON array")
	deployments := flags.String("deployments", strings.Join(oracle.DefaultDeployments, ","), "deployments to restart one pod of")
	caches := flags.String("caches", strings.Join(oracle.DefaultCaches, ","), "caches to flush")
	datasetSeed := flags.String("dataset-seed", strconv.FormatUint(dataset.DefaultSeed, 10), "seeded dataset")
	skipDataset := flags.Bool("skip-dataset", false, "do not check the seeded dataset (deployment already written to)")
	freshness := flags.Int("freshness-ms", 1000, "freshness bound of home timelines")
	replicaReads := flags.Int("replica-reads", 12, "reads per check, to reach every replica")
	skipFollowAfterFlush := flags.Bool("skip-follow-after-flush", false, "do not follow right after a cache flush")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	fail := func(format string, values ...any) int {
		fmt.Fprintf(os.Stderr, "checker: "+format+"\n", values...)
		return 2
	}
	if *baseURL == "" {
		return fail("--base-url is required")
	}
	var seed int64
	if *seedText == "random" {
		var buffer [8]byte
		if _, err := rand.Read(buffer[:]); err != nil {
			return fail("%v", err)
		}
		seed = int64(binary.BigEndian.Uint64(buffer[:]) >> 1)
	} else {
		var err error
		if seed, err = strconv.ParseInt(*seedText, 0, 64); err != nil {
			return fail("--seed: %v", err)
		}
	}
	config := oracle.Config{
		BaseURL: *baseURL, Seed: seed, Bound: time.Duration(*freshness) * time.Millisecond, Replicas: *replicaReads,
		Deployments: split(*deployments), Caches: split(*caches), FollowAfterFlush: !*skipFollowAfterFlush,
		Log: func(format string, values ...any) {
			fmt.Fprintf(os.Stderr, "checker %s "+format+"\n", append([]any{time.Now().UTC().Format(time.TimeOnly)}, values...)...)
		},
	}
	if *hookJSON != "" {
		var argv []string
		if err := json.Unmarshal([]byte(*hookJSON), &argv); err != nil || len(argv) == 0 {
			return fail("--restart-command-json must be a non-empty JSON array of strings")
		}
		config.Hook = commandHook{argv: argv}
	}
	if !*skipDataset {
		value, err := strconv.ParseUint(*datasetSeed, 0, 64)
		if err != nil {
			return fail("--dataset-seed: %v", err)
		}
		config.Dataset = dataset.Default()
		if value != dataset.DefaultSeed {
			config.Dataset = dataset.Generate(value, dataset.DefaultConfig())
		}
	}
	started := time.Now()
	results, err := oracle.RunChecks(context.Background(), config)
	if err != nil {
		return fail("%v", err)
	}
	passed := 0
	for _, result := range results {
		if result.Passed {
			passed++
			fmt.Printf("PASS %s\n", result.Name)
		} else {
			fmt.Printf("FAIL %s: %s\n", result.Name, result.Detail)
		}
	}
	summary, _ := json.Marshal(map[string]any{
		"seed": strconv.FormatInt(seed, 10), "passed": passed, "failed": len(results) - passed,
		"checks": results, "seconds": time.Since(started).Seconds(),
	})
	fmt.Println(string(summary))
	if passed != len(results) {
		return 1
	}
	return 0
}

func split(list string) []string {
	var out []string
	for _, item := range strings.Split(list, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
