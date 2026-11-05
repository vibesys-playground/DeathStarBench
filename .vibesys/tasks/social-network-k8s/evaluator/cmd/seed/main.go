// Command seed writes the task's generated Social Network dataset directly
// into the application's data tier (see internal/seed for the schemas). run.py
// runs it as a Kubernetes Job in the namespace once the data-tier pods are
// Ready and before any request reaches the application.
//
//	seed [--dataset-seed N] [--address NAME=HOST:PORT ...] [--timeout 10m]
//	seed --summary-only  # print the summary, write nothing
//	seed --dump DIR      # write the documents as JSON lines, touch nothing
//
// It is idempotent: an empty data tier is written, one that already holds
// this dataset is left alone, anything else is an error. On success the last
// stdout line is a JSON summary.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/seed"
)

type summary struct {
	DatasetSeed   string         `json:"dataset_seed"`
	DatasetDigest string         `json:"dataset_digest"`
	Counts        dataset.Counts `json:"counts"`
	Written       []seed.Result  `json:"written,omitempty"`
	Skipped       bool           `json:"skipped"`
	Seconds       float64        `json:"seconds"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	started := time.Now()
	flags := flag.NewFlagSet("seed", flag.ContinueOnError)
	seedText := flags.String("dataset-seed", strconv.FormatUint(dataset.DefaultSeed, 10), "dataset seed")
	timeout := flags.Duration("timeout", 10*time.Minute, "give up after this long")
	dump := flags.String("dump", "", "write the documents as JSON lines into this directory instead")
	summaryOnly := flags.Bool("summary-only", false, "print the summary without writing anything")
	addresses := map[string]string{}
	flags.Func("address", "data-tier server NAME=HOST:PORT (repeatable)", func(value string) error {
		name, address, ok := strings.Cut(value, "=")
		if !ok || address == "" {
			return fmt.Errorf("want NAME=HOST:PORT")
		}
		addresses[name] = address
		return nil
	})
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments %v", flags.Args())
	}
	seedValue, err := strconv.ParseUint(*seedText, 0, 64)
	if err != nil {
		return fmt.Errorf("--dataset-seed: %w", err)
	}
	data := dataset.Default()
	if seedValue != dataset.DefaultSeed {
		data = dataset.Generate(seedValue, dataset.DefaultConfig())
	}
	result := summary{DatasetSeed: strconv.FormatUint(seedValue, 10), DatasetDigest: data.Digest(), Counts: data.Counts()}
	logf("dataset %s (seed %s): %+v", result.DatasetDigest, result.DatasetSeed, result.Counts)
	targets := seed.Targets(data)
	for name := range addresses {
		if !contains(targets, name) {
			return fmt.Errorf("--address %s: unknown target, want one of %v", name, targets)
		}
	}

	switch {
	case *summaryOnly:
	case *dump != "":
		if err := dumpAll(*dump, data); err != nil {
			return err
		}
	default:
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		mongos := map[string]seed.MongoServer{}
		redises := map[string]seed.RedisServer{}
		for _, collection := range seed.MongoCollections(data) {
			address := addressOf(addresses, collection.Target, "27017")
			logf("connecting to %s at %s", collection.Target, address)
			server, err := seed.ConnectMongo(ctx, address)
			if err != nil {
				return err
			}
			defer server.Close(context.Background())
			mongos[collection.Target] = server
		}
		for _, sets := range seed.RedisSets(data) {
			address := addressOf(addresses, sets.Target, "6379")
			logf("connecting to %s at %s", sets.Target, address)
			server, err := seed.DialRedis(ctx, address)
			if err != nil {
				return err
			}
			defer server.Close()
			redises[sets.Target] = server
		}
		written, skipped, err := seed.Write(ctx, data, mongos, redises)
		for _, item := range written {
			logf("seeded %s: %d documents in %.1fs", item.Target, item.Documents, item.Seconds)
		}
		if err != nil {
			return err
		}
		if skipped {
			logf("the data tier already holds this dataset; nothing written")
		}
		result.Written, result.Skipped = written, skipped
	}
	result.Seconds = time.Since(started).Seconds()
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

func addressOf(addresses map[string]string, target, port string) string {
	if address := addresses[target]; address != "" {
		return address
	}
	return target + ":" + port
}

func dumpAll(directory string, data *dataset.Dataset) error {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	write := func(name string, count int, line func(int) ([]byte, error)) error {
		path := filepath.Join(directory, name+".jsonl")
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		writer := bufio.NewWriter(file)
		for index := 0; index < count; index++ {
			encoded, err := line(index)
			if err != nil {
				file.Close()
				return err
			}
			if encoded != nil {
				writer.Write(encoded)
				writer.WriteByte('\n')
			}
		}
		if err := errors.Join(writer.Flush(), file.Close()); err != nil {
			return err
		}
		logf("wrote %s", path)
		return nil
	}
	for _, collection := range seed.MongoCollections(data) {
		err := write(collection.Target+"."+collection.Database+"."+collection.Name, collection.Count, func(index int) ([]byte, error) {
			return bson.MarshalExtJSON(collection.Document(index), true, false)
		})
		if err != nil {
			return err
		}
	}
	for _, sets := range seed.RedisSets(data) {
		err := write(sets.Target, sets.Count, func(index int) ([]byte, error) {
			key, members := sets.Set(index)
			if len(members) == 0 {
				return nil, nil
			}
			return json.Marshal(map[string]any{"key": key, "members": members})
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func contains(values []string, value string) bool {
	sort.Strings(values)
	at := sort.SearchStrings(values, value)
	return at < len(values) && values[at] == value
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "seed %s "+format+"\n", append([]any{time.Now().UTC().Format(time.TimeOnly)}, args...)...)
}
