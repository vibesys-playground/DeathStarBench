// Command seed writes the task's generated Hotel dataset into the
// application's MongoDB databases. run.py runs it as a Kubernetes Job in the
// job's namespace after the MongoDB pods are up and before the application
// services start (they then find their collections non-empty and keep them).
//
//	seed [--address DB=HOST:PORT ...] [--timeout 5m]
//	seed --dump DIR      # write the documents as JSON lines, touch no database
//	seed --summary-only  # print the digests and sizes only
//
// On success it prints one JSON summary line to stdout: the dataset seed and
// digest, the digest of the documents written, and per-collection counts.
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
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/hotel-k8s-evaluator/internal/seed"
)

// defaultAddresses are the MongoDB services of k8s/manifest.yaml, resolved
// inside the job's namespace.
var defaultAddresses = map[string]string{
	"geo-db":            "mongodb-geo:27018",
	"profile-db":        "mongodb-profile:27019",
	"rate-db":           "mongodb-rate:27020",
	"recommendation-db": "mongodb-recommendation:27021",
	"reservation-db":    "mongodb-reservation:27022",
	"user-db":           "mongodb-user:27023",
}

type summary struct {
	DatasetSeed     string        `json:"dataset_seed"`
	DatasetDigest   string        `json:"dataset_digest"`
	DocumentsDigest string        `json:"documents_digest"`
	Collections     []seed.Stats  `json:"collections"`
	Written         []seed.Result `json:"written,omitempty"`
	Seconds         float64       `json:"seconds"`
}

type addressFlag map[string]string

func (a addressFlag) String() string { return fmt.Sprint(map[string]string(a)) }

func (a addressFlag) Set(value string) error {
	database, address, ok := strings.Cut(value, "=")
	if !ok || defaultAddresses[database] == "" || address == "" {
		return fmt.Errorf("want DB=HOST:PORT with DB one of %v", sortedKeys(defaultAddresses))
	}
	a[database] = address
	return nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	addresses := addressFlag{}
	for database, address := range defaultAddresses {
		addresses[database] = address
	}
	flags := flag.NewFlagSet("seed", flag.ContinueOnError)
	flags.Var(addresses, "address", "MongoDB server of one database, DB=HOST:PORT (repeatable)")
	timeout := flags.Duration("timeout", 5*time.Minute, "give up after this long")
	dump := flags.String("dump", "", "write the documents as JSON lines into this directory instead of MongoDB")
	summaryOnly := flags.Bool("summary-only", false, "print the summary without writing anything")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments %v", flags.Args())
	}

	started := time.Now()
	data := dataset.Default()
	collections := seed.Collections(data)
	digest, stats, err := seed.Digest(collections)
	if err != nil {
		return err
	}
	result := summary{
		DatasetSeed: fmt.Sprintf("%#x", data.Seed), DatasetDigest: data.Digest(),
		DocumentsDigest: digest, Collections: stats,
	}
	logf("dataset %s (seed %s): %d hotels, %d users; documents digest %s",
		result.DatasetDigest, result.DatasetSeed, len(data.Hotels), len(data.Users), digest)

	switch {
	case *summaryOnly:
	case *dump != "":
		if err := dumpCollections(*dump, collections); err != nil {
			return err
		}
	default:
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		stores := make(map[string]seed.Store, len(addresses))
		for _, database := range sortedKeys(addresses) {
			logf("connecting to %s at %s", database, addresses[database])
			store, err := seed.ConnectMongo(ctx, addresses[database])
			if err != nil {
				return err
			}
			defer store.Close(context.Background())
			stores[database] = store
		}
		written, err := seed.Write(ctx, stores, collections)
		for _, item := range written {
			logf("seeded %s: %d documents in %.1fs", item.Collection, item.Documents, item.Seconds)
		}
		if err != nil {
			return err
		}
		result.Written = written
	}
	result.Seconds = time.Since(started).Seconds()
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

func dumpCollections(directory string, collections []seed.Collection) error {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	for _, collection := range collections {
		path := filepath.Join(directory, collection.Database+"."+collection.Name+".jsonl")
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		writer := bufio.NewWriter(file)
		for _, document := range collection.Documents {
			line, err := bson.MarshalExtJSON(document, false, false)
			if err != nil {
				file.Close()
				return err
			}
			writer.Write(line)
			writer.WriteByte('\n')
		}
		err = errors.Join(writer.Flush(), file.Close())
		if err != nil {
			return err
		}
		logf("wrote %s (%d documents)", path, len(collection.Documents))
	}
	return nil
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "seed %s "+format+"\n", append([]any{time.Now().UTC().Format(time.TimeOnly)}, args...)...)
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
