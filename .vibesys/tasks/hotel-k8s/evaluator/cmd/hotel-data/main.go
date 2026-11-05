// Command hotel-data owns the hotel-k8s dataset's MongoDB snapshot (internal/snapshot):
//
//	hotel-data version --dataset FILE                  print the snapshot version of the dataset
//	hotel-data ensure  --dataset FILE --root DIR       build what the snapshot lacks (the data Job)
//	hotel-data verify  --dataset FILE --server DB=HOST:PORT ...   check a sample's servers (the seed)
//
// FILE is the dataset file dataset.py writes (its SPEC).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/hotel-k8s-evaluator/internal/snapshot"
)

func main() {
	if len(os.Args) < 2 {
		fail("usage: hotel-data version|ensure|verify --dataset FILE ...")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	command, args := os.Args[1], os.Args[2:]
	flags := flag.NewFlagSet(command, flag.ExitOnError)
	file := flags.String("dataset", "", "the dataset file (dataset.py's SPEC)")
	root := flags.String("root", "", "ensure: the snapshot root on the node")
	mongod := flags.String("mongod", "mongod", "ensure: the mongod binary")
	samples := flags.Int("samples", 200, "verify: random hotels and users compared document by document")
	var servers multi
	flags.Var(&servers, "server", "verify: DATABASE=HOST:PORT, once per database")
	_ = flags.Parse(args)
	if *file == "" {
		fail("--dataset is required")
	}
	started := time.Now()
	data, err := dataset.Read(*file)
	if err != nil {
		fail(err.Error())
	}
	switch command {
	case "version":
		fmt.Println(snapshot.Version(data))
	case "ensure":
		if *root == "" {
			fail("--root is required")
		}
		opts := snapshot.DefaultOptions()
		opts.Mongod = *mongod
		records, err := snapshot.Ensure(ctx, data, *root, opts)
		if err != nil {
			fail(err.Error())
		}
		out, _ := json.Marshal(map[string]any{"version": snapshot.Version(data), "databases": records,
			"seconds": time.Since(started).Seconds()})
		fmt.Println(string(out))
	case "verify":
		clients := map[string]*mongo.Client{}
		for _, server := range servers {
			database, address, ok := strings.Cut(server, "=")
			if !ok {
				fail("--server wants DATABASE=HOST:PORT, got " + server)
			}
			client, err := connect(ctx, "mongodb://"+address)
			if err != nil {
				fail(fmt.Sprintf("%s at %s: %v", database, address, err))
			}
			clients[database] = client
		}
		seed := uint64(time.Now().UnixNano())
		if err := snapshot.Verify(ctx, data, clients, *samples, seed); err != nil {
			fail(fmt.Sprintf("verify (seed %d): %v", seed, err))
		}
		fmt.Printf("verified snapshot %s: markers, counts, indexes and %d hotels and users (seed %d) in %.1fs\n",
			snapshot.Version(data), *samples, seed, time.Since(started).Seconds())
	default:
		fail("unknown command " + command)
	}
}

// connect waits up to 5 minutes for a server to answer.
func connect(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetServerSelectionTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Minute)
	for {
		err := client.Ping(ctx, nil)
		if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
			return client, err
		}
		time.Sleep(time.Second)
	}
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func fail(message string) {
	fmt.Fprintln(os.Stderr, "hotel-data: "+message)
	os.Exit(1)
}
