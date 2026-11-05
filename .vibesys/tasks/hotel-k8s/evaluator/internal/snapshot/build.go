package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// On the node, under the snapshot root (the data tier's hostPath):
//
//	<root>/<version>/<database>/db/        mongod's files after a clean shutdown
//	<root>/<version>/<database>/COMPLETE   Complete as JSON, written last
//	<root>/<version>/.lock                 held while a builder works on the version
//	<root>/<version>/last-used             touched by every Ensure
//
// A database is built in <root>/<version>/.<database>.<pid> and renamed into place, so a
// reader sees either no directory or a complete one. The restore of a sample (the data
// tier's init containers) copies <database>/db after checking COMPLETE.
const (
	CompleteFile = "COMPLETE"
	DataDir      = "db"
	lockFile     = ".lock"
	usedFile     = "last-used"
	// staleAfter: Ensure deletes other versions not used for this long.
	staleAfter = 72 * time.Hour
)

// Complete is the record of one built database.
type Complete struct {
	Version  string           `json:"version"`
	Database string           `json:"database"`
	Counts   map[string]int64 `json:"counts"`
	Bytes    int64            `json:"bytes"`
	Seconds  float64          `json:"seconds"`
	Built    time.Time        `json:"built"`
}

// Options are the builder's resources: each database's mongod cache and insert workers.
type Options struct {
	Mongod    string // the mongod binary
	CacheGB   map[string]float64
	Workers   map[string]int
	BatchSize int
	Log       func(format string, args ...any)
}

// DefaultOptions sizes a builder of about 24 CPUs and 20 GiB: reservation-db (the nights)
// gets most of both.
func DefaultOptions() Options {
	return Options{
		Mongod: "mongod",
		CacheGB: map[string]float64{"reservation-db": 8, "profile-db": 1.5, "rate-db": 1.5,
			"geo-db": 0.5, "recommendation-db": 0.5, "user-db": 1},
		Workers: map[string]int{"reservation-db": 16, "profile-db": 4, "rate-db": 4,
			"geo-db": 2, "recommendation-db": 2, "user-db": 2},
		BatchSize: 4000,
		Log:       func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) },
	}
}

// Ensure makes the snapshot of d under root complete, building the databases it lacks, and
// returns each database's record. Concurrent Ensures of one version serialize on its lock;
// another version's are independent. It then deletes versions unused for staleAfter.
func Ensure(ctx context.Context, d *dataset.Dataset, root string, opts Options) ([]Complete, error) {
	version := Version(d)
	dir := filepath.Join(root, version)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, lockFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Close() }()
	opts.Log("snapshot %s: waiting for its lock", version)
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return nil, fmt.Errorf("lock %s: %w", dir, err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()
	now := time.Now()
	if err := touch(filepath.Join(dir, usedFile), now); err != nil {
		return nil, err
	}

	records := make([]Complete, len(Databases))
	var missing []int
	for index, database := range Databases {
		record, err := ReadComplete(filepath.Join(dir, database))
		switch {
		case err == nil && record.Version == version:
			records[index] = record
		case err == nil || errors.Is(err, os.ErrNotExist):
			missing = append(missing, index)
		default:
			return nil, err
		}
	}
	if len(missing) > 0 {
		opts.Log("snapshot %s: building %d databases", version, len(missing))
		var wg sync.WaitGroup
		errs := make([]error, len(Databases))
		for port, index := range missing {
			wg.Add(1)
			go func() {
				defer wg.Done()
				records[index], errs[index] = buildDatabase(ctx, d, version, dir, Databases[index], 28000+port, opts)
			}()
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			return nil, err
		}
	}
	sweepStale(root, version, now, opts)
	return records, nil
}

// ReadComplete reads a database's COMPLETE record.
func ReadComplete(databaseDir string) (Complete, error) {
	raw, err := os.ReadFile(filepath.Join(databaseDir, CompleteFile))
	if err != nil {
		return Complete{}, err
	}
	var record Complete
	if err := json.Unmarshal(raw, &record); err != nil {
		return Complete{}, fmt.Errorf("%s: %w", databaseDir, err)
	}
	return record, nil
}

func touch(file string, now time.Time) error {
	if err := os.WriteFile(file, []byte(now.UTC().Format(time.RFC3339)+"\n"), 0o644); err != nil {
		return err
	}
	return os.Chtimes(file, now, now)
}

// sweepStale deletes other versions whose last use is older than staleAfter and that no
// builder holds.
func sweepStale(root, keep string, now time.Time, opts Options) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".deleting-") {
			_ = os.RemoveAll(filepath.Join(root, entry.Name())) // a sweep interrupted
			continue
		}
		if !entry.IsDir() || entry.Name() == keep {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		used, err := os.Stat(filepath.Join(dir, usedFile))
		if err != nil || now.Sub(used.ModTime()) < staleAfter {
			continue
		}
		lock, err := os.OpenFile(filepath.Join(dir, lockFile), os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			continue
		}
		// Held, the version is moved aside (an Ensure of it from now on starts afresh), then
		// deleted once its lock file is closed.
		doomed := filepath.Join(root, ".deleting-"+entry.Name())
		moved := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil && os.Rename(dir, doomed) == nil
		_ = lock.Close()
		if moved {
			opts.Log("snapshot %s: unused since %s, deleting", entry.Name(), used.ModTime().UTC().Format(time.RFC3339))
			_ = os.RemoveAll(doomed)
		}
	}
}

// buildDatabase runs a mongod on a fresh directory, writes the database's collections and
// marker, shuts it down cleanly and moves the directory into place.
func buildDatabase(ctx context.Context, d *dataset.Dataset, version, dir, database string, port int, opts Options) (Complete, error) {
	started := time.Now()
	work := filepath.Join(dir, fmt.Sprintf(".%s.%d", database, os.Getpid()))
	_ = os.RemoveAll(work)
	if err := os.MkdirAll(filepath.Join(work, DataDir), 0o755); err != nil {
		return Complete{}, err
	}
	defer func() { _ = os.RemoveAll(work) }()
	logFile, err := os.Create(filepath.Join(work, "mongod.log"))
	if err != nil {
		return Complete{}, err
	}
	defer func() { _ = logFile.Close() }()
	cache := opts.CacheGB[database]
	if cache <= 0 {
		cache = 1
	}
	mongod := exec.CommandContext(ctx, opts.Mongod, "--dbpath", filepath.Join(work, DataDir), "--port", fmt.Sprint(port),
		"--bind_ip", "127.0.0.1", "--nojournal", "--wiredTigerCacheSizeGB", fmt.Sprint(cache))
	mongod.Stdout, mongod.Stderr = logFile, logFile
	if err := mongod.Start(); err != nil {
		return Complete{}, fmt.Errorf("%s: start mongod: %w", database, err)
	}
	exited := make(chan error, 1)
	go func() { exited <- mongod.Wait() }()
	stopped := false
	defer func() {
		if !stopped {
			_ = mongod.Process.Kill()
			<-exited
		}
	}()
	client, err := connect(ctx, fmt.Sprintf("mongodb://127.0.0.1:%d", port), exited)
	if err != nil {
		return Complete{}, fmt.Errorf("%s: %w (mongod log: %s)", database, err, tail(filepath.Join(work, "mongod.log")))
	}
	counts := map[string]int64{}
	for _, c := range Collections() {
		if c.Database != database {
			continue
		}
		count, err := writeCollection(ctx, client, d, c, opts)
		if err != nil {
			return Complete{}, fmt.Errorf("%s.%s: %w", database, c.Name, err)
		}
		counts[c.Name] = count
	}
	marker := bson.D{{Key: "_id", Value: "snapshot"}, {Key: "version", Value: version}, {Key: "database", Value: database}}
	if _, err := client.Database(MarkerDatabase).Collection(MarkerCollection).InsertOne(ctx, marker); err != nil {
		return Complete{}, fmt.Errorf("%s: marker: %w", database, err)
	}
	// A clean shutdown (SIGTERM) checkpoints everything: the files are complete without a
	// journal.
	_ = client.Disconnect(ctx)
	if err := mongod.Process.Signal(syscall.SIGTERM); err != nil {
		return Complete{}, fmt.Errorf("%s: stop mongod: %w", database, err)
	}
	select {
	case err := <-exited:
		stopped = true
		if err != nil {
			return Complete{}, fmt.Errorf("%s: mongod exited with %v (log: %s)", database, err, tail(filepath.Join(work, "mongod.log")))
		}
	case <-time.After(10 * time.Minute):
		return Complete{}, fmt.Errorf("%s: mongod did not shut down", database)
	}
	_ = logFile.Close()
	_ = os.Remove(filepath.Join(work, "mongod.log"))
	bytes, err := treeSize(filepath.Join(work, DataDir))
	if err != nil {
		return Complete{}, err
	}
	record := Complete{Version: version, Database: database, Counts: counts, Bytes: bytes,
		Seconds: time.Since(started).Seconds(), Built: time.Now().UTC()}
	raw, _ := json.MarshalIndent(record, "", "  ")
	if err := os.WriteFile(filepath.Join(work, CompleteFile), append(raw, '\n'), 0o644); err != nil {
		return Complete{}, err
	}
	final := filepath.Join(dir, database)
	_ = os.RemoveAll(final) // an incomplete or other-version leftover
	if err := os.Rename(work, final); err != nil {
		return Complete{}, err
	}
	opts.Log("snapshot %s: %s built in %.0fs, %.1f GB on disk, %v", version, database, record.Seconds, float64(bytes)/1e9, counts)
	return record, nil
}

func connect(ctx context.Context, uri string, exited <-chan error) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetMaxPoolSize(64))
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		select {
		case err := <-exited:
			return nil, fmt.Errorf("mongod exited at startup: %v", err)
		default:
		}
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := client.Ping(pingCtx, nil)
		cancel()
		if err == nil {
			return client, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("mongod not answering: %w", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// writeCollection creates a collection and its indexes, then inserts its documents in key
// order from several workers, and checks the count.
func writeCollection(ctx context.Context, client *mongo.Client, d *dataset.Dataset, c Collection, opts Options) (int64, error) {
	db := client.Database(c.Database)
	if err := db.CreateCollection(ctx, c.Name); err != nil {
		return 0, err
	}
	collection := db.Collection(c.Name)
	for _, index := range c.Indexes {
		model := mongo.IndexModel{Keys: index.Keys, Options: options.Index().SetName(index.Name)}
		if index.Unique {
			model.Options.SetUnique(true)
		}
		if index.Sparse {
			model.Options.SetSparse(true)
		}
		if _, err := collection.Indexes().CreateOne(ctx, model); err != nil {
			return 0, err
		}
	}
	var order []int
	var produce func(int, []any) []any
	switch {
	case c.PerHotel != nil:
		order, produce = KeyOrder(d), func(i int, out []any) []any { return c.PerHotel(d, i, out) }
	case c.PerUser != nil:
		order, produce = UserOrder(d), func(u int, out []any) []any { return c.PerUser(d, u, out) }
	}
	want := c.Count(d)
	workers := max(opts.Workers[c.Database], 1)
	batch := max(opts.BatchSize, 1)
	// Workers take consecutive runs of the key order, so the inserts stay near the end of
	// every index.
	run := max(batch/8, 1)
	var next, written atomic.Int64
	started := time.Now()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				n := written.Load()
				opts.Log("%s.%s: %d of %d documents (%.0f/s)", c.Database, c.Name, n, want, float64(n)/time.Since(started).Seconds())
			}
		}
	}()
	defer close(done)
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			docs := make([]any, 0, batch+800)
			flush := func() error {
				if len(docs) == 0 {
					return nil
				}
				_, err := collection.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
				written.Add(int64(len(docs)))
				docs = docs[:0]
				return err
			}
			for {
				start := int(next.Add(int64(run))) - run
				if start >= len(order) {
					errs[worker] = flush()
					return
				}
				for _, item := range order[start:min(start+run, len(order))] {
					docs = produce(item, docs)
				}
				if len(docs) >= batch {
					if err := flush(); err != nil {
						errs[worker] = err
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return 0, err
	}
	count, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return 0, err
	}
	if count != want {
		return count, fmt.Errorf("holds %d documents, want %d", count, want)
	}
	opts.Log("%s.%s: %d documents in %.0fs", c.Database, c.Name, count, time.Since(started).Seconds())
	return count, nil
}

func treeSize(root string) (int64, error) {
	total := int64(0)
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			total += info.Size()
		}
		return err
	})
	return total, err
}

func tail(file string) string {
	f, err := os.Open(file)
	if err != nil {
		return err.Error()
	}
	defer func() { _ = f.Close() }()
	if info, err := f.Stat(); err == nil && info.Size() > 2000 {
		_, _ = f.Seek(info.Size()-2000, io.SeekStart)
	}
	raw, _ := io.ReadAll(f)
	return strings.TrimSpace(string(raw))
}
