package seed

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
)

// MarkerTarget holds the marker document vibesys-seed.meta that records a
// completed seeding.
const MarkerTarget = "post-storage-mongodb"

// MongoServer is the part of a MongoDB server the seeder uses.
type MongoServer interface {
	Count(ctx context.Context, database, collection string) (int64, error)
	Insert(ctx context.Context, database, collection string, documents []any) error
	UniqueIndex(ctx context.Context, database, collection, key string) error
	// Marker returns the digest of a completed seeding, or "" if none.
	Marker(ctx context.Context) (string, error)
	SetMarker(ctx context.Context, digest string) error
}

// RedisServer is the part of a Redis server the seeder uses.
type RedisServer interface {
	Pipeline(commands [][]string) ([]any, error)
}

// Result reports one written target.
type Result struct {
	Target    string  `json:"target"`
	Documents int     `json:"documents"`
	Seconds   float64 `json:"seconds"`
}

// Targets lists every data-tier Service the dataset is written to.
func Targets(data *dataset.Dataset) []string {
	var names []string
	for _, collection := range MongoCollections(data) {
		names = append(names, collection.Target)
	}
	for _, sets := range RedisSets(data) {
		names = append(names, sets.Target)
	}
	return names
}

// ErrNotEmpty means the data tier holds data other than this dataset.
var ErrNotEmpty = errors.New("data tier is not empty and holds no completed seeding of this dataset")

// Write seeds an empty data tier and returns skipped=true if it already holds
// this dataset. Targets are written concurrently; the marker is written last.
func Write(ctx context.Context, data *dataset.Dataset, mongos map[string]MongoServer, redises map[string]RedisServer) ([]Result, bool, error) {
	collections := MongoCollections(data)
	sets := RedisSets(data)
	for _, collection := range collections {
		if mongos[collection.Target] == nil {
			return nil, false, fmt.Errorf("no MongoDB server for %s", collection.Target)
		}
	}
	for _, set := range sets {
		if redises[set.Target] == nil {
			return nil, false, fmt.Errorf("no Redis server for %s", set.Target)
		}
	}
	marker, err := mongos[MarkerTarget].Marker(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("read marker: %w", err)
	}
	if marker == data.Digest() {
		return nil, true, nil
	}
	if marker != "" {
		return nil, false, fmt.Errorf("%w (marker digest %s)", ErrNotEmpty, marker)
	}
	for _, collection := range collections {
		count, err := mongos[collection.Target].Count(ctx, collection.Database, collection.Name)
		if err != nil {
			return nil, false, fmt.Errorf("count %s: %w", collection.Target, err)
		}
		if count != 0 {
			return nil, false, fmt.Errorf("%w (%s %s.%s holds %d documents)", ErrNotEmpty, collection.Target, collection.Database, collection.Name, count)
		}
	}
	for _, set := range sets {
		size, err := dbSize(redises[set.Target])
		if err != nil {
			return nil, false, fmt.Errorf("dbsize %s: %w", set.Target, err)
		}
		if size != 0 {
			return nil, false, fmt.Errorf("%w (%s holds %d keys)", ErrNotEmpty, set.Target, size)
		}
	}

	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		results []Result
		errs    []error
	)
	record := func(target string, documents int, started time.Time, err error) {
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			errs = append(errs, fmt.Errorf("seed %s: %w", target, err))
			return
		}
		results = append(results, Result{Target: target, Documents: documents, Seconds: time.Since(started).Seconds()})
	}
	for _, collection := range collections {
		wg.Add(1)
		go func(collection Collection) {
			defer wg.Done()
			started := time.Now()
			err := writeCollection(ctx, mongos[collection.Target], collection)
			record(collection.Target, collection.Count, started, err)
		}(collection)
	}
	for _, set := range sets {
		wg.Add(1)
		go func(set SortedSets) {
			defer wg.Done()
			started := time.Now()
			members, err := writeSets(redises[set.Target], set)
			record(set.Target, members, started, err)
		}(set)
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].Target < results[j].Target })
	if len(errs) > 0 {
		sort.Slice(errs, func(i, j int) bool { return errs[i].Error() < errs[j].Error() })
		return results, false, errs[0]
	}
	if err := mongos[MarkerTarget].SetMarker(ctx, data.Digest()); err != nil {
		return results, false, fmt.Errorf("write marker: %w", err)
	}
	return results, false, nil
}

const insertBatch = 1000

func writeCollection(ctx context.Context, server MongoServer, collection Collection) error {
	batch := make([]any, 0, insertBatch)
	for index := 0; index < collection.Count; index++ {
		batch = append(batch, collection.Document(index))
		if len(batch) == insertBatch || index == collection.Count-1 {
			if err := server.Insert(ctx, collection.Database, collection.Name, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	count, err := server.Count(ctx, collection.Database, collection.Name)
	if err != nil {
		return err
	}
	if count != int64(collection.Count) {
		return fmt.Errorf("holds %d documents after seeding, want %d", count, collection.Count)
	}
	for _, key := range collection.UniqueKeys {
		if err := server.UniqueIndex(ctx, collection.Database, collection.Name, key); err != nil {
			return fmt.Errorf("index %s: %w", key, err)
		}
	}
	return nil
}

// membersPerCommand bounds one ZADD; pipelineCommands bounds one round trip.
const (
	membersPerCommand = 500
	pipelineCommands  = 200
)

func writeSets(server RedisServer, sets SortedSets) (int, error) {
	var commands [][]string
	keys, members := 0, 0
	flush := func() error {
		if len(commands) == 0 {
			return nil
		}
		replies, err := server.Pipeline(commands)
		if err != nil {
			return err
		}
		for _, reply := range replies {
			if failure, ok := reply.(error); ok {
				return failure
			}
		}
		commands = commands[:0]
		return nil
	}
	for index := 0; index < sets.Count; index++ {
		key, entries := sets.Set(index)
		if len(entries) == 0 {
			continue
		}
		keys++
		members += len(entries)
		for start := 0; start < len(entries); start += membersPerCommand {
			end := min(start+membersPerCommand, len(entries))
			command := []string{"ZADD", key}
			for _, entry := range entries[start:end] {
				command = append(command, strconv.FormatInt(entry.Score, 10), entry.Member)
			}
			commands = append(commands, command)
			if len(commands) == pipelineCommands {
				if err := flush(); err != nil {
					return members, err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return members, err
	}
	size, err := dbSize(server)
	if err != nil {
		return members, err
	}
	if size != int64(keys) {
		return members, fmt.Errorf("holds %d keys after seeding, want %d", size, keys)
	}
	return members, nil
}

func dbSize(server RedisServer) (int64, error) {
	replies, err := server.Pipeline([][]string{{"DBSIZE"}})
	if err != nil {
		return 0, err
	}
	if failure, ok := replies[0].(error); ok {
		return 0, failure
	}
	size, ok := replies[0].(int64)
	if !ok {
		return 0, fmt.Errorf("DBSIZE reply %v", replies[0])
	}
	return size, nil
}

// Mongo is a MongoServer on a MongoDB server.
type Mongo struct{ client *mongo.Client }

// ConnectMongo connects to address (host:port) and pings it until it
// answers or ctx ends: the seed Job may start while mongod comes up.
func ConnectMongo(ctx context.Context, address string) (*Mongo, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+address).SetServerSelectionTimeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", address, err)
	}
	for {
		err = client.Ping(ctx, nil)
		if err == nil {
			return &Mongo{client: client}, nil
		}
		select {
		case <-ctx.Done():
			_ = client.Disconnect(context.Background())
			return nil, fmt.Errorf("ping %s: %w (last error: %v)", address, ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}

// Close disconnects.
func (m *Mongo) Close(ctx context.Context) error { return m.client.Disconnect(ctx) }

func (m *Mongo) Count(ctx context.Context, database, collection string) (int64, error) {
	return m.client.Database(database).Collection(collection).CountDocuments(ctx, bson.D{})
}

func (m *Mongo) Insert(ctx context.Context, database, collection string, documents []any) error {
	_, err := m.client.Database(database).Collection(collection).InsertMany(ctx, documents, options.InsertMany().SetOrdered(false))
	return err
}

func (m *Mongo) UniqueIndex(ctx context.Context, database, collection, key string) error {
	_, err := m.client.Database(database).Collection(collection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: key, Value: 1}}, Options: options.Index().SetUnique(true).SetName(key + "_1"),
	})
	return err
}

func (m *Mongo) markers() *mongo.Collection {
	return m.client.Database("vibesys-seed").Collection("meta")
}

func (m *Mongo) Marker(ctx context.Context) (string, error) {
	var marker struct {
		Digest string `bson:"digest"`
	}
	err := m.markers().FindOne(ctx, bson.D{{Key: "_id", Value: "dataset"}}).Decode(&marker)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", nil
	}
	return marker.Digest, err
}

func (m *Mongo) SetMarker(ctx context.Context, digest string) error {
	_, err := m.markers().InsertOne(ctx, bson.D{{Key: "_id", Value: "dataset"}, {Key: "digest", Value: digest}})
	return err
}
