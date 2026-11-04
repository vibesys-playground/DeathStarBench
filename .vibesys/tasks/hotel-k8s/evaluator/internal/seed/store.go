package seed

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Store is the MongoDB server holding one database.
type Store interface {
	// Replace makes the collection hold exactly the given documents and
	// indexes, whatever it held before, and returns its document count.
	Replace(ctx context.Context, collection Collection) (int64, error)
}

// Result reports one seeded collection.
type Result struct {
	Collection string  `json:"collection"`
	Documents  int64   `json:"documents"`
	Seconds    float64 `json:"seconds"`
}

// Write seeds every collection into the store of its database. Databases are
// written concurrently, the collections of one database in order. It fails
// unless every collection ends up with exactly its documents.
func Write(ctx context.Context, stores map[string]Store, collections []Collection) ([]Result, error) {
	byDatabase := make(map[string][]Collection)
	for _, collection := range collections {
		if stores[collection.Database] == nil {
			return nil, fmt.Errorf("no store for database %s", collection.Database)
		}
		byDatabase[collection.Database] = append(byDatabase[collection.Database], collection)
	}
	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		results []Result
		errs    []error
	)
	for database, list := range byDatabase {
		wg.Add(1)
		go func(store Store, list []Collection) {
			defer wg.Done()
			for _, collection := range list {
				name := collection.Database + "." + collection.Name
				started := time.Now()
				count, err := store.Replace(ctx, collection)
				if err == nil && count != int64(len(collection.Documents)) {
					err = fmt.Errorf("holds %d documents after seeding, want %d", count, len(collection.Documents))
				}
				mu.Lock()
				if err != nil {
					errs = append(errs, fmt.Errorf("seed %s: %w", name, err))
				} else {
					results = append(results, Result{Collection: name, Documents: count, Seconds: time.Since(started).Seconds()})
				}
				mu.Unlock()
				if err != nil {
					return
				}
			}
		}(stores[database], list)
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].Collection < results[j].Collection })
	if len(errs) > 0 {
		sort.Slice(errs, func(i, j int) bool { return errs[i].Error() < errs[j].Error() })
		return results, errs[0]
	}
	return results, nil
}

// insertBatch is the number of documents per InsertMany call.
const insertBatch = 1000

// MongoStore is a Store on a MongoDB server.
type MongoStore struct{ client *mongo.Client }

// ConnectMongo connects to address (host:port) and pings it until it answers
// or ctx ends: the seed Job may start while mongod is still coming up.
func ConnectMongo(ctx context.Context, address string) (*MongoStore, error) {
	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI("mongodb://"+address).
		SetServerSelectionTimeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", address, err)
	}
	for {
		err = client.Ping(ctx, nil)
		if err == nil {
			return &MongoStore{client: client}, nil
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
func (m *MongoStore) Close(ctx context.Context) error { return m.client.Disconnect(ctx) }

// Replace drops the collection, inserts the documents in batches, creates the
// indexes and counts the documents.
func (m *MongoStore) Replace(ctx context.Context, collection Collection) (int64, error) {
	database := m.client.Database(collection.Database)
	target := database.Collection(collection.Name)
	if err := target.Drop(ctx); err != nil {
		return 0, fmt.Errorf("drop: %w", err)
	}
	if err := database.CreateCollection(ctx, collection.Name); err != nil {
		return 0, fmt.Errorf("create: %w", err)
	}
	for start := 0; start < len(collection.Documents); start += insertBatch {
		end := min(start+insertBatch, len(collection.Documents))
		if _, err := target.InsertMany(ctx, collection.Documents[start:end], options.InsertMany().SetOrdered(true)); err != nil {
			return 0, fmt.Errorf("insert documents %d-%d: %w", start, end, err)
		}
	}
	if len(collection.Indexes) > 0 {
		models := make([]mongo.IndexModel, 0, len(collection.Indexes))
		for _, index := range collection.Indexes {
			models = append(models, mongo.IndexModel{
				Keys:    index.Keys,
				Options: options.Index().SetName(index.Name).SetUnique(index.Unique),
			})
		}
		if _, err := target.Indexes().CreateMany(ctx, models); err != nil {
			return 0, fmt.Errorf("create indexes: %w", err)
		}
	}
	return target.CountDocuments(ctx, bson.D{})
}
