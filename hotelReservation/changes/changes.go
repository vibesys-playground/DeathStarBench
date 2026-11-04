// Package changes tells the replicas of a service that keeps an in-memory
// copy of its MongoDB catalog (geo's index, recommendation's hotel list)
// that the catalog changed.
//
// A writer stores its documents first, then increments a version counter in
// the same database (Bump). Every replica reads the counter at startup
// before it loads the catalog, and then polls it every PollInterval (Watch);
// when the counter moved, it reloads the documents that writers add (marked
// with Added). A replica therefore serves an acknowledged write within about
// one poll interval plus a reload, well within the task's 1 s bound, and the
// polling costs one indexed point read per interval.
package changes

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// PollInterval is how often a replica reads the version counter.
const PollInterval = 200 * time.Millisecond

// Added is the field that marks documents written after seeding; writers set
// it to true and replicas reload the documents that have it.
const Added = "added"

const (
	collectionName = "changes"
	counterID      = "version"
)

// Version reads the database's change counter (0 before the first change).
func Version(ctx context.Context, database *mongo.Database) (int64, error) {
	var counter struct {
		N int64 `bson:"n"`
	}
	err := database.Collection(collectionName).FindOne(ctx, bson.D{{Key: "_id", Value: counterID}}).Decode(&counter)
	if err == mongo.ErrNoDocuments {
		return 0, nil
	}
	return counter.N, err
}

// Bump increments the counter. Call it after the change is stored.
func Bump(ctx context.Context, database *mongo.Database) error {
	_, err := database.Collection(collectionName).UpdateOne(ctx,
		bson.D{{Key: "_id", Value: counterID}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}}}},
		options.Update().SetUpsert(true),
	)
	return err
}

// EnsureIndex creates the sparse index on Added that reloads query.
func EnsureIndex(ctx context.Context, collection *mongo.Collection) error {
	_, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: Added, Value: 1}},
		Options: options.Index().SetSparse(true).SetName(Added + "_1"),
	})
	return err
}

// Watch polls the counter forever, starting from seen, and calls reload
// whenever it moved. A failed reload is retried at the next poll.
func Watch(database *mongo.Database, seen int64, reload func(context.Context) error) {
	for range time.Tick(PollInterval) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		version, err := Version(ctx, database)
		if err == nil && version != seen {
			if err = reload(ctx); err == nil {
				seen = version
			}
		}
		cancel()
		if err != nil {
			log.Error().Msgf("Failed to refresh from %s: %v", database.Name(), err)
		}
	}
}
