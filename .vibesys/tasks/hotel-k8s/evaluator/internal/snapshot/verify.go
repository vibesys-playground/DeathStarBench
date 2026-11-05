package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// Verify checks what the servers hold against d, before a sample's services start: each
// database's marker names d's version, every collection has its indexes and its count, and
// the documents of samples random hotels and users equal the generator's. clients maps a
// database to its server.
func Verify(ctx context.Context, d *dataset.Dataset, clients map[string]*mongo.Client, samples int, seed uint64) error {
	version := Version(d)
	for _, database := range Databases {
		client := clients[database]
		if client == nil {
			return fmt.Errorf("no server for %s", database)
		}
		var marker struct {
			Version  string `bson:"version"`
			Database string `bson:"database"`
		}
		err := client.Database(MarkerDatabase).Collection(MarkerCollection).FindOne(ctx, bson.D{{Key: "_id", Value: "snapshot"}}).Decode(&marker)
		if err != nil {
			return fmt.Errorf("%s: no snapshot marker (%w): the data was not restored from a snapshot", database, err)
		}
		if marker.Version != version || marker.Database != database {
			return fmt.Errorf("%s holds snapshot %s of %s, want %s", database, marker.Version, marker.Database, version)
		}
	}
	random := rand.New(rand.NewPCG(seed, 0x686f74656c))
	hotels := make([]int, samples)
	users := make([]int, samples)
	for k := range hotels {
		hotels[k], users[k] = random.IntN(d.HotelCount()), random.IntN(len(d.Users))
	}
	for _, c := range Collections() {
		collection := clients[c.Database].Database(c.Database).Collection(c.Name)
		count, err := collection.EstimatedDocumentCount(ctx)
		if err != nil {
			return fmt.Errorf("%s.%s: %w", c.Database, c.Name, err)
		}
		if want := c.Count(d); count != want {
			return fmt.Errorf("%s.%s holds %d documents, want %d", c.Database, c.Name, count, want)
		}
		if err := checkIndexes(ctx, collection, c); err != nil {
			return fmt.Errorf("%s.%s: %w", c.Database, c.Name, err)
		}
		key, items, produce := "hotelId", hotels, func(i int) []any { return nil }
		switch {
		case c.PerHotel != nil:
			produce = func(i int) []any { return c.PerHotel(d, i, nil) }
			if c.Name == "hotels" {
				key = "id"
			}
		case c.PerUser != nil:
			key, items, produce = "username", users, func(u int) []any { return c.PerUser(d, u, nil) }
		default:
			continue
		}
		for _, item := range items {
			want := produce(item)
			value := dataset.HotelID(item)
			if c.PerUser != nil {
				value = d.Users[item].Username
			}
			if err := checkDocuments(ctx, collection, key, value, want); err != nil {
				return fmt.Errorf("%s.%s %s=%s: %w", c.Database, c.Name, key, value, err)
			}
		}
	}
	return nil
}

func checkIndexes(ctx context.Context, collection *mongo.Collection, c Collection) error {
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return err
	}
	var listed []struct {
		Name string `bson:"name"`
	}
	if err := cursor.All(ctx, &listed); err != nil {
		return err
	}
	names := map[string]bool{}
	for _, index := range listed {
		names[index.Name] = true
	}
	for _, index := range c.Indexes {
		if !names[index.Name] {
			return fmt.Errorf("no index %s", index.Name)
		}
	}
	return nil
}

// checkDocuments compares the stored documents of one key, without _id and in BSON order of
// their bytes, with the generator's.
func checkDocuments(ctx context.Context, collection *mongo.Collection, key, value string, want []any) error {
	cursor, err := collection.Find(ctx, bson.D{{Key: key, Value: value}})
	if err != nil {
		return err
	}
	var stored []bson.D
	if err := cursor.All(ctx, &stored); err != nil {
		return err
	}
	encode := func(docs []any) ([][]byte, error) {
		out := make([][]byte, 0, len(docs))
		for _, doc := range docs {
			raw, err := bson.Marshal(doc)
			if err != nil {
				return nil, err
			}
			out = append(out, raw)
		}
		sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i], out[j]) < 0 })
		return out, nil
	}
	got := make([]any, 0, len(stored))
	for _, doc := range stored {
		trimmed := doc[:0:0]
		for _, field := range doc {
			if field.Key != "_id" {
				trimmed = append(trimmed, field)
			}
		}
		got = append(got, trimmed)
	}
	gotRaw, err := encode(got)
	if err != nil {
		return err
	}
	wantRaw, err := encode(want)
	if err != nil {
		return err
	}
	if len(gotRaw) != len(wantRaw) {
		return fmt.Errorf("%d documents, want %d", len(gotRaw), len(wantRaw))
	}
	for index := range gotRaw {
		if !bytes.Equal(gotRaw[index], wantRaw[index]) {
			return fmt.Errorf("document %v, want %v", bson.Raw(gotRaw[index]), bson.Raw(wantRaw[index]))
		}
	}
	return nil
}
