package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type point struct {
	Pid  string  `bson:"hotelId"`
	Plat float64 `bson:"lat"`
	Plon float64 `bson:"lon"`
}

func initializeDatabase(url string) (*mongo.Client, func()) {
	log.Info().Msg("Generating test data...")

	newPoints := []interface{}{
		point{"1", 37.7867, -122.4112},
		point{"2", 37.7854, -122.4005},
		point{"3", 37.7854, -122.4071},
		point{"4", 37.7936, -122.3930},
		point{"5", 37.7831, -122.4181},
		point{"6", 37.7863, -122.4015},
	}

	for i := 7; i <= 80; i++ {
		hotelID := strconv.Itoa(i)
		lat := 37.7835 + float64(i)/500.0*3
		lon := -122.41 + float64(i)/500.0*4

		newPoints = append(newPoints, point{hotelID, lat, lon})
	}

	uri := fmt.Sprintf("mongodb://%s", url)
	log.Info().Msgf("Attempting connection to %v", uri)

	opts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		log.Panic().Msg(err.Error())
	}
	log.Info().Msg("Successfully connected to MongoDB")

	collection := client.Database("geo-db").Collection("geo")
	if hasData(collection) {
		log.Info().Msgf("Collection %s already holds data; not inserting test data", collection.Name())
	} else {
		_, err = collection.InsertMany(context.TODO(), newPoints)
		if err != nil {
			log.Fatal().Msg(err.Error())
		}
		log.Info().Msg("Successfully inserted test data into geo DB")
	}

	return client, func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			log.Fatal().Msg(err.Error())
		}
	}
}

// hasData reports whether the collection already holds documents, for example
// a dataset that the deployment wrote before the service started. The service
// then leaves the data alone and inserts no test data.
func hasData(collection *mongo.Collection) bool {
	count, err := collection.CountDocuments(context.TODO(), bson.D{}, options.Count().SetLimit(1))
	if err != nil {
		log.Fatal().Msgf("Failed to count documents in %s: %v", collection.Name(), err)
	}
	return count > 0
}
