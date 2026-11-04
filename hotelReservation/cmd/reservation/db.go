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

type Reservation struct {
	HotelId      string `bson:"hotelId"`
	CustomerName string `bson:"customerName"`
	InDate       string `bson:"inDate"`
	OutDate      string `bson:"outDate"`
	Number       int    `bson:"number"`
}

type Number struct {
	HotelId string `bson:"hotelId"`
	Number  int    `bson:"numberOfRoom"`
}

func initializeDatabase(url string) (*mongo.Client, func()) {
	log.Info().Msg("Generating test data...")

	newReservations := []interface{}{
		Reservation{"4", "Alice", "2015-04-09", "2015-04-10", 1},
	}

	newNumbers := []interface{}{
		Number{"1", 200},
		Number{"2", 200},
		Number{"3", 200},
		Number{"4", 200},
		Number{"5", 200},
		Number{"6", 200},
	}

	for i := 7; i <= 80; i++ {
		hotelID := strconv.Itoa(i)

		roomNumber := 200
		if i%3 == 1 {
			roomNumber = 300
		} else if i%3 == 2 {
			roomNumber = 250
		}

		newNumbers = append(newNumbers, Number{hotelID, roomNumber})
	}

	uri := fmt.Sprintf("mongodb://%s", url)
	log.Info().Msgf("Attempting connection to %v", uri)

	opts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		log.Panic().Msg(err.Error())
	}
	log.Info().Msg("Successfully connected to MongoDB")

	database := client.Database("reservation-db")
	resCollection := database.Collection("reservation")
	numCollection := database.Collection("number")

	// The capacity collection marks a seeded database: a seeded database may
	// legitimately hold no reservations yet.
	if hasData(numCollection) {
		log.Info().Msgf("Collection %s already holds data; not inserting test data", numCollection.Name())
	} else {
		_, err = resCollection.InsertMany(context.TODO(), newReservations)
		if err != nil {
			log.Fatal().Msg(err.Error())
		}

		_, err = numCollection.InsertMany(context.TODO(), newNumbers)
		if err != nil {
			log.Fatal().Msg(err.Error())
		}
		log.Info().Msg("Successfully inserted test data into reservation DB")
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
