// Package seed turns the task's dataset into the MongoDB documents the Hotel
// Reservation services read, and writes them. The document types mirror the
// application's own (hotelReservation/cmd/*/db.go and services/*/server.go):
// database and collection names, field names, BSON types (profile
// coordinates are float32, capacities are int32), and the user password as
// the hex SHA-256 the user service compares.
//
// The indexes match the application's query shapes: profile by "id", rate
// plans by "hotelId", capacity by "hotelId", reservations by hotel and night,
// users by "username", and the geo and recommendation catalogs by "hotelId"
// (both services load the whole collection at startup).
package seed

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// Collection is one MongoDB collection with its documents and indexes.
type Collection struct {
	Database  string
	Name      string
	Documents []any
	Indexes   []Index
}

// Index is one secondary index.
type Index struct {
	Name   string
	Keys   bson.D
	Unique bool
}

// The document types copy the application's bson tags and field types.

// geoPoint is geo-db.geo (cmd/geo/db.go point).
type geoPoint struct {
	Pid  string  `bson:"hotelId"`
	Plat float64 `bson:"lat"`
	Plon float64 `bson:"lon"`
}

// profileHotel is profile-db.hotels (cmd/profile/db.go Hotel).
type profileHotel struct {
	Id          string          `bson:"id"`
	Name        string          `bson:"name"`
	PhoneNumber string          `bson:"phoneNumber"`
	Description string          `bson:"description"`
	Address     *profileAddress `bson:"address"`
}

type profileAddress struct {
	StreetNumber string  `bson:"streetNumber"`
	StreetName   string  `bson:"streetName"`
	City         string  `bson:"city"`
	State        string  `bson:"state"`
	Country      string  `bson:"country"`
	PostalCode   string  `bson:"postalCode"`
	Lat          float32 `bson:"lat"`
	Lon          float32 `bson:"lon"`
}

// ratePlan is rate-db.inventory (cmd/rate/db.go RatePlan).
type ratePlan struct {
	HotelId  string    `bson:"hotelId"`
	Code     string    `bson:"code"`
	InDate   string    `bson:"inDate"`
	OutDate  string    `bson:"outDate"`
	RoomType *roomType `bson:"roomType"`
}

type roomType struct {
	BookableRate       float64 `bson:"bookableRate"`
	Code               string  `bson:"code"`
	RoomDescription    string  `bson:"roomDescription"`
	TotalRate          float64 `bson:"totalRate"`
	TotalRateInclusive float64 `bson:"totalRateInclusive"`
}

// recommendationHotel is recommendation-db.recommendation (cmd/recommendation/db.go Hotel).
type recommendationHotel struct {
	HId    string  `bson:"hotelId"`
	HLat   float64 `bson:"lat"`
	HLon   float64 `bson:"lon"`
	HRate  float64 `bson:"rate"`
	HPrice float64 `bson:"price"`
}

// roomNumber is reservation-db.number (cmd/reservation/db.go Number).
type roomNumber struct {
	HotelId string `bson:"hotelId"`
	Number  int32  `bson:"numberOfRoom"`
}

// account is user-db.user (cmd/user/db.go User).
type account struct {
	Username string `bson:"username"`
	Password string `bson:"password"`
}

// Collections returns every collection the task seeds, in a fixed order.
// reservation-db.reservation has no documents: it is created with its index.
func Collections(data *dataset.Dataset) []Collection {
	n := len(data.Hotels)
	geo := make([]any, 0, n)
	profiles := make([]any, 0, n)
	plans := make([]any, 0, 3*n)
	recommendations := make([]any, 0, n)
	numbers := make([]any, 0, n)
	for index := range data.Hotels {
		hotel := &data.Hotels[index]
		geo = append(geo, geoPoint{Pid: hotel.ID, Plat: hotel.Lat, Plon: hotel.Lon})
		profiles = append(profiles, profileHotel{
			Id: hotel.ID, Name: hotel.Name, PhoneNumber: hotel.Phone, Description: hotel.Description,
			Address: &profileAddress{
				StreetNumber: hotel.Address.StreetNumber, StreetName: hotel.Address.StreetName,
				City: hotel.Address.City, State: hotel.Address.State, Country: hotel.Address.Country,
				PostalCode: hotel.Address.PostalCode,
				Lat:        float32(hotel.Lat), Lon: float32(hotel.Lon),
			},
		})
		for _, plan := range hotel.RatePlans {
			plans = append(plans, ratePlan{
				HotelId: hotel.ID, Code: plan.Code, InDate: plan.InDate, OutDate: plan.OutDate,
				RoomType: &roomType{
					BookableRate: plan.BookableRate, Code: plan.RoomCode, RoomDescription: plan.RoomDescription,
					TotalRate: plan.TotalRate, TotalRateInclusive: plan.TotalRateInclusive,
				},
			})
		}
		recommendations = append(recommendations, recommendationHotel{
			HId: hotel.ID, HLat: hotel.Lat, HLon: hotel.Lon, HRate: hotel.Rating, HPrice: hotel.Price,
		})
		numbers = append(numbers, roomNumber{HotelId: hotel.ID, Number: int32(hotel.Capacity)})
	}
	users := make([]any, 0, len(data.Users))
	for _, user := range data.Users {
		users = append(users, account{Username: user.Username, Password: PasswordHash(user.Password)})
	}
	byHotel := func(unique bool) []Index {
		return []Index{{Name: "hotelId_1", Keys: bson.D{{Key: "hotelId", Value: 1}}, Unique: unique}}
	}
	return []Collection{
		{Database: "geo-db", Name: "geo", Documents: geo, Indexes: byHotel(true)},
		{Database: "profile-db", Name: "hotels", Documents: profiles,
			Indexes: []Index{{Name: "id_1", Keys: bson.D{{Key: "id", Value: 1}}, Unique: true}}},
		{Database: "rate-db", Name: "inventory", Documents: plans, Indexes: byHotel(false)},
		{Database: "recommendation-db", Name: "recommendation", Documents: recommendations, Indexes: byHotel(true)},
		{Database: "reservation-db", Name: "number", Documents: numbers, Indexes: byHotel(true)},
		{Database: "reservation-db", Name: "reservation", Indexes: []Index{{
			Name: "hotelId_1_inDate_1_outDate_1",
			Keys: bson.D{{Key: "hotelId", Value: 1}, {Key: "inDate", Value: 1}, {Key: "outDate", Value: 1}},
		}}},
		{Database: "user-db", Name: "user", Documents: users,
			Indexes: []Index{{Name: "username_1", Keys: bson.D{{Key: "username", Value: 1}}, Unique: true}}},
	}
}

// PasswordHash is the stored form of a password: lower-case hex SHA-256, as
// the user service computes it from the login request.
func PasswordHash(password string) string {
	sum := sha256.Sum256([]byte(password))
	return hex.EncodeToString(sum[:])
}

// Stats summarizes the seeded collections.
type Stats struct {
	Collection string `json:"collection"`
	Documents  int    `json:"documents"`
	Bytes      int    `json:"bson_bytes"`
}

// Digest is the SHA-256 over every collection's name and its documents' BSON
// encodings, in order: what a correct seed leaves in MongoDB (without _id).
func Digest(collections []Collection) (string, []Stats, error) {
	hash := sha256.New()
	stats := make([]Stats, 0, len(collections))
	for _, collection := range collections {
		name := collection.Database + "." + collection.Name
		fmt.Fprintf(hash, "%s\x00%d\x00", name, len(collection.Documents))
		size := 0
		for _, document := range collection.Documents {
			encoded, err := bson.Marshal(document)
			if err != nil {
				return "", nil, fmt.Errorf("encode %s document: %w", name, err)
			}
			hash.Write(encoded)
			size += len(encoded)
		}
		stats = append(stats, Stats{Collection: name, Documents: len(collection.Documents), Bytes: size})
	}
	return hex.EncodeToString(hash.Sum(nil)), stats, nil
}
