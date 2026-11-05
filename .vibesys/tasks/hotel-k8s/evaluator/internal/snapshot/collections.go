// Package snapshot writes the task's dataset into MongoDB once per dataset version, as data
// files on the node, and checks what a sample's MongoDB servers hold.
//
// A sample does not insert its data: each MongoDB pod of the data tier copies its database's
// files from the node's snapshot (an init container of the official image, k8s/manifest.yaml)
// before mongod starts, so the writes of one sample never reach the next. `hotel-data ensure`
// (a Job of the task, before a sample deploys) builds the snapshot the first time a version is
// asked for, by running mongod of the data tier's image on the generated documents, and later
// only checks it is there. The version is a digest of what the snapshot holds (Version), so a
// change of the generator, of the collections and indexes, or of the MongoDB image names a new
// snapshot, and an unchanged one is reused.
package snapshot

import (
	"crypto/sha256"
	"sort"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// MongoImage is the data tier's MongoDB image (k8s/manifest.yaml, and the Dockerfile's data
// stage that the builder runs): the snapshot's files are this mongod's.
const MongoImage = "docker.io/mongo:5.0@sha256:41108d183e972dcbf98d09ed83f6cfc89a471a3f15d06f9d64a95e45d9db8dd2"

// Layout is the version of the snapshot's layout on the node (the directories and files
// Build writes and the manifest's restore reads). Bump it with any change of them.
const Layout = 1

// Databases are the six MongoDB servers' databases, one per server.
var Databases = []string{"geo-db", "profile-db", "rate-db", "recommendation-db", "reservation-db", "user-db"}

// MarkerDatabase and MarkerCollection hold, on each server, one document naming the snapshot
// version its files were built for.
const (
	MarkerDatabase   = "hotel-data"
	MarkerCollection = "snapshot"
)

// Index is one index the builder creates before inserting (the services' own startup
// indexes included, so a service finds them and does not build one over the data).
type Index struct {
	Name   string
	Keys   bson.D
	Unique bool
	Sparse bool
}

// Collection is one collection of the seed: its database, name, indexes, and documents.
type Collection struct {
	Database string
	Name     string
	Indexes  []Index
	// PerHotel appends hotel index i's documents; PerUser user index u's. One of them is set,
	// or neither for a collection the seed creates empty.
	PerHotel func(d *dataset.Dataset, i int, out []any) []any
	PerUser  func(d *dataset.Dataset, u int, out []any) []any
	// Count is the number of documents (computed without generating them where possible).
	Count func(d *dataset.Dataset) int64
}

// The documents, with the services' own field names and BSON types: profile coordinates are
// float32 (the profile service's schema), capacities and booked rooms int32.
type (
	geoDoc struct {
		HotelID string  `bson:"hotelId"`
		Lat     float64 `bson:"lat"`
		Lon     float64 `bson:"lon"`
	}
	addressDoc struct {
		StreetNumber string  `bson:"streetNumber"`
		StreetName   string  `bson:"streetName"`
		City         string  `bson:"city"`
		State        string  `bson:"state"`
		Country      string  `bson:"country"`
		PostalCode   string  `bson:"postalCode"`
		Lat          float32 `bson:"lat"`
		Lon          float32 `bson:"lon"`
	}
	profileDoc struct {
		ID          string     `bson:"id"`
		Name        string     `bson:"name"`
		PhoneNumber string     `bson:"phoneNumber"`
		Description string     `bson:"description"`
		Address     addressDoc `bson:"address"`
	}
	roomDoc struct {
		BookableRate       float64 `bson:"bookableRate"`
		Code               string  `bson:"code"`
		RoomDescription    string  `bson:"roomDescription"`
		TotalRate          float64 `bson:"totalRate"`
		TotalRateInclusive float64 `bson:"totalRateInclusive"`
	}
	planDoc struct {
		HotelID  string  `bson:"hotelId"`
		Code     string  `bson:"code"`
		InDate   string  `bson:"inDate"`
		OutDate  string  `bson:"outDate"`
		RoomType roomDoc `bson:"roomType"`
	}
	recommendationDoc struct {
		HotelID string  `bson:"hotelId"`
		Lat     float64 `bson:"lat"`
		Lon     float64 `bson:"lon"`
		Rate    float64 `bson:"rate"`
		Price   float64 `bson:"price"`
	}
	numberDoc struct {
		HotelID      string `bson:"hotelId"`
		NumberOfRoom int32  `bson:"numberOfRoom"`
	}
	// nightDoc is the reservation service's count of the rooms booked on one night.
	nightDoc struct {
		HotelID string `bson:"hotelId"`
		Night   string `bson:"night"`
		Rooms   int32  `bson:"rooms"`
	}
	userDoc struct {
		Username string `bson:"username"`
		Password string `bson:"password"`
	}
)

func keys(names ...string) bson.D {
	out := bson.D{}
	for _, name := range names {
		out = append(out, bson.E{Key: name, Value: 1})
	}
	return out
}

var byHotel = []Index{{Name: "hotelId_1", Keys: keys("hotelId"), Unique: true}}

// added_1 is the sparse index geo and recommendation create at startup (hotelReservation's
// changes.EnsureIndex): built here, so the services find it.
var addedIndex = Index{Name: "added_1", Keys: keys("added"), Sparse: true}

func perHotel(d *dataset.Dataset) int64 { return int64(d.HotelCount()) }

// Collections is every collection the seed holds, in the order Build writes them.
func Collections() []Collection {
	return []Collection{
		{Database: "geo-db", Name: "geo", Indexes: append(append([]Index{}, byHotel...), addedIndex), Count: perHotel,
			PerHotel: func(d *dataset.Dataset, i int, out []any) []any {
				lat, lon := d.Location(i)
				return append(out, geoDoc{dataset.HotelID(i), lat, lon})
			}},
		{Database: "profile-db", Name: "hotels", Indexes: []Index{{Name: "id_1", Keys: keys("id"), Unique: true}}, Count: perHotel,
			PerHotel: func(d *dataset.Dataset, i int, out []any) []any {
				h := d.Hotel(i)
				a := h.Address
				return append(out, profileDoc{h.ID, h.Name, h.Phone, h.Description, addressDoc{
					a.StreetNumber, a.StreetName, a.City, a.State, a.Country, a.PostalCode, float32(h.Lat), float32(h.Lon)}})
			}},
		{Database: "rate-db", Name: "inventory", Indexes: []Index{{Name: "hotelId_1", Keys: keys("hotelId")}},
			Count: func(d *dataset.Dataset) int64 {
				total := int64(0)
				for i := range d.HotelCount() {
					total += int64(d.PlanCount(i))
				}
				return total
			},
			PerHotel: func(d *dataset.Dataset, i int, out []any) []any {
				h := d.Hotel(i)
				for _, p := range h.RatePlans {
					out = append(out, planDoc{h.ID, p.Code, p.InDate, p.OutDate,
						roomDoc{p.BookableRate, p.RoomCode, p.RoomDescription, p.TotalRate, p.TotalRateInclusive}})
				}
				return out
			}},
		{Database: "recommendation-db", Name: "recommendation", Indexes: append(append([]Index{}, byHotel...), addedIndex), Count: perHotel,
			PerHotel: func(d *dataset.Dataset, i int, out []any) []any {
				lat, lon := d.Location(i)
				return append(out, recommendationDoc{dataset.HotelID(i), lat, lon, d.Rating(i), d.Price(i)})
			}},
		{Database: "reservation-db", Name: "number", Indexes: byHotel, Count: perHotel,
			PerHotel: func(d *dataset.Dataset, i int, out []any) []any {
				return append(out, numberDoc{dataset.HotelID(i), int32(d.Capacity(i))})
			}},
		{Database: "reservation-db", Name: "reservation",
			Indexes: []Index{{Name: "hotelId_1_inDate_1_outDate_1", Keys: keys("hotelId", "inDate", "outDate")}},
			Count:   func(*dataset.Dataset) int64 { return 0 }},
		{Database: "reservation-db", Name: "nights",
			Indexes: []Index{{Name: "hotelId_1_night_1", Keys: keys("hotelId", "night"), Unique: true}},
			Count:   countNights,
			PerHotel: func(d *dataset.Dataset, i int, out []any) []any {
				id := dataset.HotelID(i)
				for n, rooms := range d.Occupancy(i) {
					if rooms > 0 {
						out = append(out, nightDoc{id, dataset.Night(n), int32(rooms)})
					}
				}
				return out
			}},
		{Database: "user-db", Name: "user", Indexes: []Index{{Name: "username_1", Keys: keys("username"), Unique: true}},
			Count: func(d *dataset.Dataset) int64 { return int64(len(d.Users)) },
			PerUser: func(d *dataset.Dataset, u int, out []any) []any {
				user := d.Users[u]
				return append(out, userDoc{user.Username, PasswordHash(user.Password)})
			}},
	}
}

// PasswordHash is the stored form of a password: hex SHA-256, as the user service computes it.
func PasswordHash(password string) string {
	sum := sha256.Sum256([]byte(password))
	const hex = "0123456789abcdef"
	out := make([]byte, 64)
	for i, b := range sum {
		out[2*i], out[2*i+1] = hex[b>>4], hex[b&15]
	}
	return string(out)
}

func countNights(d *dataset.Dataset) int64 {
	counts := make([]int64, d.HotelCount())
	parallel(d.HotelCount(), func(i int) {
		for _, rooms := range d.Occupancy(i) {
			if rooms > 0 {
				counts[i]++
			}
		}
	})
	total := int64(0)
	for _, count := range counts {
		total += count
	}
	return total
}

// KeyOrder is the hotel indices in the order of their IDs as strings ("1", "10", "100", ...),
// the order of every hotel-keyed index: inserting in it appends to the indexes.
func KeyOrder(d *dataset.Dataset) []int {
	order := make([]int, d.HotelCount())
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return dataset.HotelID(order[a]) < dataset.HotelID(order[b]) })
	return order
}

// UserOrder is the user indices in username order.
func UserOrder(d *dataset.Dataset) []int {
	order := make([]int, len(d.Users))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return d.Users[order[a]].Username < d.Users[order[b]].Username })
	return order
}

func itoa(n int) string { return strconv.Itoa(n) }
