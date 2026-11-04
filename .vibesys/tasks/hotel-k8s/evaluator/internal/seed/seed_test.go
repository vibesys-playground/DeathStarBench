package seed

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/bson"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// documentsDigest pins what the seed writes for dataset.Seed. It changes only
// with the dataset or the document schema, and both change the task.
const documentsDigest = "7670033f806863ce76f47d2ba5922e04f5d9496ab5f339190affe5c634bb5cc6"

func TestDocumentsDigestIsPinned(t *testing.T) {
	digest, stats, err := Digest(Collections(dataset.Default()))
	if err != nil {
		t.Fatal(err)
	}
	if digest != documentsDigest {
		t.Fatalf("documents digest = %s, want %s", digest, documentsDigest)
	}
	total := 0
	for _, item := range stats {
		t.Logf("%-34s %6d documents %9d bytes", item.Collection, item.Documents, item.Bytes)
		total += item.Bytes
	}
	t.Logf("total %d bytes", total)
	if total < 10<<20 || total > 100<<20 {
		t.Fatalf("seeded BSON is %d bytes, want tens of MB", total)
	}
}

// The types below are verbatim copies of the application's own: the writers
// of hotelReservation/cmd/*/db.go and the readers of services/*. The proto
// readers have no bson tags, so the driver matches their lower-cased field
// names, and refuses to truncate a float64 into a float32 field.

type appGeoPoint struct {
	Pid  string  `bson:"hotelId"`
	Plat float64 `bson:"lat"`
	Plon float64 `bson:"lon"`
}

type appProfileHotel struct {
	Id          string             `bson:"id"`
	Name        string             `bson:"name"`
	PhoneNumber string             `bson:"phoneNumber"`
	Description string             `bson:"description"`
	Address     *appProfileAddress `bson:"address"`
}

type appProfileAddress struct {
	StreetNumber string  `bson:"streetNumber"`
	StreetName   string  `bson:"streetName"`
	City         string  `bson:"city"`
	State        string  `bson:"state"`
	Country      string  `bson:"country"`
	PostalCode   string  `bson:"postalCode"`
	Lat          float32 `bson:"lat"`
	Lon          float32 `bson:"lon"`
}

type appRoomType struct {
	BookableRate       float64 `bson:"bookableRate"`
	Code               string  `bson:"code"`
	RoomDescription    string  `bson:"roomDescription"`
	TotalRate          float64 `bson:"totalRate"`
	TotalRateInclusive float64 `bson:"totalRateInclusive"`
}

type appRatePlan struct {
	HotelId  string       `bson:"hotelId"`
	Code     string       `bson:"code"`
	InDate   string       `bson:"inDate"`
	OutDate  string       `bson:"outDate"`
	RoomType *appRoomType `bson:"roomType"`
}

type appRecommendation struct {
	HId    string  `bson:"hotelId"`
	HLat   float64 `bson:"lat"`
	HLon   float64 `bson:"lon"`
	HRate  float64 `bson:"rate"`
	HPrice float64 `bson:"price"`
}

type appNumber struct {
	HotelId string `bson:"hotelId"`
	Number  int    `bson:"numberOfRoom"`
}

type appUser struct {
	Username string `bson:"username"`
	Password string `bson:"password"`
}

// pbHotel, pbAddress, pbRatePlan and pbRoomType have the fields (and no bson
// tags) of the generated protobuf structs that profile and rate decode into.
type pbHotel struct {
	Id          string
	Name        string
	PhoneNumber string
	Description string
	Address     *pbAddress
}

type pbAddress struct {
	StreetNumber, StreetName, City, State, Country, PostalCode string
	Lat, Lon                                                   float32
}

type pbRatePlan struct {
	HotelId, Code, InDate, OutDate string
	RoomType                       *pbRoomType
}

type pbRoomType struct {
	BookableRate, TotalRate, TotalRateInclusive float64
	Code, Currency, RoomDescription             string
}

func TestDocumentsMatchApplicationSchemas(t *testing.T) {
	data := dataset.Default()
	collections := Collections(data)
	byName := make(map[string]Collection, len(collections))
	for _, collection := range collections {
		byName[collection.Database+"."+collection.Name] = collection
	}
	wantNames := []string{
		"geo-db.geo", "profile-db.hotels", "rate-db.inventory", "recommendation-db.recommendation",
		"reservation-db.number", "reservation-db.reservation", "user-db.user",
	}
	if len(byName) != len(wantNames) {
		t.Fatalf("collections = %v", byName)
	}
	for _, name := range wantNames {
		if _, ok := byName[name]; !ok {
			t.Fatalf("missing collection %s", name)
		}
	}
	if len(byName["reservation-db.reservation"].Documents) != 0 {
		t.Fatal("the task must not seed reservations")
	}

	// Writers: every document is byte-identical to the application's own
	// writer type filled with the same values.
	planIndex := 0
	for index := range data.Hotels {
		hotel := &data.Hotels[index]
		sameBSON(t, byName["geo-db.geo"].Documents[index], appGeoPoint{hotel.ID, hotel.Lat, hotel.Lon})
		sameBSON(t, byName["profile-db.hotels"].Documents[index], appProfileHotel{
			hotel.ID, hotel.Name, hotel.Phone, hotel.Description, &appProfileAddress{
				hotel.Address.StreetNumber, hotel.Address.StreetName, hotel.Address.City,
				hotel.Address.State, hotel.Address.Country, hotel.Address.PostalCode,
				float32(hotel.Lat), float32(hotel.Lon),
			},
		})
		for _, plan := range hotel.RatePlans {
			sameBSON(t, byName["rate-db.inventory"].Documents[planIndex], appRatePlan{
				hotel.ID, plan.Code, plan.InDate, plan.OutDate, &appRoomType{
					plan.BookableRate, plan.RoomCode, plan.RoomDescription, plan.TotalRate, plan.TotalRateInclusive,
				},
			})
			planIndex++
		}
		sameBSON(t, byName["recommendation-db.recommendation"].Documents[index],
			appRecommendation{hotel.ID, hotel.Lat, hotel.Lon, hotel.Rating, hotel.Price})
		sameBSON(t, byName["reservation-db.number"].Documents[index], appNumber{hotel.ID, hotel.Capacity})
		if t.Failed() {
			t.Fatalf("hotel %s", hotel.ID)
		}
	}
	if planIndex != len(byName["rate-db.inventory"].Documents) {
		t.Fatalf("rate documents = %d, plans = %d", len(byName["rate-db.inventory"].Documents), planIndex)
	}

	// Readers: the services' own types decode every document to the dataset's values.
	for index, document := range byName["profile-db.hotels"].Documents {
		hotel := &data.Hotels[index]
		var decoded pbHotel
		roundTrip(t, document, &decoded)
		want := pbHotel{hotel.ID, hotel.Name, hotel.Phone, hotel.Description, &pbAddress{
			hotel.Address.StreetNumber, hotel.Address.StreetName, hotel.Address.City,
			hotel.Address.State, hotel.Address.Country, hotel.Address.PostalCode,
			float32(hotel.Lat), float32(hotel.Lon),
		}}
		if !reflect.DeepEqual(decoded, want) {
			t.Fatalf("profile %s decodes to %+v %+v", hotel.ID, decoded, decoded.Address)
		}
	}
	planIndex = 0
	for index := range data.Hotels {
		for _, plan := range data.Hotels[index].RatePlans {
			var decoded pbRatePlan
			roundTrip(t, byName["rate-db.inventory"].Documents[planIndex], &decoded)
			want := pbRatePlan{data.Hotels[index].ID, plan.Code, plan.InDate, plan.OutDate, &pbRoomType{
				BookableRate: plan.BookableRate, TotalRate: plan.TotalRate,
				TotalRateInclusive: plan.TotalRateInclusive, Code: plan.RoomCode, RoomDescription: plan.RoomDescription,
			}}
			if !reflect.DeepEqual(decoded, want) {
				t.Fatalf("rate plan %d decodes to %+v", planIndex, decoded)
			}
			planIndex++
		}
	}
	for index, document := range byName["user-db.user"].Documents {
		var decoded appUser
		roundTrip(t, document, &decoded)
		user := data.Users[index]
		if decoded.Username != user.Username || decoded.Password != fmt.Sprintf("%x", sha256.Sum256([]byte(user.Password))) {
			t.Fatalf("user %d decodes to %+v", index, decoded)
		}
	}
	for index, document := range byName["reservation-db.number"].Documents {
		var decoded appNumber
		roundTrip(t, document, &decoded)
		if decoded.HotelId != data.Hotels[index].ID || decoded.Number != data.Hotels[index].Capacity {
			t.Fatalf("capacity %d decodes to %+v", index, decoded)
		}
	}
}

func TestFloat32TruncationIsRefusedLikeTheProfileService(t *testing.T) {
	// The guard the previous test relies on: a profile coordinate that is not
	// a float32 value makes the profile service's decode fail.
	encoded, err := bson.Marshal(bson.D{{Key: "lat", Value: 37.123456789}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded pbAddress
	if err := bson.Unmarshal(encoded, &decoded); err == nil {
		t.Fatal("float64 coordinate decoded into float32 without error")
	}
}

func TestIndexesCoverApplicationQueries(t *testing.T) {
	want := map[string]string{
		"geo-db.geo":                       "hotelId_1 unique",
		"profile-db.hotels":                "id_1 unique",
		"rate-db.inventory":                "hotelId_1",
		"recommendation-db.recommendation": "hotelId_1 unique",
		"reservation-db.number":            "hotelId_1 unique",
		"reservation-db.reservation":       "hotelId_1_inDate_1_outDate_1",
		"user-db.user":                     "username_1 unique",
	}
	for _, collection := range Collections(dataset.Default()) {
		names := make([]string, 0, len(collection.Indexes))
		for _, index := range collection.Indexes {
			name := index.Name
			if index.Unique {
				name += " unique"
			}
			names = append(names, name)
		}
		key := collection.Database + "." + collection.Name
		if got := strings.Join(names, ","); got != want[key] {
			t.Fatalf("%s indexes = %q, want %q", key, got, want[key])
		}
	}
}

func sameBSON(t *testing.T, got, want any) {
	t.Helper()
	left, err := bson.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	right, err := bson.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(left) != string(right) {
		t.Errorf("document %v encodes differently from the application's %v", bson.Raw(left), bson.Raw(right))
	}
}

func roundTrip(t *testing.T, document, target any) {
	t.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := bson.Unmarshal(encoded, target); err != nil {
		t.Fatalf("decode %v: %v", bson.Raw(encoded), err)
	}
}

// fakeStore records what Write sends and holds the documents in memory.
type fakeStore struct {
	mu       sync.Mutex
	held     map[string][]any
	indexes  map[string][]Index
	order    []string
	drop     int // documents to lose, to exercise the count check
	failName string
}

func (f *fakeStore) Replace(_ context.Context, collection Collection) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := collection.Database + "." + collection.Name
	if name == f.failName {
		return 0, errors.New("injected failure")
	}
	if f.held == nil {
		f.held, f.indexes = map[string][]any{}, map[string][]Index{}
	}
	documents := collection.Documents[:len(collection.Documents)-min(f.drop, len(collection.Documents))]
	f.held[name] = append([]any(nil), documents...)
	f.indexes[name] = collection.Indexes
	f.order = append(f.order, name)
	return int64(len(documents)), nil
}

func TestWriteSeedsEveryCollectionIntoItsDatabase(t *testing.T) {
	collections := Collections(dataset.Default())
	stores := map[string]Store{}
	fakes := map[string]*fakeStore{}
	for _, collection := range collections {
		if fakes[collection.Database] == nil {
			fakes[collection.Database] = &fakeStore{}
			stores[collection.Database] = fakes[collection.Database]
		}
	}
	results, err := Write(context.Background(), stores, collections)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(collections) {
		t.Fatalf("results = %v", results)
	}
	for _, collection := range collections {
		fake := fakes[collection.Database]
		name := collection.Database + "." + collection.Name
		if !reflect.DeepEqual(fake.held[name], append([]any(nil), collection.Documents...)) {
			t.Fatalf("%s holds %d documents, want %d", name, len(fake.held[name]), len(collection.Documents))
		}
		if !reflect.DeepEqual(fake.indexes[name], collection.Indexes) {
			t.Fatalf("%s indexes = %v", name, fake.indexes[name])
		}
	}
	if got := strings.Join(fakes["reservation-db"].order, ","); got != "reservation-db.number,reservation-db.reservation" {
		t.Fatalf("reservation-db collections written in order %s", got)
	}

	if _, err := Write(context.Background(), map[string]Store{"geo-db": &fakeStore{}}, collections); err == nil {
		t.Fatal("Write accepted a database without a store")
	}
	short := map[string]Store{}
	for database := range stores {
		short[database] = &fakeStore{drop: 1}
	}
	if _, err := Write(context.Background(), short, collections); err == nil || !strings.Contains(err.Error(), "holds") {
		t.Fatalf("Write accepted a collection with missing documents: %v", err)
	}
	failing := map[string]Store{}
	for database := range stores {
		failing[database] = &fakeStore{failName: "rate-db.inventory"}
	}
	if _, err := Write(context.Background(), failing, collections); err == nil || !strings.Contains(err.Error(), "rate-db.inventory") {
		t.Fatalf("Write hid a store failure: %v", err)
	}
}

func TestPasswordHashMatchesUserService(t *testing.T) {
	// services/user: fmt.Sprintf("%x", sha256.Sum256([]byte(req.Password))).
	for _, password := range []string{"", "a", "joKDz6rSJHHg2ek"} {
		if got, want := PasswordHash(password), fmt.Sprintf("%x", sha256.Sum256([]byte(password))); got != want {
			t.Fatalf("PasswordHash(%q) = %s, want %s", password, got, want)
		}
	}
}
