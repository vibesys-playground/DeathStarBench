// Package dataset holds the hotel-k8s task's Hotel Reservation data as the
// oracle sees it: the hotels (profiles, locations, rate plans, recommendation
// attributes, room capacities) and the user accounts, read from the dataset
// file that the task's dataset.py writes (vibesys-evaluator's dataset.json).
// dataset.py is the single owner of these facts: it generates them, the seed
// writes them into MongoDB, and the oracle reads them here. Nothing in Go
// generates seeded data.
//
// The file is JSON: the Python dataclasses field by field, with their
// snake_case names (see dataset.py). Load it once with Use (the oracle's
// --dataset flag) or the HOTEL_DATASET environment variable; Default returns
// it.
package dataset

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	// RateWindowDays is the length of the rate window, which starts on
	// RateWindowStart. Rate plans and canonical searches lie inside it.
	RateWindowDays = 365
	// kmPerDegreeLat is the length of one degree of latitude.
	kmPerDegreeLat = 111.32
	// PathEnv names the dataset file when Use was not called.
	PathEnv = "HOTEL_DATASET"
)

// RateWindowStart is the first night of the rate window.
var RateWindowStart = time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)

// Dataset is the task's data. Hotels[i] has ID strconv.Itoa(i+1).
type Dataset struct {
	Seed   uint64  `json:"seed"`
	Cities []City  `json:"cities"`
	Hotels []Hotel `json:"hotels"`
	Users  []User  `json:"users"`
}

// City is one city of the dataset.
type City struct {
	Name    string `json:"name"`
	Region  string `json:"region"`
	Country string `json:"country"`
	// Lat and Lon are the city centre.
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
	// PhoneFormat is the city's phone template: 'x' a digit, 'A' a letter.
	PhoneFormat string   `json:"phone_format"`
	Districts   []string `json:"districts"`
	// SpreadKm is the standard deviation of hotel offsets from the centre.
	SpreadKm float64 `json:"spread_km"`
	// KmPerDegreeLon is the length of one degree of longitude at Lat.
	KmPerDegreeLon float64 `json:"km_per_degree_lon"`
	// Hotels holds indices into Dataset.Hotels, most popular first.
	Hotels []int `json:"hotels"`
}

// Hotel is one hotel and everything the application stores about it.
type Hotel struct {
	ID          string  `json:"id"`
	City        int     `json:"city"`
	Name        string  `json:"name"`
	Phone       string  `json:"phone"`
	Description string  `json:"description"`
	Address     Address `json:"address"`
	// Lat and Lon are the location as geo-db and recommendation-db store it;
	// profile-db stores their float32 values.
	Lat   float64 `json:"lat"`
	Lon   float64 `json:"lon"`
	Stars int     `json:"stars"`
	// Rating and Price are recommendation-db's "rate" and "price".
	Rating float64 `json:"rating"`
	Price  float64 `json:"price"`
	// Capacity is the number of rooms (reservation-db "number").
	Capacity int `json:"capacity"`
	// Popularity is the 1-based popularity rank within the city.
	Popularity int        `json:"popularity"`
	RatePlans  []RatePlan `json:"rate_plans"`
}

// Address is a hotel's postal address.
type Address struct {
	StreetNumber string `json:"street_number"`
	StreetName   string `json:"street_name"`
	City         string `json:"city"`
	State        string `json:"state"`
	Country      string `json:"country"`
	PostalCode   string `json:"postal_code"`
}

// RatePlan is one rate-db inventory document of a hotel.
type RatePlan struct {
	Code               string  `json:"code"`
	InDate             string  `json:"in_date"`
	OutDate            string  `json:"out_date"`
	RoomCode           string  `json:"room_code"`
	RoomDescription    string  `json:"room_description"`
	BookableRate       float64 `json:"bookable_rate"`
	TotalRate          float64 `json:"total_rate"`
	TotalRateInclusive float64 `json:"total_rate_inclusive"`
}

// User is an account and its plaintext password (user-db stores its SHA-256).
type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

var (
	mu      sync.Mutex
	path    string
	loaded  *Dataset
	loadErr error
)

// Use makes Default read the dataset file at file. Call it before Default.
func Use(file string) {
	mu.Lock()
	defer mu.Unlock()
	path, loaded, loadErr = file, nil, nil
}

// Default returns the task's dataset, read once from the file given to Use
// (or $HOTEL_DATASET). It panics when the file is missing or malformed: every
// caller needs the data, and the oracle checks it at startup (Load).
func Default() *Dataset {
	data, err := Load()
	if err != nil {
		panic(err)
	}
	return data
}

// Load reads the dataset file once and returns it, or why it cannot.
func Load() (*Dataset, error) {
	mu.Lock()
	defer mu.Unlock()
	if loaded != nil || loadErr != nil {
		return loaded, loadErr
	}
	file := path
	if file == "" {
		file = os.Getenv(PathEnv)
	}
	if file == "" {
		loadErr = fmt.Errorf("no dataset file: pass --dataset or set $%s (dataset.json of dataset.py)", PathEnv)
		return nil, loadErr
	}
	loaded, loadErr = Read(file)
	return loaded, loadErr
}

// Read parses and checks one dataset file.
func Read(file string) (*Dataset, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read dataset: %w", err)
	}
	var data Dataset
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("parse dataset %s: %w", file, err)
	}
	if err := data.check(); err != nil {
		return nil, fmt.Errorf("dataset %s: %w", file, err)
	}
	return &data, nil
}

func (d *Dataset) check() error {
	if len(d.Hotels) == 0 || len(d.Users) == 0 || len(d.Cities) == 0 {
		return fmt.Errorf("%d hotels, %d users, %d cities", len(d.Hotels), len(d.Users), len(d.Cities))
	}
	for index := range d.Hotels {
		hotel := &d.Hotels[index]
		if hotel.ID != strconv.Itoa(index+1) {
			return fmt.Errorf("hotel %d has ID %q, want %d", index, hotel.ID, index+1)
		}
		if hotel.City < 0 || hotel.City >= len(d.Cities) || len(hotel.RatePlans) == 0 {
			return fmt.Errorf("hotel %s: city %d, %d rate plans", hotel.ID, hotel.City, len(hotel.RatePlans))
		}
	}
	for index := range d.Cities {
		for _, hotel := range d.Cities[index].Hotels {
			if hotel < 0 || hotel >= len(d.Hotels) || d.Hotels[hotel].City != index {
				return fmt.Errorf("city %s lists hotel index %d of another city", d.Cities[index].Name, hotel)
			}
		}
	}
	return nil
}

// RateWindowEnd is the day after the last night of the rate window.
func RateWindowEnd() time.Time { return RateWindowStart.AddDate(0, 0, RateWindowDays) }

func fill(r *rng, template string) string {
	out := []byte(template)
	for index, char := range out {
		switch char {
		case 'x':
			out[index] = byte('0' + r.intn(10))
		case 'A':
			out[index] = byte('A' + r.intn(26))
		}
	}
	return string(out)
}

func round(value, scale float64) float64 { return math.Round(value*scale) / scale }

// cosDegrees is cos(degrees) by its Taylor series, so the sandbox grid does
// not depend on the platform's math.Cos (dataset.py computes it the same way).
func cosDegrees(degrees float64) float64 {
	x := degrees * math.Pi / 180
	x2 := x * x
	term, sum := 1.0, 1.0
	for n := 1; n <= 12; n++ {
		term *= -x2 / float64((2*n-1)*(2*n))
		sum += term
	}
	return sum
}
