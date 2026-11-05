// Package dataset is the hotel-k8s task's Hotel Reservation data: the hotels (profiles,
// locations, rate plans, recommendation attributes, room capacities, the rooms already booked
// on each night of the rate window) and the user accounts. It is the single owner of these
// facts: the snapshot builder (internal/snapshot) writes them into MongoDB, and the benchmark's
// generator and the accuracy oracle read them here.
//
// The data is a pure function of a Spec (dataset.py's SPEC, which vibesys-evaluator writes as
// the sample's dataset file): Generate computes the parts every reader needs at once (each
// hotel's city, location, popularity, rating, price and capacity, and every user) and the rest
// of a hotel on demand (Hotel, Occupancy), from per-hotel streams, so a reader holds about
// 100 bytes per hotel however large the profiles and the booking calendar are. Generation uses
// only IEEE-exact arithmetic (+, -, *, /, sqrt, rounding) on SplitMix64 streams, so it is
// identical on every machine.
//
// Shape (Spec.Hotels hotels, Spec.Users users): 20 metropolitan areas in popularity order; the
// hotels spread over them by Zipf rank (exponent 1), each near its area's centre (Irwin-Hall
// normal offsets whose spread grows with the area's size and with the square root of the hotel
// count, so the density stays that of a city; 15% of the hotels at 2.5 times the spread),
// coordinates rounded to 6 decimals and unique; 1-5 rate plans each (RACK over the whole rate
// window, the others over 14-180 days of it); a rating 1 + 4 Beta(5, 2) rounded to 0.01; a room
// capacity by star class; a popularity rank within the area; for every night of the window the
// rooms already booked (seasonal, weekly and lead-time patterns around the hotel's own
// occupancy, never the whole hotel); users with unique names and random passwords. Hotel IDs
// are "1".."N" in a random order of areas.
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
	// RateWindowDays is the length of the rate window, which starts on RateWindowStart. Rate
	// plans, canonical searches and the seeded bookings lie inside it.
	RateWindowDays = 730
	// KmPerDegreeLat is the length of one degree of latitude.
	KmPerDegreeLat = 111.32
	kmPerDegreeLat = KmPerDegreeLat
	// PathEnv names the dataset file when Use was not called.
	PathEnv = "HOTEL_DATASET"
	// Format is the generator's version: Spec.Format must equal it. Bump it with any change of
	// what Generate produces for a spec (the snapshot version covers the documents anyway).
	Format = 3

	outskirtsShare  = 0.15
	outskirtsSpread = 2.5
	// densityHotels is the hotel count at which the areas have their base spread (1.5-4 km);
	// larger datasets spread over sqrt(Hotels / densityHotels) times that.
	densityHotels  = 25_000
	usernameDigits = 0.7
)

// RateWindowStart is the first night of the rate window.
var RateWindowStart = time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)

// Spec is what the data is generated from: dataset.py's SPEC, the sample's dataset file.
type Spec struct {
	Format int    `json:"format"`
	Seed   uint64 `json:"seed"`
	Hotels int    `json:"hotels"`
	Users  int    `json:"users"`
}

// Check reports a spec this generator cannot generate.
func (s Spec) Check() error {
	switch {
	case s.Format != Format:
		return fmt.Errorf("dataset format %d, this generator writes format %d", s.Format, Format)
	case s.Hotels < len(cityTable) || s.Hotels > 50_000_000:
		return fmt.Errorf("%d hotels, want %d to 50,000,000", s.Hotels, len(cityTable))
	case s.Users < 1 || s.Users > 50_000_000:
		return fmt.Errorf("%d users, want 1 to 50,000,000", s.Users)
	}
	return nil
}

// Dataset is the task's data, generated from Spec. Hotel indices run 0..HotelCount()-1, and
// hotel index i has ID strconv.Itoa(i+1).
type Dataset struct {
	Spec   Spec
	Seed   uint64
	Cities []City
	Users  []User
	hotels []core
}

// core is what Generate keeps of each hotel: what the read paths' reference answers need.
type core struct {
	lat, lon      float64
	rating, price float64
	capacity      int32
	popularity    int32
	city          int16
	stars         int8
	plans         int8
}

// City is one metropolitan area of the dataset.
type City struct {
	Name    string `json:"name"`
	Region  string `json:"region"`
	Country string `json:"country"`
	// Lat and Lon are the centre.
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
	// PhoneFormat is the city's phone template: 'x' a digit, 'A' a letter.
	PhoneFormat string   `json:"phone_format"`
	Districts   []string `json:"districts"`
	// SpreadKm is the standard deviation of hotel offsets from the centre.
	SpreadKm float64 `json:"spread_km"`
	// KmPerDegreeLon is the length of one degree of longitude at Lat.
	KmPerDegreeLon float64 `json:"km_per_degree_lon"`
	// Hotels holds hotel indices, most popular first.
	Hotels []int `json:"hotels"`
}

// Hotel is one hotel and everything the application stores about it.
type Hotel struct {
	ID          string  `json:"id"`
	City        int     `json:"city"` // index into Dataset.Cities; -1 for a hotel a write adds
	Name        string  `json:"name"`
	Phone       string  `json:"phone"`
	Description string  `json:"description"`
	Address     Address `json:"address"`
	// Lat and Lon are the location as geo-db and recommendation-db store it; profile-db stores
	// their float32 values.
	Lat   float64 `json:"lat"`
	Lon   float64 `json:"lon"`
	Stars int     `json:"stars"`
	// Rating and Price are recommendation-db's "rate" and "price".
	Rating float64 `json:"rating"`
	Price  float64 `json:"price"`
	// Capacity is the number of rooms (reservation-db "number").
	Capacity int `json:"capacity"`
	// Popularity is the 1-based popularity rank within the city; 0 for an added hotel.
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

// HotelCount is the number of seeded hotels.
func (d *Dataset) HotelCount() int { return len(d.hotels) }

// HotelID is the ID of hotel index i.
func HotelID(index int) string { return strconv.Itoa(index + 1) }

// HotelIndex is the index of a seeded hotel's ID, or false for any other string.
func (d *Dataset) HotelIndex(id string) (int, bool) {
	if id == "" || id[0] < '1' || id[0] > '9' || len(id) > 9 {
		return 0, false
	}
	n, err := strconv.Atoi(id)
	if err != nil || n < 1 || n > len(d.hotels) || strconv.Itoa(n) != id {
		return 0, false
	}
	return n - 1, true
}

// Hotel generates hotel index i in full (a few microseconds; the caller may keep it).
func (d *Dataset) Hotel(index int) *Hotel {
	c := &d.hotels[index]
	hotel := &Hotel{
		ID: HotelID(index), City: int(c.city), Lat: c.lat, Lon: c.lon,
		Popularity: int(c.popularity),
	}
	r := newRNG(d.Seed, "hotel", index)
	hotel.Stars = 1 + pick(r, starWeights)
	describeHotel(r, hotel, &cityTable[c.city])
	return hotel
}

// Location is hotel index i's coordinates (geo-db's float64 values).
func (d *Dataset) Location(index int) (lat, lon float64) {
	return d.hotels[index].lat, d.hotels[index].lon
}

// Rating, Price, Capacity, CityOf and PlanCount are hotel index i's recommendation-db rate
// and price, seeded room count, city index and number of rate plans.
func (d *Dataset) Rating(index int) float64 { return d.hotels[index].rating }
func (d *Dataset) Price(index int) float64  { return d.hotels[index].price }
func (d *Dataset) Capacity(index int) int   { return int(d.hotels[index].capacity) }
func (d *Dataset) CityOf(index int) int     { return int(d.hotels[index].city) }
func (d *Dataset) PlanCount(index int) int  { return int(d.hotels[index].plans) }

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

// UseDataset makes Default return data (tests).
func UseDataset(data *Dataset) {
	mu.Lock()
	defer mu.Unlock()
	path, loaded, loadErr = "", data, nil
}

// Default returns the task's dataset, generated once from the file given to Use (or
// $HOTEL_DATASET). It panics when the file is missing or malformed: every caller needs the
// data, and the oracle checks it at startup (Load).
func Default() *Dataset {
	data, err := Load()
	if err != nil {
		panic(err)
	}
	return data
}

// Load generates the dataset of the dataset file once and returns it, or why it cannot.
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
		loadErr = fmt.Errorf("no dataset file: pass --dataset or set $%s (dataset.py writes it)", PathEnv)
		return nil, loadErr
	}
	loaded, loadErr = Read(file)
	return loaded, loadErr
}

// ReadSpec parses one dataset file.
func ReadSpec(file string) (Spec, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return Spec{}, fmt.Errorf("read dataset: %w", err)
	}
	return ParseSpec(raw)
}

// ParseSpec parses a dataset file's bytes; unknown fields are an error.
func ParseSpec(raw []byte) (Spec, error) {
	var spec Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return Spec{}, fmt.Errorf("parse dataset spec: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Spec{}, fmt.Errorf("parse dataset spec: %w", err)
	}
	for name := range fields {
		switch name {
		case "format", "seed", "hotels", "users":
		default:
			return Spec{}, fmt.Errorf("dataset spec: unknown field %q", name)
		}
	}
	return spec, spec.Check()
}

// Read parses one dataset file and generates its dataset.
func Read(file string) (*Dataset, error) {
	spec, err := ReadSpec(file)
	if err != nil {
		return nil, fmt.Errorf("dataset %s: %w", file, err)
	}
	return Generate(spec)
}

// RateWindowEnd is the day after the last night of the rate window.
func RateWindowEnd() time.Time { return RateWindowStart.AddDate(0, 0, RateWindowDays) }

// Night is the date (YYYY-MM-DD) of night n of the rate window.
func Night(n int) string { return RateWindowStart.AddDate(0, 0, n).Format(time.DateOnly) }

func fill(r Source, template string) string {
	out := []byte(template)
	for index, char := range out {
		switch char {
		case 'x':
			out[index] = byte('0' + r.Below(10))
		case 'A':
			out[index] = byte('A' + r.Below(26))
		}
	}
	return string(out)
}

func round(value, scale float64) float64 { return Round(value, scale) }

// Round is value rounded to a multiple of 1/scale, half away from zero (Go's math.Round).
func Round(value, scale float64) float64 { return math.Round(value*scale) / scale }

// cosDegrees is cos(degrees) by its Taylor series, so the data does not depend on the
// platform's math.Cos.
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
