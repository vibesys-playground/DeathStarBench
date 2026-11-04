// Package dataset generates the hotel-k8s task's Hotel Reservation data: the
// hotels (profiles, locations, rate plans, recommendation attributes, room
// capacities) and the user accounts. It is the single owner of these facts.
// The seed command writes them into the application's MongoDB databases before
// the services start, and the load generator and the semantic oracle compute
// their expected answers from the same generated model.
//
// Generation is a pure function of Seed. It uses its own SplitMix64 streams
// (one per hotel, per city and for the users) and only IEEE-exact arithmetic
// (+, -, *, /, sqrt, rounding), so the same seed produces a byte-identical
// dataset on every machine and Go version; Digest pins it.
//
// # Shape
//
// 20 real cities, in popularity order (cityTable). 25,000 hotels are spread
// over them by Zipf rank (exponent 1: London holds about 6,950, Rio de Janeiro
// about 350). Each hotel lies near its city centre: both coordinate offsets
// are approximately normal (Irwin-Hall sum of four uniforms, bounded at 3.5
// sigma) with sigma 1.5 to 4 km growing with the city's size, and 15% of the
// hotels use 2.5 sigma (outskirts, airports). Coordinates are rounded to 6
// decimals and unique. Every hotel has 1 to 5 rate plans (RACK valid over the
// whole rate window, the others over 14 to 180 days of it), a guest rating, a
// room capacity by star class and a popularity rank within its city. 10,000
// users have unique names and random passwords.
//
// # Choices that follow from the application's semantics
//
//   - Search: geo returns the 5 nearest hotels within 10 km (go-geoindex on a
//     0.5 km grid, approximate), rate returns their plans, reservation removes
//     full hotels. The clustering keeps every request location near a city
//     centre within reach of 5 hotels, while the density makes the 5 nearest
//     hotels change every few hundred metres, so searches touch thousands of
//     distinct hotels and cache keys.
//   - The rate service ignores the stay dates, so every hotel's plans are
//     returned whatever the dates; every hotel has at least one plan, so search
//     returns the geo result minus full hotels.
//   - Recommendation by distance scans all hotels for the global nearest;
//     "rate" and "price" return the hotels with the globally highest rating and
//     lowest price, independent of the location. Ratings are 1 + 4 Beta(5, 2)
//     rounded to 0.01, whose density vanishes at 5.00, so the top tie group is
//     a handful of hotels, not hundreds (each answer hotel is one profile in
//     the response). The price is the hotel's cheapest plan rate.
//   - The profile service stores coordinates as float32 and refuses to
//     truncate a float64, so profile coordinates are the float32 values of the
//     geo coordinates.
//   - Hotel IDs are "1".."25000" in a random order of cities, so an ID tells
//     nothing about the location.
package dataset

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Seed is the dataset seed of this task version. Changing it, or any constant
// below, changes the task: results measured on different datasets do not
// compare.
const Seed uint64 = 0x686f74656c2d7631 // "hotel-v1"

const (
	// HotelCount and UserCount are the dataset's sizes.
	HotelCount = 25000
	UserCount  = 10000
	// RateWindowDays is the length of the rate window, which starts on
	// RateWindowStart. Rate plans and canonical searches lie inside it.
	RateWindowDays = 365
	// kmPerDegreeLat is the length of one degree of latitude.
	kmPerDegreeLat = 111.32
	// outskirtsShare of the hotels use outskirtsSpread times the city spread.
	outskirtsShare  = 0.15
	outskirtsSpread = 2.5
)

// RateWindowStart is the first night of the rate window.
var RateWindowStart = time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)

// Dataset is the generated data. Hotels[i] has ID strconv.Itoa(i+1).
type Dataset struct {
	Seed   uint64
	Cities []City
	Hotels []Hotel
	Users  []User
}

// City is one city of the dataset.
type City struct {
	Name, Region, Country string
	// Lat and Lon are the city centre.
	Lat, Lon float64
	// SpreadKm is the standard deviation of hotel offsets from the centre.
	SpreadKm float64
	// KmPerDegreeLon is the length of one degree of longitude at Lat.
	KmPerDegreeLon float64
	// Hotels holds indices into Dataset.Hotels, most popular first.
	Hotels []int
}

// Hotel is one hotel and everything the application stores about it.
type Hotel struct {
	ID          string
	City        int
	Name        string
	Phone       string
	Description string
	Address     Address
	// Lat and Lon are the location as geo-db and recommendation-db store it;
	// profile-db stores their float32 values.
	Lat, Lon float64
	Stars    int
	// Rating and Price are recommendation-db's "rate" and "price".
	Rating float64
	Price  float64
	// Capacity is the number of rooms (reservation-db "number").
	Capacity int
	// Popularity is the 1-based popularity rank within the city.
	Popularity int
	RatePlans  []RatePlan
}

// Address is a hotel's postal address.
type Address struct {
	StreetNumber, StreetName, City, State, Country, PostalCode string
}

// RatePlan is one rate-db inventory document of a hotel.
type RatePlan struct {
	Code               string
	InDate, OutDate    string
	RoomCode           string
	RoomDescription    string
	BookableRate       float64
	TotalRate          float64
	TotalRateInclusive float64
}

// User is an account and its plaintext password (user-db stores its SHA-256).
type User struct {
	Username, Password string
}

var (
	defaultOnce    sync.Once
	defaultDataset *Dataset
)

// Default returns the task's dataset, Generate(Seed), generated once.
func Default() *Dataset {
	defaultOnce.Do(func() { defaultDataset = Generate(Seed) })
	return defaultDataset
}

// Digest is the SHA-256 of the dataset's canonical JSON encoding.
func (d *Dataset) Digest() string {
	encoded, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// RateWindowEnd is the day after the last night of the rate window.
func RateWindowEnd() time.Time { return RateWindowStart.AddDate(0, 0, RateWindowDays) }

// Generate builds the dataset for seed.
func Generate(seed uint64) *Dataset {
	data := &Dataset{Seed: seed}
	counts := zipfCounts(HotelCount, len(cityTable))
	for index, template := range cityTable {
		data.Cities = append(data.Cities, City{
			Name: template.name, Region: template.region, Country: template.country,
			Lat: template.lat, Lon: template.lon,
			SpreadKm:       1.5 + 2.5*math.Sqrt(float64(counts[index])/float64(counts[0])),
			KmPerDegreeLon: kmPerDegreeLat * cosDegrees(template.lat),
		})
	}

	// Hotel IDs are assigned to cities in a random order.
	assignment := make([]int, 0, HotelCount)
	for city, count := range counts {
		for n := 0; n < count; n++ {
			assignment = append(assignment, city)
		}
	}
	shuffle(newRNG(seed, "assignment", 0), len(assignment), func(i, j int) {
		assignment[i], assignment[j] = assignment[j], assignment[i]
	})
	for index, city := range assignment {
		data.Cities[city].Hotels = append(data.Cities[city].Hotels, index)
	}

	data.Hotels = make([]Hotel, HotelCount)
	taken := make(map[[2]float64]struct{}, HotelCount)
	for index, city := range assignment {
		data.Hotels[index] = generateHotel(newRNG(seed, "hotel", index), index, city, &data.Cities[city], taken)
	}
	for city := range data.Cities {
		order := data.Cities[city].Hotels
		shuffle(newRNG(seed, "popularity", city), len(order), func(i, j int) {
			order[i], order[j] = order[j], order[i]
		})
		for rank, index := range order {
			data.Hotels[index].Popularity = rank + 1
		}
	}
	data.Users = generateUsers(newRNG(seed, "users", 0))
	return data
}

var (
	starWeights    = []int{8, 20, 35, 25, 12}
	starPrice      = []float64{0.45, 0.65, 1.0, 1.6, 2.8}
	starRooms      = [][2]int{{10, 60}, {30, 150}, {60, 300}, {100, 450}, {150, 800}}
	planWeights    = []int{20, 30, 25, 15, 10}
	minPlanDays    = 14
	maxPlanDays    = 180
	usernameDigits = 0.7
)

func generateHotel(r *rng, index, cityIndex int, city *City, taken map[[2]float64]struct{}) Hotel {
	template := cityTable[cityIndex]
	stars := 1 + r.pick(starWeights)
	hotel := Hotel{ID: strconv.Itoa(index + 1), City: cityIndex, Stars: stars}

	spread := city.SpreadKm
	if r.float() < outskirtsShare {
		spread *= outskirtsSpread
	}
	for {
		dy, dx := spread*r.normal(), spread*r.normal()
		hotel.Lat = round(city.Lat+dy/kmPerDegreeLat, 1e6)
		hotel.Lon = round(city.Lon+dx/city.KmPerDegreeLon, 1e6)
		key := [2]float64{hotel.Lat, hotel.Lon}
		if _, duplicate := taken[key]; !duplicate {
			taken[key] = struct{}{}
			break
		}
	}

	district := template.districts[r.intn(len(template.districts))]
	bare := strings.TrimPrefix(district, "the ")
	prefix := namePrefixes[r.intn(len(namePrefixes))]
	suffix := nameSuffixes[r.intn(len(nameSuffixes))]
	switch r.pick([]int{40, 35, 25}) {
	case 0:
		hotel.Name = prefix + " " + suffix
	case 1:
		hotel.Name = prefix + " " + bare + " " + suffix
	default:
		hotel.Name = "Hotel " + prefix + " " + template.name
	}
	hotel.Phone = fill(r, template.phone)
	hotel.Address = Address{
		StreetNumber: strconv.Itoa(1 + r.intn(400)),
		StreetName:   template.streets[r.intn(len(template.streets))],
		City:         template.name, State: template.region, Country: template.country,
		PostalCode: fill(r, template.postal),
	}
	adjectives := starAdjectives[stars-1]
	first := r.intn(len(amenities))
	second := (first + 1 + r.intn(len(amenities)-1)) % len(amenities)
	other := template.districts[r.intn(len(template.districts))]
	hotel.Description = fmt.Sprintf(
		"A %d-minute walk from %s, this %s %s in %s is %d minutes from %s and offers %s and %s.",
		2+r.intn(14), transit[r.intn(len(transit))], adjectives[r.intn(len(adjectives))],
		kinds[r.intn(len(kinds))], district, 5+r.intn(26), other, amenities[first], amenities[second],
	)

	// 1 + 4 Beta(5, 2): the fifth smallest of six uniforms is Beta(5, 2).
	var uniforms [6]float64
	for n := range uniforms {
		uniforms[n] = r.float()
	}
	sortFloats(uniforms[:])
	hotel.Rating = round(1+4*uniforms[4], 100)

	rooms := starRooms[stars-1]
	hotel.Capacity = rooms[0] + int(float64(rooms[1]-rooms[0])*(r.float()+r.float())/2)

	base := template.basePrice * starPrice[stars-1] * (0.75 + 0.25*(r.float()+r.float()))
	plans := 1 + r.pick(planWeights)
	codes := make([]int, len(ratePlanCodes)-1)
	for n := range codes {
		codes[n] = n + 1
	}
	hotel.Price = math.MaxFloat64
	for n := 0; n < plans; n++ {
		code, start, days := 0, 0, RateWindowDays
		if n > 0 {
			pick := n - 1 + r.intn(len(codes)-(n-1))
			codes[n-1], codes[pick] = codes[pick], codes[n-1]
			code = codes[n-1]
			days = minPlanDays + r.intn(maxPlanDays-minPlanDays+1)
			start = r.intn(RateWindowDays - days + 1)
		}
		room := roomTypes[r.intn(len(roomTypes))]
		total := round(base*ratePlanCodes[code].factor*room.factor, 100)
		hotel.RatePlans = append(hotel.RatePlans, RatePlan{
			Code:            ratePlanCodes[code].code,
			InDate:          RateWindowStart.AddDate(0, 0, start).Format(time.DateOnly),
			OutDate:         RateWindowStart.AddDate(0, 0, start+days).Format(time.DateOnly),
			RoomCode:        room.code,
			RoomDescription: room.description,
			BookableRate:    total, TotalRate: total,
			TotalRateInclusive: round(total*(1+template.tax), 100),
		})
		hotel.Price = math.Min(hotel.Price, total)
	}
	return hotel
}

func generateUsers(r *rng) []User {
	users := make([]User, 0, UserCount)
	seen := make(map[string]struct{}, UserCount)
	for len(users) < UserCount {
		name := firstNames[r.intn(len(firstNames))] + "." + lastNames[r.intn(len(lastNames))]
		if r.float() < usernameDigits {
			name += strconv.Itoa(r.intn(1000))
		}
		for {
			if _, duplicate := seen[name]; !duplicate {
				break
			}
			name += strconv.Itoa(r.intn(10))
		}
		seen[name] = struct{}{}
		password := make([]byte, 10+r.intn(7))
		for n := range password {
			password[n] = passwordAlphabet[r.intn(len(passwordAlphabet))]
		}
		users = append(users, User{Username: name, Password: string(password)})
	}
	return users
}

// zipfCounts splits total over n ranks in proportion to 1/rank, rounding by
// largest remainder (lower rank first on ties).
func zipfCounts(total, n int) []int {
	weights := ZipfWeights(n, ZipfOne)
	counts := make([]int, n)
	remainders := make([]float64, n)
	assigned := 0
	for index, weight := range weights {
		share := float64(total) * weight
		counts[index] = int(share)
		remainders[index] = share - float64(counts[index])
		assigned += counts[index]
	}
	for ; assigned < total; assigned++ {
		best := 0
		for index := range remainders {
			if remainders[index] > remainders[best] {
				best = index
			}
		}
		counts[best]++
		remainders[best] = -1
	}
	return counts
}

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

func sortFloats(values []float64) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

// cosDegrees is cos(degrees) by its Taylor series, so the generator does not
// depend on the platform's math.Cos.
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
