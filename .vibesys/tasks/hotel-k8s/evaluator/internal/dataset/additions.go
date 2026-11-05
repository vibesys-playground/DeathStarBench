package dataset

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Values that writes put into the application: the workload and the oracle
// change profiles and prices of seeded hotels and add new hotels through the
// frontend's admin routes. Seeded data (Generate) does not depend on them.

// Source is a stream of random draws that write values are drawn from: the oracle's own
// SplitMix64 streams, or a request generator's (the vibesys-evaluator SDK's workload.Rand, whose
// Below and Random are the same draws as dataset.py's Rand).
type Source interface {
	// Below is one draw modulo n.
	Below(n int) int
	// Random is a uniform float in [0, 1) from the top 53 bits of one draw.
	Random() float64
}

// NewPhone draws a phone number in the format of the hotel's city.
func NewPhone(hotel *Hotel, random uint64) string {
	return NewPhoneFrom(Default(), hotel, &rng{state: random})
}

// NewPhoneFrom is dataset.py's new_phone: a phone number in the format of the hotel's city
// in data (the additions' city for an added hotel), drawn from r.
func NewPhoneFrom(data *Dataset, hotel *Hotel, r Source) string {
	return fill(r, templateOf(data, hotel).phone)
}

// NewName draws a hotel name as the generator does for the hotel's city.
func NewName(hotel *Hotel, random uint64) string {
	return NewNameFrom(Default(), hotel, &rng{state: random})
}

// NewNameFrom is dataset.py's new_name: a hotel name as the generator draws them for the
// hotel's city in data, drawn from r.
func NewNameFrom(data *Dataset, hotel *Hotel, r Source) string {
	template := templateOf(data, hotel)
	prefix := namePrefixes[r.Below(len(namePrefixes))]
	suffix := nameSuffixes[r.Below(len(nameSuffixes))]
	district := template.districts[r.Below(len(template.districts))]
	if r.Below(2) == 0 {
		return prefix + " " + suffix
	}
	return prefix + " " + trimThe(district) + " " + suffix
}

// AddedHotel generates a hotel to add under id at (lat, lon), from random:
// a profile in the additions' fictional city, a star class, a rating, a
// capacity and 1-5 rate plans over the rate window, as Generate draws them.
// Its City is -1 and its Popularity 0.
func AddedHotel(id string, random uint64, lat, lon float64) Hotel {
	return AddedHotelFrom(id, &rng{state: Mix(random, 0)}, lat, lon)
}

// AddedHotelFrom is AddedHotel drawn from r: dataset.py's added_hotel.
func AddedHotelFrom(id string, r Source, lat, lon float64) Hotel {
	hotel := Hotel{ID: id, City: -1, Stars: 1 + pick(r, starWeights), Lat: lat, Lon: lon}
	describeHotel(r, &hotel, &additionCity)
	return hotel
}

// templateOf is what writes draw a seeded hotel's new phone and name from:
// its city in the dataset file; additionCity for an added hotel.
func templateOf(data *Dataset, hotel *Hotel) *cityTemplate {
	if hotel.City < 0 || hotel.City >= len(data.Cities) {
		return &additionCity
	}
	city := &data.Cities[hotel.City]
	return &cityTemplate{name: city.Name, region: city.Region, country: city.Country,
		lat: city.Lat, lon: city.Lon, phone: city.PhoneFormat, districts: city.Districts}
}

func trimThe(district string) string {
	if len(district) > 4 && district[:4] == "the " {
		return district[4:]
	}
	return district
}

// additionCity describes the hotels that writes add. They lie in the
// additions' region (Sandbox), far from every seeded city.
var additionCity = cityTemplate{
	"Isla Nueva", "Pacifico", "Chile", -40, -125, "+56 32 xxx xxxx", "2xxxxxx", 140, 0.19,
	[]string{"the Harbour", "Playa Norte", "the Old Town", "Punta Alta", "the Marina"},
	[]string{"Av del Mar", "Calle Larga", "Paseo Costanera", "Calle del Faro", "Av Pacifico"},
}

// The additions' region, in the South Pacific, more than 1,000 km from
// every seeded city, is cut into cells of SandboxSpacingKm. A run that adds
// hotels puts them within SandboxRadiusKm of one cell's centre, so a search
// (10 km) or a nearest-hotel query there sees no other cell's hotels, and
// queries near the seeded cities see none of the added hotels.
const (
	sandboxLatMin    = -58.0
	sandboxLatMax    = -20.0
	sandboxLonMin    = -165.0
	sandboxLonMax    = -85.0
	SandboxSpacingKm = 25.0
	SandboxRadiusKm  = 2.0
)

// sandboxKmPerDegreeLon is the length of a degree of longitude at the
// region's latitude farthest from the equator, where columns are closest.
var sandboxKmPerDegreeLon = kmPerDegreeLat * cosDegrees(sandboxLatMin)

var (
	sandboxLatSpan = sandboxLatMax - sandboxLatMin
	sandboxRows    = int(sandboxLatSpan * kmPerDegreeLat / SandboxSpacingKm)
	sandboxColumns = int((sandboxLonMax - sandboxLonMin) * sandboxKmPerDegreeLon / SandboxSpacingKm)
)

// SandboxCount is the number of cells.
func SandboxCount() uint64 { return uint64(sandboxRows * sandboxColumns) }

// Sandbox returns the centre of cell index (modulo SandboxCount) and the
// length of a degree of longitude there.
func Sandbox(index uint64) (lat, lon, kmPerDegreeLon float64) {
	cell := int(index % SandboxCount())
	row, column := cell/sandboxColumns, cell%sandboxColumns
	lat = sandboxLatMin + (float64(row)+0.5)*SandboxSpacingKm/kmPerDegreeLat
	lon = sandboxLonMin + (float64(column)+0.5)*SandboxSpacingKm/sandboxKmPerDegreeLon
	return round(lat, 1e6), round(lon, 1e6), kmPerDegreeLat * cosDegrees(lat)
}

// SandboxPoint draws a location within SandboxRadiusKm of a cell's centre
// (uniform in the square, 6 decimals), from random.
func SandboxPoint(index uint64, random uint64) (lat, lon float64) {
	centreLat, centreLon, kmPerDegreeLon := Sandbox(index)
	dy := (2*Unit(random, 0) - 1) * SandboxRadiusKm / math.Sqrt2
	dx := (2*Unit(random, 1) - 1) * SandboxRadiusKm / math.Sqrt2
	return round(centreLat+dy/kmPerDegreeLat, 1e6), round(centreLon+dx/kmPerDegreeLon, 1e6)
}

// SandboxPointFrom is SandboxPoint drawn from r: dataset.py's sandbox_point.
func SandboxPointFrom(index uint64, r Source) (lat, lon float64) {
	centreLat, centreLon, kmPerDegreeLon := Sandbox(index)
	dy := (2*r.Random() - 1) * SandboxRadiusKm / math.Sqrt2
	dx := (2*r.Random() - 1) * SandboxRadiusKm / math.Sqrt2
	return round(centreLat+dy/kmPerDegreeLat, 1e6), round(centreLon+dx/kmPerDegreeLon, 1e6)
}

// AddedHotelID is the ID of the n-th hotel a run adds; tag identifies the
// run (hexadecimal), so runs never collide with each other or with the
// seeded IDs (1..HotelCount).
func AddedHotelID(tag uint64, n int) string {
	return "a" + strconv.FormatUint(tag, 16) + "-" + strconv.Itoa(n)
}

var (
	starWeights = []int{8, 20, 35, 25, 12}
	starPrice   = []float64{0.45, 0.65, 1.0, 1.6, 2.8}
	starRooms   = [][2]int{{10, 60}, {30, 150}, {60, 300}, {100, 450}, {150, 800}}
	planWeights = []int{20, 30, 25, 15, 10}
	minPlanDays = 14
	maxPlanDays = 180
)

// describeHotel draws everything of a hotel after its location from the
// template's city: name, phone, address, description, rating, capacity and
// rate plans.
func describeHotel(r Source, hotel *Hotel, template *cityTemplate) {
	stars := hotel.Stars
	district := template.districts[r.Below(len(template.districts))]
	bare := strings.TrimPrefix(district, "the ")
	prefix := namePrefixes[r.Below(len(namePrefixes))]
	suffix := nameSuffixes[r.Below(len(nameSuffixes))]
	switch pick(r, []int{40, 35, 25}) {
	case 0:
		hotel.Name = prefix + " " + suffix
	case 1:
		hotel.Name = prefix + " " + bare + " " + suffix
	default:
		hotel.Name = "Hotel " + prefix + " " + template.name
	}
	hotel.Phone = fill(r, template.phone)
	hotel.Address = Address{
		StreetNumber: strconv.Itoa(1 + r.Below(400)),
		StreetName:   template.streets[r.Below(len(template.streets))],
		City:         template.name, State: template.region, Country: template.country,
		PostalCode: fill(r, template.postal),
	}
	adjectives := starAdjectives[stars-1]
	first := r.Below(len(amenities))
	second := (first + 1 + r.Below(len(amenities)-1)) % len(amenities)
	other := template.districts[r.Below(len(template.districts))]
	hotel.Description = fmt.Sprintf(
		"A %d-minute walk from %s, this %s %s in %s is %d minutes from %s and offers %s and %s.",
		2+r.Below(14), transit[r.Below(len(transit))], adjectives[r.Below(len(adjectives))],
		kinds[r.Below(len(kinds))], district, 5+r.Below(26), other, amenities[first], amenities[second],
	)

	// 1 + 4 Beta(5, 2): the fifth smallest of six uniforms is Beta(5, 2).
	var uniforms [6]float64
	for n := range uniforms {
		uniforms[n] = r.Random()
	}
	sortFloats(uniforms[:])
	hotel.Rating = round(1+4*uniforms[4], 100)

	rooms := starRooms[stars-1]
	hotel.Capacity = rooms[0] + int(float64(rooms[1]-rooms[0])*(r.Random()+r.Random())/2)

	base := template.basePrice * starPrice[stars-1] * (0.75 + 0.25*(r.Random()+r.Random()))
	plans := 1 + pick(r, planWeights)
	codes := make([]int, len(ratePlanCodes)-1)
	for n := range codes {
		codes[n] = n + 1
	}
	hotel.Price = math.MaxFloat64
	for n := 0; n < plans; n++ {
		code, start, days := 0, 0, RateWindowDays
		if n > 0 {
			pick := n - 1 + r.Below(len(codes)-(n-1))
			codes[n-1], codes[pick] = codes[pick], codes[n-1]
			code = codes[n-1]
			days = minPlanDays + r.Below(maxPlanDays-minPlanDays+1)
			start = r.Below(RateWindowDays - days + 1)
		}
		room := roomTypes[r.Below(len(roomTypes))]
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
}

func sortFloats(values []float64) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
