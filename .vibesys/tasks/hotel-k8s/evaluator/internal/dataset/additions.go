package dataset

import (
	"math"
	"strconv"
)

// Values that writes put into the application: the workload and the oracle
// change profiles and prices of seeded hotels and add new hotels through the
// frontend's admin routes. Seeded data (Generate) does not depend on them.

// NewPhone draws a phone number in the format of the hotel's city.
func NewPhone(hotel *Hotel, random uint64) string {
	return fill(&rng{state: random}, templateOf(hotel).phone)
}

// NewName draws a hotel name as the generator does for the hotel's city.
func NewName(hotel *Hotel, random uint64) string {
	r := &rng{state: random}
	template := templateOf(hotel)
	prefix := namePrefixes[r.intn(len(namePrefixes))]
	suffix := nameSuffixes[r.intn(len(nameSuffixes))]
	district := template.districts[r.intn(len(template.districts))]
	if r.intn(2) == 0 {
		return prefix + " " + suffix
	}
	return prefix + " " + trimThe(district) + " " + suffix
}

// AddedHotel generates a hotel to add under id at (lat, lon), from random:
// a profile in the additions' fictional city, a star class, a rating, a
// capacity and 1-5 rate plans over the rate window, as Generate draws them.
// Its City is -1 and its Popularity 0.
func AddedHotel(id string, random uint64, lat, lon float64) Hotel {
	r := &rng{state: Mix(random, 0)}
	hotel := Hotel{ID: id, City: -1, Stars: 1 + r.pick(starWeights), Lat: lat, Lon: lon}
	describeHotel(r, &hotel, &additionCity)
	return hotel
}

func templateOf(hotel *Hotel) *cityTemplate {
	if hotel.City < 0 || hotel.City >= len(cityTable) {
		return &additionCity
	}
	return &cityTable[hotel.City]
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

// AddedHotelID is the ID of the n-th hotel a run adds; tag identifies the
// run (hexadecimal), so runs never collide with each other or with the
// seeded IDs (1..HotelCount).
func AddedHotelID(tag uint64, n int) string {
	return "a" + strconv.FormatUint(tag, 16) + "-" + strconv.Itoa(n)
}
