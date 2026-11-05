package dataset

import (
	"math"
	"runtime"
	"sync"
)

// Generate builds the dataset of spec (about 3 s per million hotels on one core; the
// per-hotel part runs on every core). Every process of a sample builds the same one.
func Generate(spec Spec) (*Dataset, error) {
	if err := spec.Check(); err != nil {
		return nil, err
	}
	seed := spec.Seed
	counts := zipfCounts(spec.Hotels, len(cityTable))
	density := math.Sqrt(float64(spec.Hotels) / densityHotels)
	if density < 1 {
		density = 1
	}
	spreads := make([]float64, len(cityTable))
	kmPerLon := make([]float64, len(cityTable))
	for city, count := range counts {
		spreads[city] = (1.5 + 2.5*math.Sqrt(float64(count)/float64(counts[0]))) * density
		kmPerLon[city] = kmPerDegreeLat * cosDegrees(cityTable[city].lat)
	}
	assignment := make([]int16, 0, spec.Hotels)
	for city, count := range counts {
		for range count {
			assignment = append(assignment, int16(city))
		}
	}
	shuffle(newRNG(seed, "assignment", 0), len(assignment), func(i, j int) {
		assignment[i], assignment[j] = assignment[j], assignment[i]
	})

	d := &Dataset{Spec: spec, Seed: seed, hotels: make([]core, spec.Hotels)}
	// Locations in index order: a point another hotel took is drawn again (rare at 6
	// decimals), so this pass is sequential.
	taken := make(map[[2]float64]struct{}, spec.Hotels)
	for index, city := range assignment {
		r := newRNG(seed, "location", index)
		template := &cityTable[city]
		spread := spreads[city]
		if r.float() < outskirtsShare {
			spread *= outskirtsSpread
		}
		for {
			lat := round(template.lat+spread*r.normal()/kmPerDegreeLat, 1e6)
			lon := round(template.lon+spread*r.normal()/kmPerLon[city], 1e6)
			if _, ok := taken[[2]float64{lat, lon}]; !ok {
				taken[[2]float64{lat, lon}] = struct{}{}
				d.hotels[index] = core{lat: lat, lon: lon, city: city}
				break
			}
		}
	}
	taken = nil
	// Everything else of a hotel comes from its own stream.
	parallel(spec.Hotels, func(index int) {
		hotel := d.Hotel(index)
		c := &d.hotels[index]
		c.rating, c.price, c.capacity = hotel.Rating, hotel.Price, int32(hotel.Capacity)
		c.stars, c.plans = int8(hotel.Stars), int8(len(hotel.RatePlans))
	})
	members := make([][]int, len(cityTable))
	for city, count := range counts {
		members[city] = make([]int, 0, count)
	}
	for index, city := range assignment {
		members[city] = append(members[city], index)
	}
	for city, template := range cityTable {
		order := members[city]
		shuffle(newRNG(seed, "popularity", city), len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		for rank, index := range order {
			d.hotels[index].popularity = int32(rank + 1)
		}
		d.Cities = append(d.Cities, City{
			Name: template.name, Region: template.region, Country: template.country,
			Lat: template.lat, Lon: template.lon, PhoneFormat: template.phone, Districts: template.districts,
			SpreadKm: spreads[city], KmPerDegreeLon: kmPerLon[city], Hotels: order,
		})
	}
	d.Users = generateUsers(newRNG(seed, "users", 0), spec.Users)
	return d, nil
}

// parallel runs body(0..n-1) on every core; body must touch only its own index's state.
func parallel(n int, body func(int)) {
	workers := runtime.GOMAXPROCS(0)
	chunk := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for start := 0; start < n; start += chunk {
		end := min(start+chunk, n)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := start; index < end; index++ {
				body(index)
			}
		}()
	}
	wg.Wait()
}

func generateUsers(r *rng, count int) []User {
	users := make([]User, 0, count)
	seen := make(map[string]struct{}, count)
	password := make([]byte, 0, 16)
	for len(users) < count {
		name := firstNames[r.intn(len(firstNames))] + "." + lastNames[r.intn(len(lastNames))]
		if r.float() < usernameDigits {
			name += itoa(r.intn(1000))
		}
		for {
			if _, ok := seen[name]; !ok {
				break
			}
			name += itoa(r.intn(10))
		}
		seen[name] = struct{}{}
		length := 10 + r.intn(7)
		password = password[:0]
		for range length {
			password = append(password, passwordAlphabet[r.intn(len(passwordAlphabet))])
		}
		users = append(users, User{Username: name, Password: string(password)})
	}
	return users
}

func itoa(n int) string {
	var buf [20]byte
	i := len(buf)
	for {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
		if n == 0 {
			return string(buf[i:])
		}
	}
}

// zipfCounts splits total over n ranks in proportion to 1/rank, by largest remainder (lower
// rank first on ties).
func zipfCounts(total, n int) []int {
	weights := make([]float64, n)
	norm := 0.0
	for k := range weights {
		weights[k] = 1 / float64(k+1)
		norm += weights[k]
	}
	counts := make([]int, n)
	remainders := make([]float64, n)
	sum := 0
	for k, weight := range weights {
		share := float64(total) * (weight / norm)
		counts[k] = int(share)
		remainders[k] = share - float64(counts[k])
		sum += counts[k]
	}
	for range total - sum {
		best := 0
		for k, remainder := range remainders {
			if remainder > remainders[best] {
				best = k
			}
		}
		counts[best]++
		remainders[best] = -1
	}
	return counts
}

// ------------------------------------------------------------------------- bookings

// The rooms already booked on each night of the rate window: the hotel's own occupancy,
// higher in summer and at weekends, lower the further ahead the night lies, with noise;
// never the whole hotel (at most Capacity-1 rooms), so every seeded hotel has a room on every
// night and a capacity write down to SeededMaxRooms+1 keeps it so.
const (
	occupancyLow, occupancyHigh = 0.30, 0.85 // the hotel's own occupancy, uniform between
	seasonAmplitude             = 0.15       // +-15% over the year, peaking in mid-July
	leadDecline                 = 0.55       // the last night of the window holds 45% of the first's
	occupancyNoise              = 0.10       // standard deviation, as a share of the capacity
)

// weekdayFactor[weekday of the night] (Sunday first): Friday and Saturday nights fill most.
var weekdayFactor = [7]float64{0.85, 0.90, 0.95, 1.0, 1.05, 1.20, 1.20}

// seasonFactor[n] for night n of the window.
var seasonFactor = func() []float64 {
	factors := make([]float64, RateWindowDays)
	for n := range factors {
		day := float64(RateWindowStart.AddDate(0, 0, n).YearDay())
		factors[n] = 1 + seasonAmplitude*cosDegrees(360*(day-196)/365)
	}
	return factors
}()

// Occupancy is the rooms booked on each night of the rate window at hotel index i, as the
// seed writes them (reservation-db "nights"; a night with none has no document).
func (d *Dataset) Occupancy(index int) []int16 {
	return occupancy(newRNG(d.Seed, "occupancy", index), int(d.hotels[index].capacity))
}

func occupancy(r *rng, capacity int) []int16 {
	nights := make([]int16, RateWindowDays)
	own := occupancyLow + (occupancyHigh-occupancyLow)*r.float()
	weekday := int(RateWindowStart.Weekday())
	for n := range nights {
		lead := 1 - leadDecline*float64(n)/float64(RateWindowDays-1)
		share := own*seasonFactor[n]*weekdayFactor[(weekday+n)%7]*lead + occupancyNoise*r.normal()
		rooms := int(math.Round(share * float64(capacity)))
		nights[n] = int16(max(0, min(rooms, capacity-1)))
	}
	return nights
}

// SeededMaxRooms is the most rooms booked on one night of hotel index i's seeded calendar.
func (d *Dataset) SeededMaxRooms(index int) int {
	most := 0
	for _, rooms := range d.Occupancy(index) {
		most = max(most, int(rooms))
	}
	return most
}
