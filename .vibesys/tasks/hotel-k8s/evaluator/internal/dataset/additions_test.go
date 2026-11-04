package dataset

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestSandboxesAreFarFromTheSeededHotels(t *testing.T) {
	if SandboxCount() < 30000 {
		t.Fatalf("only %d sandboxes", SandboxCount())
	}
	// The region's bounding box is more than 1,000 km (9 degrees of
	// latitude, or longitude at most latitudes) from every seeded hotel.
	for _, hotel := range Default().Hotels {
		inLat := hotel.Lat > sandboxLatMin-9 && hotel.Lat < sandboxLatMax+9
		inLon := hotel.Lon > sandboxLonMin-12 && hotel.Lon < sandboxLonMax+12
		if inLat && inLon {
			t.Fatalf("hotel %s at %v,%v is near the additions' region", hotel.ID, hotel.Lat, hotel.Lon)
		}
	}
	for _, index := range []uint64{0, 1, uint64(sandboxColumns), SandboxCount() - 1, SandboxCount() + 3} {
		lat, lon, _ := Sandbox(index)
		if lat < sandboxLatMin || lat > sandboxLatMax || lon < sandboxLonMin || lon > sandboxLonMax {
			t.Fatalf("sandbox %d centre %v,%v outside the region", index, lat, lon)
		}
	}
	// Neighbouring centres are at least SandboxSpacingKm apart.
	lat0, lon0, perDegree := Sandbox(0)
	lat1, lon1, _ := Sandbox(1)
	latN, _, _ := Sandbox(uint64(sandboxColumns))
	if dx := (lon1 - lon0) * perDegree; dx < SandboxSpacingKm*0.99 || lat1 != lat0 {
		t.Fatalf("columns %.2f km apart", dx)
	}
	if dy := (latN - lat0) * kmPerDegreeLat; dy < SandboxSpacingKm*0.99 {
		t.Fatalf("rows %.2f km apart", dy)
	}
	for random := uint64(0); random < 1000; random++ {
		lat, lon := SandboxPoint(7, Mix(random, 1))
		centreLat, centreLon, perDegree := Sandbox(7)
		if d := math.Hypot((lat-centreLat)*kmPerDegreeLat, (lon-centreLon)*perDegree); d > SandboxRadiusKm*1.001 {
			t.Fatalf("point %.2f km from the centre", d)
		}
	}
}

func TestAddedHotelsAndWriteValues(t *testing.T) {
	first := AddedHotel(AddedHotelID(0xabc, 1), 42, -40.1, -125.2)
	again := AddedHotel(AddedHotelID(0xabc, 1), 42, -40.1, -125.2)
	if first.Name != again.Name || first.Capacity != again.Capacity || len(first.RatePlans) != len(again.RatePlans) {
		t.Fatal("AddedHotel is not deterministic")
	}
	if first.ID != "aabc-1" || first.City != -1 || first.Lat != -40.1 || first.Address.City != "Isla Nueva" {
		t.Fatalf("hotel %+v", first)
	}
	if len(first.RatePlans) == 0 || first.RatePlans[0].Code != "RACK" || first.Capacity < 10 || first.Rating < 1 {
		t.Fatalf("hotel %+v", first)
	}
	if !strings.HasPrefix(first.Phone, "+56 32 ") {
		t.Fatalf("phone %q", first.Phone)
	}
	if _, err := strconv.Atoi(first.ID); err == nil {
		t.Fatal("an added ID looks like a seeded one")
	}
	seeded := &Default().Hotels[0]
	names := map[string]bool{}
	for random := uint64(0); random < 50; random++ {
		phone := NewPhone(seeded, random)
		if len(phone) != len(seeded.Phone) {
			t.Fatalf("phone %q does not have the city's format %q", phone, seeded.Phone)
		}
		names[NewName(seeded, random)] = true
	}
	if len(names) < 20 {
		t.Fatalf("only %d distinct names", len(names))
	}
}
