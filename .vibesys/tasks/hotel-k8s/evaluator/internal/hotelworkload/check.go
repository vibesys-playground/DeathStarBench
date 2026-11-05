package hotelworkload

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/workload"

	"vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// Checks run after the phase, in start order, for every write and 10% of the reads. Responses
// are checked strictly: exact JSON fields, values and the data or values the writes allow.

const (
	loginSuccess       = "Login successfully!"
	loginFailure       = "Failed. Please check your username and password. "
	reservationSuccess = "Reserve successfully!"
	reservationFailure = "Failed. Already reserved. "
)

// object is a JSON object whose fields are exactly want (sorted).
func object(value any, want []string, what string) (map[string]any, error) {
	fields, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object, got %s", what, jsonType(value))
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if !slices.Equal(keys, want) {
		return nil, fmt.Errorf("%s fields %q, want %q", what, keys, want)
	}
	return fields, nil
}

func jsonType(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case float64:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "an array"
	default:
		return fmt.Sprintf("%T", value)
	}
}

func body(call workload.Call, step int) (any, error) {
	var value any
	if err := json.Unmarshal(call.Responses[step].Body, &value); err != nil {
		return nil, fmt.Errorf("not JSON (%v)", err)
	}
	return value, nil
}

func message(call workload.Call, step int, expected string) error {
	value, err := body(call, step)
	if err != nil {
		return fmt.Errorf("step %d: %w", step+1, err)
	}
	fields, err := object(value, []string{"message"}, fmt.Sprintf("step %d response", step+1))
	if err != nil {
		return err
	}
	if fields["message"] != expected {
		return fmt.Errorf("step %d: message %#v, want %q", step+1, fields["message"], expected)
	}
	return nil
}

// checkMessage checks that step's response is exactly {"message": expected}.
func checkMessage(step int, expected string) func(*Model, workload.Call) error {
	return func(_ *Model, call workload.Call) error { return message(call, step, expected) }
}

// checkMessages checks a two-step request's messages.
func checkMessages(first, second string) func(*Model, workload.Call) error {
	return func(_ *Model, call workload.Call) error {
		if err := message(call, 0, first); err != nil {
			return err
		}
		return message(call, 1, second)
	}
}

// accept judges the sorted hotel IDs of an answer.
type accept func(ids []string) error

// checkFeatures checks a strict GeoJSON FeatureCollection: profiles the writes allow, float32
// coordinates, IDs that accept takes. added are the additions the answer may hold.
func (m *Model) checkFeatures(call workload.Call, judge accept, added []*dataset.Hotel) error {
	root, err := body(call, 0)
	if err != nil {
		return err
	}
	fields, err := object(root, []string{"features", "type"}, "GeoJSON root")
	if err != nil {
		return err
	}
	if fields["type"] != "FeatureCollection" {
		return fmt.Errorf("GeoJSON type %#v, want FeatureCollection", fields["type"])
	}
	features, ok := fields["features"].([]any)
	if !ok {
		return errors.New("GeoJSON features must be an array")
	}
	byID := make(map[string]*dataset.Hotel, len(added))
	for _, h := range added {
		byID[h.ID] = h
	}
	ids := make([]string, 0, len(features))
	seen := make(map[string]bool, len(features))
	for n, value := range features {
		feature, err := object(value, []string{"geometry", "id", "properties", "type"}, fmt.Sprintf("feature %d", n))
		if err != nil {
			return err
		}
		id, isString := feature["id"].(string)
		if feature["type"] != "Feature" || !isString {
			return fmt.Errorf("feature %d: type %#v and id %#v", n, feature["type"], feature["id"])
		}
		var h *dataset.Hotel
		var allowed []hotel.Profile
		if seeded, ok := m.ref.Hotel(id); ok {
			h = seeded
			allowed = m.profiles.Allowed(id, hotel.Profile{Name: h.Name, Phone: h.Phone}, call.Start, call.End)
		} else if addition, ok := byID[id]; ok {
			h = addition
			allowed = []hotel.Profile{{Name: h.Name, Phone: h.Phone}}
		} else {
			return fmt.Errorf("feature %d: unknown hotel %q", n, id)
		}
		properties, err := object(feature["properties"], []string{"name", "phone_number"}, fmt.Sprintf("feature %d properties", n))
		if err != nil {
			return err
		}
		name, nameOK := properties["name"].(string)
		phone, phoneOK := properties["phone_number"].(string)
		if !nameOK || !phoneOK || !slices.Contains(allowed, hotel.Profile{Name: name, Phone: phone}) {
			return fmt.Errorf("hotel %s: profile %v, allowed %v", id, properties, allowed)
		}
		geometry, err := object(feature["geometry"], []string{"coordinates", "type"}, fmt.Sprintf("feature %d geometry", n))
		if err != nil {
			return err
		}
		coordinates, ok := geometry["coordinates"].([]any)
		if geometry["type"] != "Point" || !ok || len(coordinates) != 2 {
			return fmt.Errorf("hotel %s: geometry %v", id, geometry)
		}
		lon, lat := hotel.EncodedCoordinates(h)
		if coordinates[0] != any(lon) || coordinates[1] != any(lat) {
			return fmt.Errorf("hotel %s: coordinates %v, want [%v %v]", id, coordinates, lon, lat)
		}
		if seen[id] {
			return fmt.Errorf("hotel %s twice", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return judge(ids)
}

func sorted(ids []string) []string {
	out := slices.Clone(ids)
	sort.Strings(out)
	if out == nil {
		out = []string{}
	}
	return out
}

func exactly(expected []string) accept {
	want := sorted(expected)
	return func(ids []string) error {
		if !slices.Equal(ids, want) {
			return fmt.Errorf("hotels %q, want exactly %q", ids, want)
		}
		return nil
	}
}

// additionsAt is (required, optional): the additions a read over the call's window must, or may, see.
func (m *Model) additionsAt(call workload.Call) (required, optional []*dataset.Hotel) {
	for _, h := range m.added {
		allowed := m.additions.Allowed(h.ID, false, call.Start, call.End)
		switch {
		case len(allowed) == 1 && allowed[0]:
			required = append(required, h)
		case slices.Contains(allowed, true):
			optional = append(optional, h)
		}
	}
	return required, optional
}

func subsetOf(hotels []*dataset.Hotel, limit int) accept {
	allowed := map[string]bool{}
	for _, h := range hotels {
		allowed[h.ID] = true
	}
	return func(ids []string) error {
		if len(ids) > limit {
			return fmt.Errorf("%d hotels, at most %d", len(ids), limit)
		}
		var stray []string
		for _, id := range ids {
			if !allowed[id] {
				stray = append(stray, id)
			}
		}
		if stray != nil {
			return fmt.Errorf("hotels %q not among %q", stray, sorted(idsOf(hotels)))
		}
		return nil
	}
}

func idsOf(hotels []*dataset.Hotel) []string {
	ids := make([]string, len(hotels))
	for i, h := range hotels {
		ids[i] = h.ID
	}
	return ids
}

// sandboxSearch accepts the answer for the required additions plus some subset of the optional ones.
func (m *Model) sandboxSearch(lat, lon string, required, optional []*dataset.Hotel) accept {
	if len(optional) > maxOptionalAdditions {
		return subsetOf(append(slices.Clone(required), optional...), geoMaxResults)
	}
	return func(ids []string) error {
		var answers [][]string
		for mask := range 1 << len(optional) {
			hotels := slices.Clone(required)
			for bit, h := range optional {
				if mask>>bit&1 == 1 {
					hotels = append(hotels, h)
				}
			}
			candidates, exact, err := m.ref.SearchCandidatesWith(lat, lon, hotel.NewAdditions(hotels...))
			if err != nil {
				return err
			}
			if !exact { // geo's ranking ties: the set is not determined
				return subsetOf(hotels, geoMaxResults)(ids)
			}
			answer := sorted(candidates)
			if slices.Equal(answer, ids) {
				return nil
			}
			answers = append(answers, answer)
		}
		return fmt.Errorf("sandbox search answered %q, want one of %q", ids, answers)
	}
}

// sandboxNearest accepts the hotels at one minimal distance over the seeded, required and some
// optional ones.
func (m *Model) sandboxNearest(lat, lon float64, required, optional []*dataset.Hotel) accept {
	seeded, seededDistance := m.ref.NearestHotelsDistance(lat, lon)
	distances := map[string]float64{}
	var order []string // the candidates in a fixed order, for the message
	mandatory := map[string]bool{}
	for _, id := range seeded {
		distances[id], mandatory[id] = seededDistance, true
		order = append(order, id)
	}
	for _, h := range required {
		distances[h.ID], mandatory[h.ID] = hotel.RecommendDistanceKm(lat, lon, h), true
		order = append(order, h.ID)
	}
	for _, h := range optional {
		distances[h.ID] = hotel.RecommendDistanceKm(lat, lon, h)
		order = append(order, h.ID)
	}
	return func(ids []string) error {
		if len(ids) == 0 {
			return errors.New("distance recommendation answered no hotel")
		}
		answered := map[string]bool{}
		for _, id := range ids {
			d, ok := distances[id]
			if !ok || d != distances[ids[0]] {
				return fmt.Errorf("distance recommendation answered %q, not hotels at one minimal distance", ids)
			}
			answered[id] = true
		}
		distance := distances[ids[0]]
		for _, id := range order {
			if d := distances[id]; mandatory[id] && (d < distance || (d == distance && !answered[id])) {
				return fmt.Errorf("distance recommendation answered %q at %.6f km, but %s is at %.6f km", ids, distance, id, d)
			}
		}
		return nil
	}
}

func (m *Model) checkSearch(call workload.Call) error {
	info, err := decode[searchInfo](call)
	if err != nil {
		return err
	}
	if info.Expected != nil {
		return m.checkFeatures(call, exactly(*info.Expected), nil)
	}
	required, optional := m.additionsAt(call)
	return m.checkFeatures(call, m.sandboxSearch(info.Near[0], info.Near[1], required, optional), append(required, optional...))
}

func checkRecommendation(require string) func(*Model, workload.Call) error {
	return func(m *Model, call workload.Call) error {
		switch require {
		case "rate":
			return m.checkFeatures(call, exactly(m.ref.BestRated()), nil)
		case "price":
			return m.checkFeatures(call, exactly(m.ref.Cheapest()), nil)
		}
		info, err := decode[locationInfo](call)
		if err != nil {
			return err
		}
		lat, err := strconv.ParseFloat(info.Lat, 64)
		if err != nil {
			return err
		}
		lon, err := strconv.ParseFloat(info.Lon, 64)
		if err != nil {
			return err
		}
		if !info.Sandbox {
			return m.checkFeatures(call, exactly(m.ref.NearestHotels(lat, lon)), nil)
		}
		required, optional := m.additionsAt(call)
		return m.checkFeatures(call, m.sandboxNearest(lat, lon, required, optional), append(required, optional...))
	}
}

// checkRates checks every seeded plan once, with its dates and room, and prices the writes allow.
func (m *Model) checkRates(call workload.Call) error {
	info, err := decode[hotelInfo](call)
	if err != nil {
		return err
	}
	h, ok := m.ref.Hotel(info.Hotel)
	if !ok {
		return fmt.Errorf("rates of unknown hotel %q", info.Hotel)
	}
	root, err := body(call, 0)
	if err != nil {
		return err
	}
	fields, err := object(root, []string{"hotelId", "ratePlans"}, "rates response")
	if err != nil {
		return err
	}
	plans, ok := fields["ratePlans"].([]any)
	if fields["hotelId"] != h.ID || !ok {
		return fmt.Errorf("rates of hotel %#v, want %s", fields["hotelId"], h.ID)
	}
	if len(plans) != len(h.RatePlans) {
		return fmt.Errorf("hotel %s has %d rate plans, want %d", h.ID, len(plans), len(h.RatePlans))
	}
	byCode := make(map[string]*dataset.RatePlan, len(h.RatePlans))
	for i := range h.RatePlans {
		byCode[h.RatePlans[i].Code] = &h.RatePlans[i]
	}
	seen := map[string]bool{}
	for n, value := range plans {
		got, err := object(value, []string{"code", "inDate", "outDate", "roomType"}, fmt.Sprintf("rate plan %d", n))
		if err != nil {
			return err
		}
		code, _ := got["code"].(string)
		plan := byCode[code]
		if plan == nil || seen[code] {
			return fmt.Errorf("hotel %s: rate plan %#v unknown or listed twice", h.ID, got["code"])
		}
		seen[code] = true
		if got["inDate"] != plan.InDate || got["outDate"] != plan.OutDate {
			return fmt.Errorf("plan %s of hotel %s: dates %v-%v", code, h.ID, got["inDate"], got["outDate"])
		}
		room, err := object(got["roomType"], []string{"bookableRate", "code", "roomDescription", "totalRate", "totalRateInclusive"},
			fmt.Sprintf("plan %s roomType", code))
		if err != nil {
			return err
		}
		if room["code"] != plan.RoomCode || room["roomDescription"] != plan.RoomDescription {
			return fmt.Errorf("plan %s of hotel %s: room %v %#v", code, h.ID, room["code"], room["roomDescription"])
		}
		bookable, ok1 := room["bookableRate"].(float64)
		total, ok2 := room["totalRate"].(float64)
		inclusive, ok3 := room["totalRateInclusive"].(float64)
		if !ok1 || !ok2 || !ok3 {
			return fmt.Errorf("plan %s of hotel %s: prices %v %v %v are not numbers", code, h.ID,
				room["bookableRate"], room["totalRate"], room["totalRateInclusive"])
		}
		seededPrices := hotel.Prices{Bookable: plan.BookableRate, Total: plan.TotalRate, Inclusive: plan.TotalRateInclusive}
		allowed := m.prices.Allowed(hotel.PlanKey{HotelID: h.ID, Code: code}, seededPrices, call.Start, call.End)
		if got := (hotel.Prices{Bookable: bookable, Total: total, Inclusive: inclusive}); !slices.Contains(allowed, got) {
			return fmt.Errorf("plan %s of hotel %s: prices %v, allowed %v", code, h.ID, got, allowed)
		}
	}
	return nil
}
