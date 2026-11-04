package hotel

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/api"
)

// validateSandbox checks a search or distance recommendation in a trial's
// sandbox. Only the trial's additions lie within reach there: those
// acknowledged more than StalenessBound before the read must be served, the
// ones in flight or acknowledged since may or may not be (every subset is
// allowed).
func (a *Application) validateSandbox(
	operation string,
	plan api.OperationPlan,
	result api.ProtocolResult,
	state *operationState,
	end time.Time,
	profiles profileSource,
) api.ValidationResult {
	query := plan.Invocations[0].Payload.(api.HTTPRequestSpec).Query
	required, optional := a.writes.additionsAt(state.sandbox, state.built, end)
	if operation == operationSearch {
		return validateFeatures(result, func(ids []string) error {
			return a.acceptSandboxSearch(query["lat"], query["lon"], required, optional, ids)
		}, profiles)
	}
	lat, latErr := strconv.ParseFloat(query["lat"], 64)
	lon, lonErr := strconv.ParseFloat(query["lon"], 64)
	if latErr != nil || lonErr != nil {
		return invalid("invalid_plan", fmt.Sprintf("sandbox location %v", query))
	}
	return validateFeatures(result, func(ids []string) error {
		return a.acceptSandboxNearest(lat, lon, required, optional, ids)
	}, profiles)
}

func (a *Application) acceptSandboxSearch(lat, lon string, required, optional []*dataset.Hotel, ids []string) error {
	if len(optional) > maxOptionalAdditions {
		return acceptSubset(ids, append(append([]*dataset.Hotel(nil), required...), optional...), 5)
	}
	var answers []string
	for mask := 0; mask < 1<<len(optional); mask++ {
		hotels := append([]*dataset.Hotel(nil), required...)
		for bit, hotel := range optional {
			if mask&(1<<bit) != 0 {
				hotels = append(hotels, hotel)
			}
		}
		candidates, exact, err := a.reference.SearchCandidatesWith(lat, lon, hotelsupport.NewAdditions(hotels...))
		if err != nil {
			return err
		}
		if !exact {
			// The geo service's ranking ties: the set is not determined.
			return acceptSubset(ids, hotels, 5)
		}
		sort.Strings(candidates)
		if reflect.DeepEqual(candidates, ids) || (len(candidates) == 0 && len(ids) == 0) {
			return nil
		}
		answers = append(answers, fmt.Sprint(candidates))
	}
	return fmt.Errorf("sandbox search answered %v, want one of %s", ids, strings.Join(answers, " "))
}

// acceptSandboxNearest accepts the hotels at the minimum distance over the
// seeded hotels, the required additions and any subset of the optional ones.
func (a *Application) acceptSandboxNearest(lat, lon float64, required, optional []*dataset.Hotel, ids []string) error {
	if len(ids) == 0 {
		return fmt.Errorf("distance recommendation answered no hotel")
	}
	seeded, seededDistance := a.reference.NearestHotelsDistance(lat, lon)
	distances := map[string]float64{}
	mandatory := map[string]bool{}
	for _, id := range seeded {
		distances[id], mandatory[id] = seededDistance, true
	}
	for _, hotel := range required {
		distances[hotel.ID], mandatory[hotel.ID] = hotelsupport.RecommendDistanceKm(lat, lon, hotel), true
	}
	for _, hotel := range optional {
		distances[hotel.ID] = hotelsupport.RecommendDistanceKm(lat, lon, hotel)
	}
	answered := map[string]bool{}
	distance, known := distances[ids[0]]
	for _, id := range ids {
		d, ok := distances[id]
		if !ok || d != distance || !known {
			return fmt.Errorf("distance recommendation answered %v, not hotels at one minimal distance", ids)
		}
		answered[id] = true
	}
	for id, d := range distances {
		if mandatory[id] && (d < distance || (d == distance && !answered[id])) {
			return fmt.Errorf("distance recommendation answered %v at %.6f km, but hotel %s is at %.6f km", ids, distance, id, d)
		}
	}
	return nil
}

func acceptSubset(ids []string, hotels []*dataset.Hotel, limit int) error {
	allowed := map[string]bool{}
	for _, hotel := range hotels {
		allowed[hotel.ID] = true
	}
	if len(ids) > limit {
		return fmt.Errorf("answered %d hotels, at most %d", len(ids), limit)
	}
	for _, id := range ids {
		if !allowed[id] {
			return fmt.Errorf("answered hotel %s, not among %v", id, allowed)
		}
	}
	return nil
}

// validateRates checks GET /rates of a seeded hotel: every seeded plan once,
// with its seeded dates and room, and prices the writes allow.
func (a *Application) validateRates(result api.ProtocolResult, hotel *dataset.Hotel, start, end time.Time) api.ValidationResult {
	value, validation := decodeHTTPJSON(result)
	if !validation.Success {
		return validation
	}
	root, ok := value.(map[string]any)
	if !ok {
		return invalid("response_schema", fmt.Sprintf("rates response must be an object, got %T", value))
	}
	if keys := sortedKeys(root); !reflect.DeepEqual(keys, []string{"hotelId", "ratePlans"}) {
		return invalid("response_schema", fmt.Sprintf("rates response fields %v, want [hotelId ratePlans]", keys))
	}
	if root["hotelId"] != hotel.ID {
		return invalid("response_value", fmt.Sprintf("rates of hotel %v, want %s", root["hotelId"], hotel.ID))
	}
	plans, ok := root["ratePlans"].([]any)
	if !ok {
		return invalid("response_schema", "ratePlans must be an array")
	}
	if len(plans) != len(hotel.RatePlans) {
		return invalid("response_value", fmt.Sprintf("hotel %s has %d rate plans, want %d", hotel.ID, len(plans), len(hotel.RatePlans)))
	}
	seen := map[string]bool{}
	for index, raw := range plans {
		code, prices, err := decodeRatePlan(raw, hotel)
		if err != nil {
			return invalid("response_value", fmt.Sprintf("rate plan %d of hotel %s: %v", index, hotel.ID, err))
		}
		if seen[code.Code] {
			return invalid("response_value", fmt.Sprintf("hotel %s lists rate plan %s twice", hotel.ID, code.Code))
		}
		seen[code.Code] = true
		allowed := a.writes.allowedPrices(hotel, code, start, end)
		matched := false
		for _, candidate := range allowed {
			matched = matched || candidate == prices
		}
		if !matched {
			return invalid("response_value", fmt.Sprintf(
				"rate plan %s of hotel %s has prices %+v, none of the allowed %+v", code.Code, hotel.ID, prices, allowed))
		}
	}
	return api.ValidationResult{Success: true}
}

// decodeRatePlan checks one plan of a /rates response against the hotel's
// seeded plan with its code, and returns that plan and the response's prices.
func decodeRatePlan(raw any, hotel *dataset.Hotel) (*dataset.RatePlan, hotelsupport.Prices, error) {
	object, ok := raw.(map[string]any)
	if !ok {
		return nil, hotelsupport.Prices{}, fmt.Errorf("must be an object")
	}
	if keys := sortedKeys(object); !reflect.DeepEqual(keys, []string{"code", "inDate", "outDate", "roomType"}) {
		return nil, hotelsupport.Prices{}, fmt.Errorf("fields %v, want [code inDate outDate roomType]", keys)
	}
	var plan *dataset.RatePlan
	for index := range hotel.RatePlans {
		if hotel.RatePlans[index].Code == object["code"] {
			plan = &hotel.RatePlans[index]
		}
	}
	if plan == nil {
		return nil, hotelsupport.Prices{}, fmt.Errorf("unknown code %v", object["code"])
	}
	if object["inDate"] != plan.InDate || object["outDate"] != plan.OutDate {
		return nil, hotelsupport.Prices{}, fmt.Errorf("dates %v-%v, want %s-%s", object["inDate"], object["outDate"], plan.InDate, plan.OutDate)
	}
	room, ok := object["roomType"].(map[string]any)
	if !ok {
		return nil, hotelsupport.Prices{}, fmt.Errorf("roomType must be an object")
	}
	if keys := sortedKeys(room); !reflect.DeepEqual(keys, []string{"bookableRate", "code", "roomDescription", "totalRate", "totalRateInclusive"}) {
		return nil, hotelsupport.Prices{}, fmt.Errorf("roomType fields %v", keys)
	}
	if room["code"] != plan.RoomCode || room["roomDescription"] != plan.RoomDescription {
		return nil, hotelsupport.Prices{}, fmt.Errorf("room %v %v, want %s %s", room["code"], room["roomDescription"], plan.RoomCode, plan.RoomDescription)
	}
	var amounts [3]float64
	for index, key := range []string{"bookableRate", "totalRate", "totalRateInclusive"} {
		amount, ok := room[key].(float64)
		if !ok {
			return nil, hotelsupport.Prices{}, fmt.Errorf("%s must be a number", key)
		}
		amounts[index] = amount
	}
	return plan, hotelsupport.Prices{Bookable: amounts[0], Total: amounts[1], Inclusive: amounts[2]}, nil
}
