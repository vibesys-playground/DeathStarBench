package hotel

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"sort"
	"time"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/api"
)

const (
	loginSuccess       = "Login successfully!"
	loginFailure       = "Failed. Please check your username and password. "
	reservationSuccess = "Reserve successfully!"
	reservationFailure = "Failed. Already reserved. "
)

func (a *Application) ValidateOperation(
	operation api.Operation,
	plan api.OperationPlan,
	results []api.ProtocolResult,
) api.ValidationResult {
	state, ok := plan.State.(*operationState)
	if !ok || state == nil {
		return invalid("invalid_plan", fmt.Sprintf("unexpected Hotel plan state %T", plan.State))
	}
	if state.kind != operation.Name {
		return invalid("invalid_plan", fmt.Sprintf("plan kind %q does not match operation %q", state.kind, operation.Name))
	}
	if len(results) != len(plan.Invocations) {
		return invalid("result_count", fmt.Sprintf("got %d protocol results, want %d", len(results), len(plan.Invocations)))
	}
	end := time.Now()

	switch operation.Name {
	case operationSearch, operationRecommendDistance, operationRecommendRate, operationRecommendPrice:
		profiles := a.profilesAt(state.built, end, state.sandbox)
		if state.sandbox != nil {
			return a.validateSandbox(operation.Name, plan, results[0], state, end, profiles)
		}
		return validateFeatures(results[0], exactSet(state.expectedIDs), profiles)
	case operationRates:
		return a.validateRates(results[0], state.hotel, state.built, end)
	case operationUpdateRate, operationUpdateProfile, operationSetCapacity, operationAddHotel:
		validation := api.ValidationResult{Success: true}
		for index, message := range state.messages {
			if validation = validateMessage(results[index], message); !validation.Success {
				validation.ErrorMessage = fmt.Sprintf("step %d: %s", index+1, validation.ErrorMessage)
				break
			}
		}
		if state.finish != nil {
			// The write was acknowledged if its first step answered 200
			// with the expected message.
			state.finish(end, validateMessage(results[0], state.messages[0]).Success)
			state.finish = nil
		}
		return validation
	case operationLoginValid:
		return validateMessage(results[0], loginSuccess)
	case operationLoginInvalid:
		return validateMessage(results[0], loginFailure)
	case operationReserveCapacity:
		if validation := validateMessage(results[0], reservationSuccess); !validation.Success {
			validation.ErrorMessage = "capacity-fill step: " + validation.ErrorMessage
			return validation
		}
		validation := validateMessage(results[1], reservationFailure)
		if !validation.Success {
			validation.ErrorMessage = "over-capacity rejection step: " + validation.ErrorMessage
		}
		return validation
	default:
		return invalid("invalid_plan", fmt.Sprintf("unknown Hotel operation %q", operation.Name))
	}
}

func validateMessage(result api.ProtocolResult, expected string) api.ValidationResult {
	value, validation := decodeHTTPJSON(result)
	if !validation.Success {
		return validation
	}
	object, ok := value.(map[string]any)
	if !ok {
		return invalid("response_schema", fmt.Sprintf("message response must be an object, got %T", value))
	}
	if keys := sortedKeys(object); !reflect.DeepEqual(keys, []string{"message"}) {
		return invalid("response_schema", fmt.Sprintf("message response fields %v, want [message]", keys))
	}
	message, ok := object["message"].(string)
	if !ok {
		return invalid("response_schema", "message must be a string")
	}
	if message != expected {
		return invalid("response_value", fmt.Sprintf("message = %q, want %q", message, expected))
	}
	return api.ValidationResult{Success: true}
}

// validateGeoJSON checks a GeoJSON answer of seeded hotels that no write
// changed: exactly expectedIDs, with their seeded profiles.
func validateGeoJSON(result api.ProtocolResult, expectedIDs []string) api.ValidationResult {
	return validateFeatures(result, exactSet(expectedIDs), seededProfiles)
}

// profileSource returns the hotel with an ID and the profiles a response may
// show for it.
type profileSource func(id string) (*dataset.Hotel, []hotelsupport.Profile, bool)

func seededProfiles(id string) (*dataset.Hotel, []hotelsupport.Profile, bool) {
	hotel, ok := hotelsupport.DefaultReference().Hotel(id)
	if !ok {
		return nil, nil, false
	}
	return hotel, []hotelsupport.Profile{{Name: hotel.Name, Phone: hotel.Phone}}, true
}

// profilesAt is the profile source for a read built at start and validated
// at end: seeded hotels with the profiles the writes allow, and the
// additions of the trial whose sandbox the read queries.
func (a *Application) profilesAt(start, end time.Time, sandbox *fixture) profileSource {
	reference := a.reference
	return func(id string) (*dataset.Hotel, []hotelsupport.Profile, bool) {
		if hotel, ok := reference.Hotel(id); ok {
			return hotel, a.writes.allowedProfiles(hotel, start, end), true
		}
		if sandbox != nil {
			required, optional := a.writes.additionsAt(sandbox, start, end)
			for _, hotel := range append(required, optional...) {
				if hotel.ID == id {
					return hotel, []hotelsupport.Profile{{Name: hotel.Name, Phone: hotel.Phone}}, true
				}
			}
		}
		return nil, nil, false
	}
}

// exactSet accepts exactly one set of hotel IDs.
func exactSet(expectedIDs []string) func([]string) error {
	want := append([]string(nil), expectedIDs...)
	sort.Strings(want)
	return func(actual []string) error {
		if len(actual) != len(want) {
			return fmt.Errorf("GeoJSON has %d features, want exactly %d", len(actual), len(want))
		}
		if !reflect.DeepEqual(actual, want) {
			return fmt.Errorf("hotel IDs %v, want exact set %v", actual, want)
		}
		return nil
	}
}

// validateFeatures checks a GeoJSON FeatureCollection strictly: its schema,
// each feature's profile (one the source allows) and float32 coordinates,
// no duplicates, and the sorted set of IDs with accept.
func validateFeatures(result api.ProtocolResult, accept func(sortedIDs []string) error, profiles profileSource) api.ValidationResult {
	value, validation := decodeHTTPJSON(result)
	if !validation.Success {
		return validation
	}
	root, ok := value.(map[string]any)
	if !ok {
		return invalid("response_schema", fmt.Sprintf("GeoJSON root must be an object, got %T", value))
	}
	if keys := sortedKeys(root); !reflect.DeepEqual(keys, []string{"features", "type"}) {
		return invalid("response_schema", fmt.Sprintf("GeoJSON root fields %v, want [features type]", keys))
	}
	if root["type"] != "FeatureCollection" {
		return invalid("response_schema", fmt.Sprintf("GeoJSON type = %v, want FeatureCollection", root["type"]))
	}
	features, ok := root["features"].([]any)
	if !ok {
		return invalid("response_schema", "GeoJSON features must be an array")
	}
	seen := make(map[string]struct{}, len(features))
	actualIDs := make([]string, 0, len(features))
	for index, raw := range features {
		id, err := validateFeature(raw, profiles)
		if err != nil {
			return invalid("response_schema", fmt.Sprintf("feature %d: %v", index, err))
		}
		if _, duplicate := seen[id]; duplicate {
			return invalid("response_value", fmt.Sprintf("GeoJSON contains duplicate hotel ID %q", id))
		}
		seen[id] = struct{}{}
		actualIDs = append(actualIDs, id)
	}
	sort.Strings(actualIDs)
	if err := accept(actualIDs); err != nil {
		return invalid("response_value", err.Error())
	}
	return api.ValidationResult{Success: true}
}

func validateFeature(raw any, profiles profileSource) (string, error) {
	feature, ok := raw.(map[string]any)
	if !ok {
		return "", fmt.Errorf("must be an object, got %T", raw)
	}
	if keys := sortedKeys(feature); !reflect.DeepEqual(keys, []string{"geometry", "id", "properties", "type"}) {
		return "", fmt.Errorf("fields %v, want [geometry id properties type]", keys)
	}
	if feature["type"] != "Feature" {
		return "", fmt.Errorf("type = %v, want Feature", feature["type"])
	}
	id, ok := feature["id"].(string)
	if !ok {
		return "", fmt.Errorf("id must be a string")
	}
	expected, allowed, ok := profiles(id)
	if !ok {
		return "", fmt.Errorf("unknown Hotel profile ID %q", id)
	}
	properties, ok := feature["properties"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("properties must be an object")
	}
	if keys := sortedKeys(properties); !reflect.DeepEqual(keys, []string{"name", "phone_number"}) {
		return "", fmt.Errorf("property fields %v, want [name phone_number]", keys)
	}
	matched := false
	for _, profile := range allowed {
		matched = matched || (properties["name"] == profile.Name && properties["phone_number"] == profile.Phone)
	}
	if !matched {
		return "", fmt.Errorf("properties %v of hotel %s are none of the allowed profiles %v", properties, id, allowed)
	}
	geometry, ok := feature["geometry"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("geometry must be an object")
	}
	if keys := sortedKeys(geometry); !reflect.DeepEqual(keys, []string{"coordinates", "type"}) {
		return "", fmt.Errorf("geometry fields %v, want [coordinates type]", keys)
	}
	if geometry["type"] != "Point" {
		return "", fmt.Errorf("geometry type = %v, want Point", geometry["type"])
	}
	coordinates, ok := geometry["coordinates"].([]any)
	if !ok || len(coordinates) != 2 {
		return "", fmt.Errorf("coordinates must be a two-element array")
	}
	lon, lonOK := coordinates[0].(float64)
	lat, latOK := coordinates[1].(float64)
	if !lonOK || !latOK || math.IsNaN(lon) || math.IsInf(lon, 0) || math.IsNaN(lat) || math.IsInf(lat, 0) {
		return "", fmt.Errorf("coordinates must contain finite numbers")
	}
	wantLon, wantLat := hotelsupport.EncodedCoordinates(expected)
	if lon != wantLon || lat != wantLat {
		return "", fmt.Errorf("coordinates [%v %v], want catalog [%v %v]", lon, lat, wantLon, wantLat)
	}
	return id, nil
}

func decodeHTTPJSON(result api.ProtocolResult) (any, api.ValidationResult) {
	if !result.TransportSuccess {
		return nil, invalid(result.ErrorCategory, result.ErrorMessage)
	}
	response, ok := result.Payload.(api.HTTPResponse)
	if !ok {
		return nil, invalid("invalid_response", fmt.Sprintf("expected HTTP response, got %T", result.Payload))
	}
	if response.StatusCode != http.StatusOK {
		return nil, invalid("http_status", fmt.Sprintf("HTTP %d, want %d", response.StatusCode, http.StatusOK))
	}
	var value any
	if err := json.Unmarshal(response.Body, &value); err != nil {
		return nil, invalid("response_json", err.Error())
	}
	return value, api.ValidationResult{Success: true}
}

func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func invalid(category, message string) api.ValidationResult {
	return api.ValidationResult{ErrorCategory: category, ErrorMessage: message}
}
