package hotel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"math/rand"
	"net/http"
	"reflect"
	"testing"

	hotelsupport "vibesys/hotel-k8s-evaluator/internal/appsupport/hotel"
	"vibesys/microservice-evaluator/api"
)

// goldenDigest is the digest goldenStream computes for the Hotel benchmark
// adapter at vibesys commit de037943c (resources/evaluators/microservice/
// apps/hotel), from which this package was copied. The task-local adapter
// must reproduce it: same operation plans, requests and validation decisions
// for the same seeds (2000 samples per operation in each of two trials).
const goldenDigest = "3d7c9d775c97b43c85cc397d0381f564abdb5589e21b3dd9edf958048bd90c62"

const goldenSamplesPerOperation = 2000

var goldenOperations = []string{
	operationSearch, operationRecommendDistance, operationRecommendRate, operationRecommendPrice,
	operationLoginValid, operationLoginInvalid, operationReserveCapacity,
}

var goldenTrials = []api.TrialContext{
	{Index: 0, Seed: 48, FixtureSeed: 48},
	{Index: 1, Seed: 48, FixtureSeed: 49},
}

func TestGoldenOperationStream(t *testing.T) {
	application, err := New(hotelWorkload(goldenOperations...))
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := goldenStream(t, []api.Application{application}, goldenSamplesPerOperation)
	if digest != goldenDigest {
		t.Fatalf("operation stream digest = %s, want %s", digest, goldenDigest)
	}
}

// goldenStream drives every application through the same trials and samples,
// requires identical plans and validation decisions across them, and returns
// a digest of the stream plus per-variant success counts.
func goldenStream(t *testing.T, applications []api.Application, samples int) (string, map[string]int) {
	t.Helper()
	sum := sha256.New()
	successes := make(map[string]int)
	for _, trial := range goldenTrials {
		prepared := make([]any, len(applications))
		for index, application := range applications {
			value, err := application.Prepare(context.Background(), nil, trial)
			if err != nil {
				t.Fatal(err)
			}
			prepared[index] = value
		}
		runtime := &handlerRuntime{handler: newFakeHotel(t)}
		for operationIndex, name := range goldenOperations {
			operation := api.Operation{Name: name, Target: hotelsupport.GatewayTarget}
			random := rand.New(rand.NewSource(int64(trial.FixtureSeed)*1000 + int64(operationIndex)))
			for counter := 0; counter < samples; counter++ {
				sample := api.Sample{Counter: int64(counter), Random: random.Uint64()}
				goldenSample(t, sum, successes, applications, prepared, runtime, operation, sample)
			}
		}
	}
	return hex.EncodeToString(sum.Sum(nil)), successes
}

func goldenSample(
	t *testing.T,
	sum hash.Hash,
	successes map[string]int,
	applications []api.Application,
	prepared []any,
	runtime *handlerRuntime,
	operation api.Operation,
	sample api.Sample,
) {
	t.Helper()
	plans := make([]api.OperationPlan, len(applications))
	for index, application := range applications {
		plan, err := application.BuildOperation(operation, sample, prepared[index])
		if err != nil {
			t.Fatalf("%s sample %+v: %v", operation.Name, sample, err)
		}
		plans[index] = plan
	}
	defer func() {
		for index, application := range applications {
			application.FinishOperation(plans[index])
		}
	}()
	encoded := encodePlan(t, plans[0])
	for index := 1; index < len(plans); index++ {
		if other := encodePlan(t, plans[index]); string(other) != string(encoded) {
			t.Fatalf("%s sample %+v: plan %d differs:\n%s\n%s", operation.Name, sample, index, encoded, other)
		}
	}
	fmt.Fprintf(sum, "plan %s %d %d %s\n", operation.Name, sample.Counter, sample.Random, encoded)

	results := make([]api.ProtocolResult, len(plans[0].Invocations))
	for index, invocation := range plans[0].Invocations {
		results[index] = runtime.Invoke(context.Background(), invocation)
	}
	random := rand.New(rand.NewSource(int64(sample.Random >> 1)))
	for _, variant := range resultVariants(results, random) {
		decisions := make([]api.ValidationResult, len(applications))
		for index, application := range applications {
			decisions[index] = application.ValidateOperation(operation, plans[index], variant.results)
		}
		for index := 1; index < len(decisions); index++ {
			if !reflect.DeepEqual(decisions[0], decisions[index]) {
				t.Fatalf("%s sample %+v variant %s: decision %d differs: %+v != %+v",
					operation.Name, sample, variant.name, index, decisions[0], decisions[index])
			}
		}
		if decisions[0].Success {
			successes[operation.Name+"/"+variant.name]++
		}
		fmt.Fprintf(sum, "validate %s %+v\n", variant.name, decisions[0])
	}
}

// encodePlan serializes the invocations and the comparable plan state (the
// release closure of a reservation plan is not comparable and is omitted).
func encodePlan(t *testing.T, plan api.OperationPlan) []byte {
	t.Helper()
	state := reflect.ValueOf(plan.State).Elem()
	expected := state.FieldByName("expectedIDs")
	ids := make([]string, expected.Len())
	for index := range ids {
		ids[index] = expected.Index(index).String()
	}
	encoded, err := json.Marshal(struct {
		Invocations []api.Invocation
		Kind        string
		ExpectedIDs []string
		Release     bool
	}{plan.Invocations, state.FieldByName("kind").String(), ids, !state.FieldByName("release").IsNil()})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

type resultVariant struct {
	name    string
	results []api.ProtocolResult
}

// resultVariants returns the fake service's answer and deterministic
// corruptions of it, so both accepting and rejecting decisions are compared.
func resultVariants(results []api.ProtocolResult, random *rand.Rand) []resultVariant {
	mutate := func(name string, change func(index int, result api.ProtocolResult) api.ProtocolResult) resultVariant {
		index := random.Intn(len(results))
		changed := append([]api.ProtocolResult(nil), results...)
		changed[index] = change(index, changed[index])
		return resultVariant{name: name, results: changed}
	}
	body := func(result api.ProtocolResult) []byte {
		response, _ := result.Payload.(api.HTTPResponse)
		return append([]byte(nil), response.Body...)
	}
	withBody := func(result api.ProtocolResult, value []byte) api.ProtocolResult {
		response, _ := result.Payload.(api.HTTPResponse)
		response.Body = value
		result.Payload = response
		return result
	}
	variants := []resultVariant{
		{name: "real", results: results},
		{name: "missing", results: results[:len(results)-1]},
		mutate("status", func(_ int, result api.ProtocolResult) api.ProtocolResult {
			response, _ := result.Payload.(api.HTTPResponse)
			response.StatusCode = http.StatusInternalServerError
			result.Payload = response
			return result
		}),
		mutate("transport", func(_ int, result api.ProtocolResult) api.ProtocolResult {
			return api.ProtocolResult{ErrorCategory: "transport", ErrorMessage: "connection reset"}
		}),
		mutate("flip", func(_ int, result api.ProtocolResult) api.ProtocolResult {
			value := body(result)
			if len(value) != 0 {
				value[random.Intn(len(value))] ^= byte(1 + random.Intn(255))
			}
			return withBody(result, value)
		}),
		mutate("truncate", func(_ int, result api.ProtocolResult) api.ProtocolResult {
			value := body(result)
			return withBody(result, value[:random.Intn(len(value)+1)])
		}),
		mutate("reencode", func(_ int, result api.ProtocolResult) api.ProtocolResult {
			var decoded any
			if err := json.Unmarshal(body(result), &decoded); err != nil {
				return result
			}
			dropFeature(decoded, random)
			value, _ := json.Marshal(decoded)
			return withBody(result, value)
		}),
	}
	if len(results) == 2 {
		variants = append(variants, resultVariant{name: "swap", results: []api.ProtocolResult{results[1], results[0]}})
	}
	return variants
}

// dropFeature removes one GeoJSON feature, or rewrites a message.
func dropFeature(decoded any, random *rand.Rand) {
	object, ok := decoded.(map[string]any)
	if !ok {
		return
	}
	if features, ok := object["features"].([]any); ok && len(features) != 0 {
		index := random.Intn(len(features))
		object["features"] = append(features[:index:index], features[index+1:]...)
		return
	}
	if message, ok := object["message"].(string); ok {
		object["message"] = message + " "
	}
}
