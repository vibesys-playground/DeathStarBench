package hotel

import (
	"reflect"
	"testing"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
	"vibesys/microservice-evaluator/api"
)

func TestSeedInputsComeFromTheDataset(t *testing.T) {
	data := dataset.Default()
	for _, index := range []int{0, 12, len(data.Users) - 1, len(data.Users)} {
		username, password := User(index)
		want := data.Users[index%len(data.Users)]
		if username != want.Username || password != want.Password {
			t.Fatalf("User(%d) = (%q, %q), want %+v", index, username, password, want)
		}
		if !DefaultReference().ValidLogin(username, password) || DefaultReference().ValidLogin(username, password+"x") {
			t.Fatalf("ValidLogin disagrees for user %d", index)
		}
	}
	for _, hotel := range []dataset.Hotel{data.Hotels[0], data.Hotels[len(data.Hotels)-1]} {
		if got, err := Capacity(hotel.ID); err != nil || got != hotel.Capacity {
			t.Fatalf("Capacity(%s) = %d, %v; want %d", hotel.ID, got, err, hotel.Capacity)
		}
	}
	if _, err := Capacity("0"); err == nil {
		t.Fatal("Capacity accepted an unknown hotel")
	}
}

func TestValidateTopology(t *testing.T) {
	workload := api.Workload{
		Load:    api.Load{TimeoutSeconds: 2},
		Targets: []api.Target{{Name: GatewayTarget, Protocol: "http", SessionPolicy: "reuse"}},
	}
	if _, err := ValidateTopology(workload); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*api.Workload)
	}{
		{"missing", func(w *api.Workload) { w.Targets = nil }},
		{"protocol", func(w *api.Workload) { w.Targets[0].Protocol = "grpc" }},
		{"session", func(w *api.Workload) { w.Targets[0].SessionPolicy = "new_per_request" }},
		{"unknown config", func(w *api.Workload) { w.ApplicationConfig = map[string]any{"x": int64(1)} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := workload
			candidate.Targets = append([]api.Target(nil), workload.Targets...)
			test.mutate(&candidate)
			if _, err := ValidateTopology(candidate); err == nil {
				t.Fatal("malformed topology was accepted")
			}
		})
	}
}

func TestPreflightIsDeterministic(t *testing.T) {
	left := PreflightProbes()
	right := PreflightProbes()
	for index := range left {
		if left[index].Name != right[index].Name || !reflect.DeepEqual(left[index].Invocation, right[index].Invocation) {
			t.Fatalf("probe %d differs", index)
		}
	}
}
