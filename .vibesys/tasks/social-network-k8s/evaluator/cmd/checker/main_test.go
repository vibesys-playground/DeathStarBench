package main

import (
	"net/http/httptest"
	"testing"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/fakeapp"
)

func TestCommandExitCodes(t *testing.T) {
	good := httptest.NewServer(fakeapp.New(dataset.Default(), fakeapp.Options{}))
	defer good.Close()
	stale := httptest.NewServer(fakeapp.New(dataset.Default(), fakeapp.Options{StaleCache: true}))
	defer stale.Close()
	common := []string{"--seed", "5", "--restart-command-json", `["true"]`, "--deployments", "nginx-thrift",
		"--caches", "user-memcached", "--freshness-ms", "100", "--replica-reads", "2"}
	if code := run(append([]string{"--base-url", good.URL}, common...)); code != 0 {
		t.Fatalf("correct app: exit %d", code)
	}
	if code := run(append([]string{"--base-url", stale.URL}, common...)); code != 1 {
		t.Fatalf("stale cache: exit %d, want 1", code)
	}
	if code := run([]string{"--base-url", "http://127.0.0.1:1", "--seed", "5"}); code != 2 {
		t.Fatalf("unreachable: exit %d, want 2", code)
	}
	if code := run(append([]string{"--base-url", good.URL}, "--restart-command-json", `["false"]`, "--deployments", "x", "--freshness-ms", "50", "--skip-dataset")); code != 1 {
		t.Fatalf("failing hook: exit %d, want 1", code)
	}
}
