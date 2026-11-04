package main

import (
	"reflect"
	"testing"
)

func TestStateFlagsAreRemoved(t *testing.T) {
	rest, in, out, err := stateFlags([]string{"--mode", "benchmark", "--state-in", "a.bin", "--workload", "w.toml", "--state-out=b.bin", "-rate", "5"})
	if err != nil || in != "a.bin" || out != "b.bin" || !reflect.DeepEqual(rest, []string{"--mode", "benchmark", "--workload", "w.toml", "-rate", "5"}) {
		t.Fatalf("stateFlags = %v %q %q %v", rest, in, out, err)
	}
	if _, _, _, err := stateFlags([]string{"--state-out"}); err == nil {
		t.Fatal("missing path accepted")
	}
}
