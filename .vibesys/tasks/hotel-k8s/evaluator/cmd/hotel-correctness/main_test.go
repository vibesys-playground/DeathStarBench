package main

import (
	"strings"
	"testing"
)

func TestSplitRestartFlag(t *testing.T) {
	for _, args := range [][]string{
		{"--mode", "accuracy", "--restart-command-json", `["podctl","--deployments","geo","restart"]`, "--seed", "5"},
		{"--mode", "accuracy", `--restart-command-json=["podctl","--deployments","geo","restart"]`, "--seed", "5"},
	} {
		rest, command, err := splitRestartFlag(args)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(rest, " ") != "--mode accuracy --seed 5" || strings.Join(command, " ") != "podctl --deployments geo restart" {
			t.Fatalf("%v: rest %v, command %v", args, rest, command)
		}
	}
	rest, command, err := splitRestartFlag([]string{"--seed", "random"})
	if err != nil || command != nil || len(rest) != 2 {
		t.Fatalf("without the flag: %v %v %v", rest, command, err)
	}
	for _, args := range [][]string{{"--restart-command-json"}, {"--restart-command-json", "podctl"}} {
		if _, _, err := splitRestartFlag(args); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}
