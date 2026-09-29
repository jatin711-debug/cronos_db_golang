package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestStartupRejectsInvalidPolicyBeforeInitializingData(t *testing.T) {
	if os.Getenv("CRONOS_TEST_POLICY_CHILD") == "1" {
		os.Args = []string{"cronos-api", "--dev", "--node-id=policy-test", "--auth-enabled", "--auth-jwt-secret=test-only", "--auth-policy-file=" + os.Getenv("CRONOS_TEST_POLICY_PATH"), "--data-dir=" + os.Getenv("CRONOS_TEST_DATA_PATH")}
		main()
		return
	}
	for _, test := range []struct{ name, contents string }{
		{"missing", ""}, {"empty", ""}, {"malformed", "{"}, {"empty-subjects", "{}"}, {"null-subject", `{"operator":null}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			policy := filepath.Join(dir, "policy.json")
			if test.name != "missing" {
				if err := os.WriteFile(policy, []byte(test.contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			data := filepath.Join(dir, "node-data")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartupRejectsInvalidPolicyBeforeInitializingData$")
			cmd.Env = append(os.Environ(), "CRONOS_TEST_POLICY_CHILD=1", "CRONOS_TEST_POLICY_PATH="+policy, "CRONOS_TEST_DATA_PATH="+data)
			output, err := cmd.CombinedOutput()
			if err == nil || ctx.Err() != nil {
				t.Fatalf("expected immediate startup failure, got %v: %s", err, output)
			}
			if !bytes.Contains(output, []byte("Failed to load auth policy")) {
				t.Fatalf("startup failed for an unexpected reason: %s", output)
			}
			if _, err := os.Stat(data); !os.IsNotExist(err) {
				t.Fatalf("invalid policy initialized node data: %v", err)
			}
		})
	}
}
