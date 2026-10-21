package config

import (
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every server flag is explained in the configuration reference, with its
// environment variable, and the reference names no flag the server lacks.
func TestEveryFlagIsInTheConfigurationReference(t *testing.T) {
	oldArgs, oldFlags := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldFlags })
	os.Args = []string{"test", "-dev", "-node-id=test-node"}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	if _, err := LoadConfig(); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "CONFIGURATION.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)

	defined := map[string]bool{}
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		defined[f.Name] = true
		row := regexp.MustCompile("(?m)^\\| `--" + regexp.QuoteMeta(f.Name) + "` \\| `(CRONOS_[A-Z0-9_]+)` \\|")
		m := row.FindStringSubmatch(doc)
		if m == nil {
			t.Errorf("--%s has no row \"| `--%s` | `CRONOS_...` | ...\" in docs/CONFIGURATION.md", f.Name, f.Name)
			return
		}
		want := "CRONOS_" + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
		switch f.Name {
		case "dedup-ttl":
			want = "CRONOS_DEDUP_TTL_HOURS"
		case "min-insync-replicas":
			want = "CRONOS_MIN_IN_SYNC_REPLICAS"
		}
		if m[1] != want {
			t.Errorf("--%s is documented with %s; the server reads %s", f.Name, m[1], want)
		}
	})
	for _, m := range regexp.MustCompile("(?m)^\\| `--([a-z0-9-]+)` \\|").FindAllStringSubmatch(doc, -1) {
		if !defined[m[1]] {
			t.Errorf("docs/CONFIGURATION.md documents --%s, which the server does not have", m[1])
		}
	}
}
