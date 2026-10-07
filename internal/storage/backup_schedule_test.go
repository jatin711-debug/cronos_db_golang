package storage

import (
	"strings"
	"testing"
	"time"
)

// Scheduled backups run at multiples of the interval on the wall clock, so
// every node of a cluster takes its backup at the same moment whenever each
// was started.
func TestNextBackupTime(t *testing.T) {
	at := func(clock string) time.Time {
		t.Helper()
		parsed, err := time.Parse(time.RFC3339, "2026-10-07T"+clock+"Z")
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	for _, tc := range []struct {
		now      string
		interval time.Duration
		want     string
	}{
		{"09:17:42", time.Hour, "10:00:00"},
		{"09:59:59", time.Hour, "10:00:00"},
		// Exactly on a boundary: that backup is running; the next is a full interval away.
		{"10:00:00", time.Hour, "11:00:00"},
		{"09:17:42", 15 * time.Minute, "09:30:00"},
		{"09:17:42", 6 * time.Hour, "12:00:00"},
	} {
		if got := nextBackupTime(at(tc.now), tc.interval); !got.Equal(at(tc.want)) {
			t.Errorf("next backup after %s every %s = %s, want %s", tc.now, tc.interval, got.Format("15:04:05"), tc.want)
		}
	}

	// Two nodes started minutes apart agree on when the next backup is.
	if a, b := nextBackupTime(at("09:02:10"), time.Hour), nextBackupTime(at("09:41:55"), time.Hour); !a.Equal(b) {
		t.Errorf("nodes started at different times plan backups for %s and %s", a, b)
	}
}

// The value stored in a backup identifies the key without giving it away.
func TestKeyCheckValue(t *testing.T) {
	key, other := []byte("0123456789abcdef0123456789abcdef"), []byte("0123456789abcdef0123456789abcdeX")
	check := KeyCheckValue(key)
	if check != KeyCheckValue(append([]byte(nil), key...)) {
		t.Fatal("the same key gives two different check values")
	}
	if check == KeyCheckValue(other) {
		t.Fatal("two keys give the same check value")
	}
	if len(check) != 32 || strings.Contains(check, string(key)) || strings.Contains(string(key), check) {
		t.Fatalf("check value %q is not a 16-byte digest independent of the key text", check)
	}
}
