package utils

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestGoSafe_RecoversPanic(t *testing.T) {
	var ran atomic.Bool
	GoSafe("test-panic", func() {
		ran.Store(true)
		panic("intentional test panic")
	})

	time.Sleep(100 * time.Millisecond)
	if !ran.Load() {
		t.Error("expected goroutine to run")
	}
	// If GoSafe did not recover, the test process would have crashed.
}

func TestGoSafe_RunsNormally(t *testing.T) {
	done := make(chan struct{})
	GoSafe("test-normal", func() {
		close(done)
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("expected goroutine to finish")
	}
}

func TestGoSafeLogged_InvokesOnError(t *testing.T) {
	done := make(chan error, 1)
	GoSafeLogged("test-logged-panic", func() {
		panic("logged panic")
	}, func(err error) {
		done <- err
	})

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected non-nil error")
		}
	case <-time.After(time.Second):
		t.Error("expected onError to be invoked")
	}
}
