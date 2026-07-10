package utils

import (
	"fmt"
	"log"
	"runtime/debug"
)

// GoSafe starts fn in a new goroutine and recovers from panics so a single
// background loop cannot crash the process. name is included in the recovery log.
func GoSafe(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[PANIC RECOVERED] goroutine=%s err=%v\n%s", name, r, debug.Stack())
			}
		}()
		fn()
	}()
}

// GoSafeLogged starts fn in a new goroutine and recovers from panics, invoking
// onError with a descriptive error when a panic occurs. This is useful for tests
// or components that need to forward panics to an error handler.
func GoSafeLogged(name string, fn func(), onError func(error)) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("goroutine %s panicked: %v\n%s", name, r, debug.Stack())
				log.Printf("[PANIC RECOVERED] %v", err)
				if onError != nil {
					onError(err)
				}
			}
		}()
		fn()
	}()
}
