package main

import (
	"log/slog"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

// startPprofServer serves runtime profiles on their own listener, apart from
// the health/metrics/dashboard mux. Mutex and block sampling are switched on
// only here, so they cost nothing unless --pprof-addr is set.
func startPprofServer(addr string) *http.Server {
	runtime.SetMutexProfileFraction(100)
	runtime.SetBlockProfileRate(int(time.Millisecond))

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("Serving pprof", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("pprof server failed", "error", err)
		}
	}()
	return server
}
