// Command mockvendor serves a fault-injectable emulation of the Open-Meteo APIs for
// local chaos drills and CI smoke tests. It is never part of the production image.
package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/votiakov/airq/internal/mockvendor"
)

func main() {
	addr := os.Getenv("MOCK_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           mockvendor.New().Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	slog.Info("mock vendor listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("mock vendor stopped", "error", err)
		os.Exit(1)
	}
}
