// Command mockvendor serves a fault-injectable emulation of the Open-Meteo APIs for
// local chaos drills and CI smoke tests. It is never part of the production image.
//
// Usage:
//
//	mockvendor              serve on MOCK_ADDR (default :8090)
//	mockvendor healthcheck  probe the local /healthz (used by the container HEALTHCHECK)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/votiakov/airq/internal/mockvendor"
)

func main() {
	addr := os.Getenv("MOCK_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := healthcheck(addr); err != nil {
			fmt.Fprintln(os.Stderr, "mockvendor:", err)
			os.Exit(1)
		}
		return
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

// healthcheck probes the local /healthz; distroless images have no curl.
func healthcheck(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if n, perr := strconv.Atoi(port); err != nil || perr != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid MOCK_ADDR %q", addr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	target := "http://" + net.JoinHostPort("127.0.0.1", port) + "/healthz"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil) //nolint:gosec // G704: loopback, numeric port
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: target fixed to loopback above
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthz returned %d", resp.StatusCode)
	}
	return nil
}
