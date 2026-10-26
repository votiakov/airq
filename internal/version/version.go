// Package version exposes build metadata injected at link time, for example:
//
//	go build -ldflags "-X github.com/votiakov/airq/internal/version.version=v1.2.3"
package version

import (
	"runtime"
	"runtime/debug"
	"sync"
)

// Overridden via -ldflags -X at build time.
var (
	version   = "dev"
	revision  = ""
	buildTime = ""
)

// Info describes the running binary.
type Info struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	BuildTime string `json:"build_time,omitempty"`
	GoVersion string `json:"go_version"`
}

var (
	once sync.Once
	info Info
)

// Get returns the build metadata. When the revision was not injected (for example
// under `go run`), it falls back to the VCS stamp embedded by the Go toolchain.
func Get() Info {
	once.Do(func() {
		info = Info{Version: version, Revision: revision, BuildTime: buildTime, GoVersion: runtime.Version()}
		if info.Revision != "" {
			return
		}
		info.Revision = "unknown"
		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, s := range bi.Settings {
				if s.Key == "vcs.revision" && s.Value != "" {
					info.Revision = s.Value
				}
			}
		}
	})
	return info
}
