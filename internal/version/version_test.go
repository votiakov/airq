package version

import (
	"runtime"
	"testing"
)

func TestGet(t *testing.T) {
	v := Get()
	if v.Version != "dev" {
		t.Errorf("Version = %q, want dev when not injected via ldflags", v.Version)
	}
	if v.GoVersion != runtime.Version() {
		t.Errorf("GoVersion = %q", v.GoVersion)
	}
	if v.Revision == "" {
		t.Error("Revision must fall back to a placeholder, never empty")
	}
}
