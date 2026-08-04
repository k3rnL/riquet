package buildinfo

import "testing"

func TestCurrentHasDefaults(t *testing.T) {
	t.Parallel()

	got := Current()
	if got.Version == "" || got.Commit == "" || got.Date == "" {
		t.Fatalf("Current() returned empty metadata: %+v", got)
	}
}
