package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := run(context.Background(), []string{"-version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run returned %d, stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "version=") {
		t.Fatalf("version output %q does not contain version", stdout.String())
	}
}
