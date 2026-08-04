// Package testkit provides shared black-box and process test infrastructure.
package testkit

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

// Artifacts returns a directory for test diagnostics. When
// RIQUET_TEST_ARTIFACTS is set the directory is retained; otherwise it is a
// normal testing temporary directory.
func Artifacts(t testing.TB) string {
	t.Helper()

	root := os.Getenv("RIQUET_TEST_ARTIFACTS")
	if root == "" {
		return t.TempDir()
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(t.Name()))
	dir := filepath.Join(root, fmt.Sprintf("%s-%08x", safeName(t.Name()), hash.Sum32()))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("create retained artifact directory: %v", err)
	}
	return dir
}

func safeName(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.' {
			return r
		}
		return '-'
	}, value)
	value = strings.Trim(value, "-.")
	if value == "" {
		return "test"
	}
	return value
}
