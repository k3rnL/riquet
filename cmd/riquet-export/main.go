// Command riquet-export creates a portable Riquet snapshot from a Confluent API.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/k3rnL/riquet/internal/migration"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("riquet-export", flag.ContinueOnError)
	flags.SetOutput(stderr)
	source := flags.String("source", "", "Confluent Schema Registry base URL")
	output := flags.String("output", "-", "snapshot path, or - for standard output")
	username := flags.String("username", os.Getenv("RIQUET_EXPORT_BASIC_USERNAME"), "Basic authentication username")
	timeout := flags.Duration("timeout", 30*time.Second, "per-request HTTP timeout")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *source == "" {
		_, _ = fmt.Fprintln(stderr, "riquet-export: --source is required")
		return 2
	}
	var snapshot bytes.Buffer
	report, err := migration.ExportConfluent(ctx, &snapshot, migration.ConfluentOptions{
		BaseURL:       *source,
		HTTPClient:    &http.Client{Timeout: *timeout},
		BasicUsername: *username,
		BasicPassword: os.Getenv("RIQUET_EXPORT_BASIC_PASSWORD"),
		BearerToken:   os.Getenv("RIQUET_EXPORT_BEARER_TOKEN"),
	})
	_ = json.NewEncoder(stderr).Encode(report)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "riquet-export: %v\n", err)
		return 1
	}
	if *output == "-" {
		if _, err := io.Copy(stdout, &snapshot); err != nil {
			_, _ = fmt.Fprintf(stderr, "riquet-export: write snapshot: %v\n", err)
			return 1
		}
		return 0
	}
	if err := writeAtomically(*output, snapshot.Bytes()); err != nil {
		_, _ = fmt.Fprintf(stderr, "riquet-export: write snapshot: %v\n", err)
		return 1
	}
	return 0
}

func writeAtomically(path string, contents []byte) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".riquet-export-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
