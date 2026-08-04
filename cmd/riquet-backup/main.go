// Command riquet-backup captures a portable logical snapshot from a backend.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/k3rnL/riquet/internal/backup"
	"github.com/k3rnL/riquet/internal/storage"
	boltstore "github.com/k3rnL/riquet/internal/storage/bolt"
	kafkastore "github.com/k3rnL/riquet/internal/storage/kafka"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("riquet-backup", flag.ContinueOnError)
	flags.SetOutput(stderr)
	output := flags.String("output", "-", "logical snapshot path, or - for standard output")
	backend := flags.String("backend", "pvc", "source backend (pvc or kafka)")
	dataPath := flags.String("data", ".riquet/riquet.db", "PVC database path")
	brokers := flags.String("brokers", os.Getenv("RIQUET_KAFKA_BROKERS"), "comma-separated Kafka brokers")
	topic := flags.String("topic", "_riquet_state", "Kafka state topic")
	timeout := flags.Duration("timeout", 2*time.Minute, "capture operation timeout")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	captureCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	var source storage.Store
	var err error
	switch *backend {
	case "pvc":
		source, err = boltstore.Open(*dataPath, boltstore.Options{})
	case "kafka":
		brokerList := splitNonEmpty(*brokers)
		if len(brokerList) == 0 {
			_, _ = fmt.Fprintln(stderr, "riquet-backup: --brokers or RIQUET_KAFKA_BROKERS is required for Kafka")
			return 2
		}
		source, err = kafkastore.Open(captureCtx, kafkastore.Options{
			Brokers: brokerList, Topic: *topic,
			TransactionalID: fmt.Sprintf("riquet-backup-%d", time.Now().UnixNano()),
		})
	default:
		_, _ = fmt.Fprintf(stderr, "riquet-backup: unsupported backend %q\n", *backend)
		return 2
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "riquet-backup: open source: %v\n", err)
		return 1
	}
	defer func() { _ = source.Close() }()
	var encoded bytes.Buffer
	if err := backup.Capture(captureCtx, source, &encoded, *backend, time.Now()); err != nil {
		_, _ = fmt.Fprintf(stderr, "riquet-backup: capture: %v\n", err)
		return 1
	}
	if *output == "-" {
		if _, err := io.Copy(stdout, &encoded); err != nil {
			_, _ = fmt.Fprintf(stderr, "riquet-backup: write: %v\n", err)
			return 1
		}
		return 0
	}
	if err := writeAtomically(*output, encoded.Bytes()); err != nil {
		_, _ = fmt.Fprintf(stderr, "riquet-backup: write: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "backup written to %s\n", *output)
	return 0
}

func writeAtomically(path string, contents []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".riquet-backup-*")
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

func splitNonEmpty(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}
