// Command riquet-restore atomically initializes an empty Riquet backend.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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
	code := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("riquet-restore", flag.ContinueOnError)
	flags.SetOutput(stderr)
	snapshotPath := flags.String("snapshot", "-", "logical snapshot path, or - for standard input")
	validateOnly := flags.Bool("validate-only", false, "validate metadata, state, and checksum without opening a target")
	backend := flags.String("backend", "pvc", "empty target backend (pvc or kafka)")
	dataPath := flags.String("data", ".riquet/riquet.db", "empty PVC database path")
	brokers := flags.String("brokers", os.Getenv("RIQUET_KAFKA_BROKERS"), "comma-separated Kafka brokers")
	topic := flags.String("topic", "_riquet_state", "empty Kafka state topic")
	transactionalID := flags.String("transactional-id", "", "Kafka transactional producer ID")
	replicationFactor := flags.Int("replication-factor", 1, "Kafka state-topic replication factor")
	autoCreate := flags.Bool("auto-create-topic", false, "create the Kafka state topic if missing")
	timeout := flags.Duration("timeout", 2*time.Minute, "restore operation timeout")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *replicationFactor < 1 || *replicationFactor > int(^uint16(0)>>1) {
		_, _ = fmt.Fprintln(stderr, "riquet-restore: replication factor must be between 1 and 32767")
		return 2
	}

	reader := stdin
	var source *os.File
	var err error
	if *snapshotPath != "-" {
		source, err = os.Open(*snapshotPath)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "riquet-restore: open snapshot: %v\n", err)
			return 1
		}
		defer func() { _ = source.Close() }()
		reader = source
	}
	envelope, err := backup.Decode(reader)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "riquet-restore: validate snapshot: %v\n", err)
		return 1
	}
	if *validateOnly {
		_, _ = fmt.Fprintf(stdout, "valid sequence=%s source=%s\n", strconv.FormatUint(uint64(envelope.Snapshot.Sequence), 10), envelope.Source)
		return 0
	}

	restoreCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	var target storage.Store
	switch *backend {
	case "pvc":
		if err := os.MkdirAll(filepath.Dir(*dataPath), 0o750); err != nil {
			_, _ = fmt.Fprintf(stderr, "riquet-restore: create data directory: %v\n", err)
			return 1
		}
		target, err = boltstore.Open(*dataPath, boltstore.Options{})
	case "kafka":
		brokerList := splitNonEmpty(*brokers)
		if len(brokerList) == 0 {
			_, _ = fmt.Fprintln(stderr, "riquet-restore: --brokers or RIQUET_KAFKA_BROKERS is required for Kafka")
			return 2
		}
		target, err = kafkastore.Open(restoreCtx, kafkastore.Options{
			Brokers: brokerList, Topic: *topic, TransactionalID: *transactionalID,
			ReplicationFactor: int16(*replicationFactor), AutoCreateTopic: *autoCreate,
		})
	default:
		_, _ = fmt.Fprintf(stderr, "riquet-restore: unsupported backend %q\n", *backend)
		return 2
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "riquet-restore: open target: %v\n", err)
		return 1
	}
	defer func() { _ = target.Close() }()
	if err := backup.Restore(restoreCtx, target, envelope); err != nil {
		_, _ = fmt.Fprintf(stderr, "riquet-restore: restore: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "restored backend=%s sequence=%s source=%s\n", *backend, strconv.FormatUint(uint64(envelope.Snapshot.Sequence), 10), envelope.Source)
	return 0
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
