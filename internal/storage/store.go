// Package storage defines durable transition stores and coordination adapters.
package storage

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/k3rnL/riquet/internal/domain"
)

// ErrNotPrimary indicates that a mutation reached a replica without the
// current fenced Kafka leadership lease.
var ErrNotPrimary = errors.New("replica is not the current primary")

// Capabilities declares guarantees an implementation actually provides.
type Capabilities struct {
	Writable           bool `json:"writable"`
	Replay             bool `json:"replay"`
	Snapshots          bool `json:"snapshots"`
	Backup             bool `json:"backup"`
	MultiReplica       bool `json:"multiReplica"`
	Leadership         bool `json:"leadership"`
	ConsistentSequence bool `json:"consistentSequence"`
}

// Health describes backend serving state without secrets.
type Health struct {
	Healthy      bool            `json:"healthy"`
	Writable     bool            `json:"writable"`
	LastSequence domain.Sequence `json:"lastSequence"`
	Detail       string          `json:"detail,omitempty"`
	CheckedAt    time.Time       `json:"checkedAt"`
}

// Store is the backend-independent persistence boundary.
type Store interface {
	domain.Committer
	Capabilities() Capabilities
	LoadSnapshot(context.Context) (*domain.Snapshot, error)
	SaveSnapshot(context.Context, domain.Snapshot) error
	Replay(context.Context, domain.Sequence, func(domain.Envelope) error) error
	Backup(context.Context, io.Writer) error
	Health(context.Context) Health
	Close() error
}

// SnapshotRestorer is implemented by stores that can atomically initialize an
// empty target at a logical snapshot sequence.
type SnapshotRestorer interface {
	RestoreSnapshot(context.Context, domain.Snapshot) error
}

// Lease identifies one fenced leadership epoch.
type Lease struct {
	Holder string
	Epoch  uint64
}

// Coordinator is implemented only by HA-capable backends.
type Coordinator interface {
	Acquire(context.Context, string) (Lease, error)
	Renew(context.Context, Lease) error
	Release(context.Context, Lease) error
}
