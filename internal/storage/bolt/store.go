// Package bolt implements the single-writer PVC storage profile with bbolt.
package bolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/storage"
	bolt "go.etcd.io/bbolt"
)

const formatVersion = 1

var (
	metaBucket        = []byte("meta")
	transitionsBucket = []byte("transitions")
	formatKey         = []byte("format-version")
	sequenceKey       = []byte("last-sequence")
	snapshotKey       = []byte("snapshot")
)

// Options controls safe database opening.
type Options struct {
	LockTimeout time.Duration
	NoSync      bool
}

// Store is a bbolt-backed transition store.
type Store struct {
	db      *bolt.DB
	path    string
	mu      sync.RWMutex
	closed  bool
	lastErr error
}

// Open obtains the exclusive database lock, initializes an empty database, and
// validates its storage format before returning.
func Open(path string, options Options) (*Store, error) {
	if path == "" {
		return nil, errors.New("bbolt path is required")
	}
	if options.LockTimeout <= 0 {
		options.LockTimeout = time.Second
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: options.LockTimeout, NoSync: options.NoSync})
	if err != nil {
		return nil, fmt.Errorf("open bbolt store: %w", err)
	}
	store := &Store{db: db, path: path}
	if err := db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(transitionsBucket); err != nil {
			return err
		}
		stored := meta.Get(formatKey)
		if stored == nil {
			if err := meta.Put(formatKey, uint64Key(formatVersion)); err != nil {
				return err
			}
			return meta.Put(sequenceKey, uint64Key(0))
		}
		if len(stored) != 8 {
			return errors.New("invalid bbolt format version encoding")
		}
		if binary.BigEndian.Uint64(stored) != formatVersion {
			return fmt.Errorf("unsupported bbolt format version %d", binary.BigEndian.Uint64(stored))
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize bbolt store: %w", err)
	}
	return store, nil
}

// Capabilities reports the intentionally single-writer PVC guarantees.
func (s *Store) Capabilities() storage.Capabilities {
	return storage.Capabilities{
		Writable: true, Replay: true, Snapshots: true, Backup: true,
		ConsistentSequence: true, MultiReplica: false, Leadership: false,
	}
}

// Commit atomically appends the next checksum-valid transition. Exact retries
// of an already stored sequence are idempotent.
func (s *Store) Commit(ctx context.Context, envelope domain.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := envelope.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	err = s.update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(metaBucket)
		transitions := tx.Bucket(transitionsBucket)
		last := readSequence(meta.Get(sequenceKey))
		key := uint64Key(uint64(envelope.Sequence))
		if envelope.Sequence <= last {
			if bytes.Equal(transitions.Get(key), encoded) {
				return nil
			}
			return fmt.Errorf("sequence %d conflicts with an existing transition", envelope.Sequence)
		}
		if envelope.Sequence != last+1 {
			return fmt.Errorf("sequence %d does not follow %d", envelope.Sequence, last)
		}
		if err := transitions.Put(key, encoded); err != nil {
			return err
		}
		return meta.Put(sequenceKey, uint64Key(uint64(envelope.Sequence)))
	})
	s.recordError(err)
	return err
}

// Replay reads checksum-valid transitions strictly after the supplied sequence.
func (s *Store) Replay(ctx context.Context, after domain.Sequence, apply func(domain.Envelope) error) error {
	if apply == nil {
		return errors.New("replay callback is required")
	}
	err := s.view(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(transitionsBucket).Cursor()
		key, value := cursor.Seek(uint64Key(uint64(after + 1)))
		expected := after + 1
		for ; key != nil; key, value = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var envelope domain.Envelope
			if err := json.Unmarshal(value, &envelope); err != nil {
				return fmt.Errorf("decode transition at sequence %d: %w", expected, err)
			}
			if envelope.Sequence != expected {
				return fmt.Errorf("transition sequence %d does not follow %d", envelope.Sequence, expected-1)
			}
			if err := envelope.Validate(); err != nil {
				return err
			}
			if err := apply(envelope); err != nil {
				return err
			}
			expected++
		}
		return nil
	})
	s.recordError(err)
	return err
}

// LoadSnapshot returns a defensive decoded snapshot or nil when absent.
func (s *Store) LoadSnapshot(ctx context.Context) (*domain.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var snapshot *domain.Snapshot
	err := s.view(func(tx *bolt.Tx) error {
		raw := tx.Bucket(metaBucket).Get(snapshotKey)
		if raw == nil {
			return nil
		}
		var decoded domain.Snapshot
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}
		if _, err := domain.Restore(decoded); err != nil {
			return err
		}
		snapshot = &decoded
		return nil
	})
	s.recordError(err)
	return snapshot, err
}

// SaveSnapshot atomically replaces the optional replay accelerator.
func (s *Store) SaveSnapshot(ctx context.Context, snapshot domain.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := domain.Restore(snapshot); err != nil {
		return err
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	err = s.update(func(tx *bolt.Tx) error {
		last := readSequence(tx.Bucket(metaBucket).Get(sequenceKey))
		if snapshot.Sequence > last {
			return fmt.Errorf("snapshot sequence %d exceeds committed sequence %d", snapshot.Sequence, last)
		}
		return tx.Bucket(metaBucket).Put(snapshotKey, encoded)
	})
	s.recordError(err)
	return err
}

// RestoreSnapshot atomically initializes an empty PVC store from a validated
// logical snapshot while preserving its next transition sequence.
func (s *Store) RestoreSnapshot(ctx context.Context, snapshot domain.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := domain.Restore(snapshot); err != nil {
		return err
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	err = s.update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(metaBucket)
		if readSequence(meta.Get(sequenceKey)) != 0 || tx.Bucket(transitionsBucket).Stats().KeyN != 0 || meta.Get(snapshotKey) != nil {
			return errors.New("restore target is not empty")
		}
		if err := meta.Put(snapshotKey, encoded); err != nil {
			return err
		}
		return meta.Put(sequenceKey, uint64Key(uint64(snapshot.Sequence)))
	})
	s.recordError(err)
	return err
}

// Backup writes a transactionally consistent physical database image.
func (s *Store) Backup(ctx context.Context, destination io.Writer) error {
	if destination == nil {
		return errors.New("backup destination is required")
	}
	err := s.view(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := tx.WriteTo(destination)
		return err
	})
	s.recordError(err)
	return err
}

// Health checks readable metadata and reports the most recent backend error.
func (s *Store) Health(ctx context.Context) storage.Health {
	health := storage.Health{CheckedAt: time.Now().UTC(), Writable: true}
	err := s.view(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		health.LastSequence = readSequence(tx.Bucket(metaBucket).Get(sequenceKey))
		return nil
	})
	s.mu.RLock()
	lastErr := s.lastErr
	closed := s.closed
	s.mu.RUnlock()
	if err != nil {
		lastErr = err
	}
	health.Healthy = !closed && lastErr == nil
	if closed {
		health.Writable = false
		health.Detail = "closed"
	} else if lastErr != nil {
		health.Detail = lastErr.Error()
	}
	return health
}

// Close flushes and releases the exclusive database lock.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

// Path returns the explicit database path for operator diagnostics.
func (s *Store) Path() string { return s.path }

func (s *Store) view(fn func(*bolt.Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return os.ErrClosed
	}
	return s.db.View(fn)
}

func (s *Store) update(fn func(*bolt.Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return os.ErrClosed
	}
	return s.db.Update(fn)
}

func (s *Store) recordError(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	s.mu.Lock()
	s.lastErr = err
	s.mu.Unlock()
}

func uint64Key(value uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, value)
	return key
}

func readSequence(value []byte) domain.Sequence {
	if len(value) != 8 {
		return 0
	}
	return domain.Sequence(binary.BigEndian.Uint64(value))
}
