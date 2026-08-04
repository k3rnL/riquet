// Package backup defines the backend-neutral portable registry snapshot.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/storage"
)

const FormatVersion = 1

type Envelope struct {
	FormatVersion int             `json:"formatVersion"`
	CreatedAt     time.Time       `json:"createdAt"`
	Source        string          `json:"source"`
	Snapshot      domain.Snapshot `json:"snapshot"`
	Checksum      string          `json:"checksum"`
}

// Export writes a validated, secret-free logical snapshot and checksum.
func Export(destination io.Writer, state domain.State, source string, now time.Time) error {
	if destination == nil {
		return errors.New("backup destination is required")
	}
	if source == "" {
		source = "riquet"
	}
	envelope := Envelope{FormatVersion: FormatVersion, CreatedAt: now.UTC(), Source: source, Snapshot: state.Snapshot()}
	envelope.Checksum = checksum(envelope.Snapshot)
	return json.NewEncoder(destination).Encode(envelope)
}

// Capture reconstructs one consistent committed prefix from any storage
// backend and writes the portable logical envelope. A concurrent commit may be
// excluded, but a partial transition can never be included.
func Capture(ctx context.Context, source storage.Store, destination io.Writer, sourceName string, now time.Time) error {
	if source == nil {
		return errors.New("backup source store is required")
	}
	state := domain.NewState()
	snapshot, err := source.LoadSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("load backup snapshot: %w", err)
	}
	if snapshot != nil {
		state, err = domain.Restore(*snapshot)
		if err != nil {
			return err
		}
	}
	if err := source.Replay(ctx, state.Sequence(), func(envelope domain.Envelope) error {
		var applyErr error
		state, applyErr = state.Apply(envelope)
		return applyErr
	}); err != nil {
		return fmt.Errorf("replay backup state: %w", err)
	}
	return Export(destination, state, sourceName, now)
}

func Decode(source io.Reader) (Envelope, error) {
	if source == nil {
		return Envelope{}, errors.New("backup source is required")
	}
	decoder := json.NewDecoder(source)
	decoder.DisallowUnknownFields()
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, fmt.Errorf("decode logical backup: %w", err)
	}
	if envelope.FormatVersion != FormatVersion || envelope.CreatedAt.IsZero() || envelope.Source == "" {
		return Envelope{}, errors.New("logical backup metadata is invalid")
	}
	if _, err := domain.Restore(envelope.Snapshot); err != nil {
		return Envelope{}, err
	}
	if envelope.Checksum != checksum(envelope.Snapshot) {
		return Envelope{}, errors.New("logical backup checksum mismatch")
	}
	return envelope, nil
}

// Restore atomically initializes an empty capable target.
func Restore(ctx context.Context, target storage.Store, envelope Envelope) error {
	if target == nil {
		return errors.New("restore target is required")
	}
	if envelope.FormatVersion != FormatVersion || envelope.Checksum != checksum(envelope.Snapshot) {
		return errors.New("logical backup is invalid")
	}
	restorer, ok := target.(storage.SnapshotRestorer)
	if !ok {
		return errors.New("storage backend does not support logical restore")
	}
	return restorer.RestoreSnapshot(ctx, envelope.Snapshot)
}

func checksum(snapshot domain.Snapshot) string {
	raw, _ := json.Marshal(snapshot)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
