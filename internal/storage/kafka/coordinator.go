package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/k3rnL/riquet/internal/storage"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CoordinatorOptions configures one member of the single-partition election.
type CoordinatorOptions struct {
	GroupID        string
	Advertisement  string
	SessionTimeout time.Duration
	AcquireTimeout time.Duration
}

type primaryAnnouncement struct {
	FormatVersion int    `json:"formatVersion"`
	Holder        string `json:"holder"`
	Epoch         uint64 `json:"epoch"`
	Address       string `json:"address"`
}

func (p primaryAnnouncement) validate() error {
	if p.FormatVersion != stateFormatVersion {
		return fmt.Errorf("unsupported Kafka coordination record version %d", p.FormatVersion)
	}
	if p.Holder == "" || p.Epoch == 0 || p.Address == "" {
		return errors.New("kafka coordination record is missing holder, epoch, or address")
	}
	return nil
}

// Coordinator uses ownership of the state topic's sole partition as the
// election and the store's stable transactional producer as the final fence.
type Coordinator struct {
	store   *Store
	options CoordinatorOptions

	mu       sync.RWMutex
	client   *kgo.Client
	assigned bool
	released bool
	lease    storage.Lease
	changed  chan struct{}
	done     chan struct{}
}

// NewCoordinator prepares a group member. Acquire starts group participation.
func NewCoordinator(store *Store, options CoordinatorOptions) (*Coordinator, error) {
	if store == nil {
		return nil, errors.New("kafka store is required")
	}
	if options.GroupID == "" {
		options.GroupID = "riquet-" + store.options.Topic + "-primary"
	}
	if options.Advertisement == "" {
		return nil, errors.New("internal primary advertisement is required")
	}
	if options.SessionTimeout <= 0 {
		options.SessionTimeout = 10 * time.Second
	}
	if options.AcquireTimeout <= 0 {
		options.AcquireTimeout = 30 * time.Second
	}
	return &Coordinator{store: store, options: options, changed: make(chan struct{}), done: make(chan struct{})}, nil
}

// Acquire joins the coordination group, waits to own partition zero, catches
// up, and publishes a transactionally fenced monotonically increasing epoch.
func (c *Coordinator) Acquire(ctx context.Context, holder string) (storage.Lease, error) {
	if holder == "" {
		return storage.Lease{}, errors.New("coordination holder is required")
	}
	if err := c.start(); err != nil {
		return storage.Lease{}, err
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, c.options.AcquireTimeout)
	defer cancel()
	for {
		c.mu.RLock()
		assigned, released, existing, changed := c.assigned, c.released, c.lease, c.changed
		c.mu.RUnlock()
		if released {
			return storage.Lease{}, os.ErrClosed
		}
		if existing.Holder == holder && existing.Epoch > 0 && assigned {
			return existing, nil
		}
		if assigned {
			lease, err := c.store.activatePrimary(deadlineCtx, holder, c.options.Advertisement)
			if err != nil {
				return storage.Lease{}, err
			}
			c.mu.Lock()
			if !c.assigned || c.released {
				c.mu.Unlock()
				c.store.deactivatePrimary(lease)
				continue
			}
			c.lease = lease
			c.signalLocked()
			c.mu.Unlock()
			return lease, nil
		}
		select {
		case <-deadlineCtx.Done():
			return storage.Lease{}, deadlineCtx.Err()
		case <-changed:
		}
	}
}

// Renew verifies both current group ownership and the observed fenced epoch.
func (c *Coordinator) Renew(ctx context.Context, lease storage.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	valid := !c.released && c.assigned && c.lease == lease
	c.mu.RUnlock()
	if !valid || !c.store.ownsLease(lease) {
		return storage.ErrNotPrimary
	}
	return nil
}

// Release leaves the group and immediately disables local mutation authority.
func (c *Coordinator) Release(_ context.Context, lease storage.Lease) error {
	c.mu.Lock()
	if c.released {
		c.mu.Unlock()
		return nil
	}
	if c.lease != (storage.Lease{}) && c.lease != lease {
		c.mu.Unlock()
		return storage.ErrNotPrimary
	}
	c.released = true
	client := c.client
	c.signalLocked()
	c.mu.Unlock()
	c.store.deactivatePrimary(lease)
	if client != nil {
		client.Close()
		<-c.done
	}
	return nil
}

// Primary returns the latest committed advertisement observed by this replica.
func (c *Coordinator) Primary() (storage.Lease, string, bool) {
	return c.store.Primary()
}

// LocalLease returns this process's active lease, if it still owns the group partition.
func (c *Coordinator) LocalLease() (storage.Lease, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.released || !c.assigned || c.lease == (storage.Lease{}) {
		return storage.Lease{}, false
	}
	return c.lease, true
}

func (c *Coordinator) start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.released {
		return os.ErrClosed
	}
	if c.client != nil {
		return nil
	}
	options := append([]kgo.Opt{}, commonClientOptions(c.store.options)...)
	options = append(options,
		kgo.ConsumerGroup(c.options.GroupID),
		kgo.ConsumeTopics(c.store.options.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.SessionTimeout(c.options.SessionTimeout),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			if hasStatePartition(partitions, c.store.options.Topic) {
				c.setAssigned(true)
			}
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			if hasStatePartition(partitions, c.store.options.Topic) {
				c.setAssigned(false)
			}
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			if hasStatePartition(partitions, c.store.options.Topic) {
				c.setAssigned(false)
			}
		}),
	)
	client, err := kgo.NewClient(options...)
	if err != nil {
		return fmt.Errorf("create Kafka coordination client: %w", err)
	}
	c.client = client
	go func() {
		defer close(c.done)
		for {
			fetches := client.PollFetches(context.Background())
			if errors.Is(fetches.Err(), kgo.ErrClientClosed) {
				return
			}
		}
	}()
	return nil
}

func (c *Coordinator) setAssigned(value bool) {
	c.mu.Lock()
	c.assigned = value
	lease := c.lease
	if !value {
		c.lease = storage.Lease{}
	}
	c.signalLocked()
	c.mu.Unlock()
	if !value && lease != (storage.Lease{}) {
		c.store.deactivatePrimary(lease)
	}
}

func (c *Coordinator) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func hasStatePartition(partitions map[string][]int32, topic string) bool {
	for _, partition := range partitions[topic] {
		if partition == topicPartition {
			return true
		}
	}
	return false
}

func (s *Store) activatePrimary(ctx context.Context, holder, address string) (storage.Lease, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if err := s.CatchUp(ctx); err != nil {
		return storage.Lease{}, fmt.Errorf("recover before primary activation: %w", err)
	}
	s.mu.RLock()
	epoch := s.primary.Epoch + 1
	s.mu.RUnlock()
	announcement := primaryAnnouncement{FormatVersion: stateFormatVersion, Holder: holder, Epoch: epoch, Address: address}
	encoded, err := json.Marshal(announcement)
	if err != nil {
		return storage.Lease{}, err
	}
	record := &kgo.Record{Topic: s.options.Topic, Partition: topicPartition, Key: []byte(primaryRecordKey), Value: encoded}
	if err := s.transaction(ctx, record); err != nil {
		return storage.Lease{}, fmt.Errorf("publish fenced Kafka primary epoch: %w", err)
	}
	if err := s.waitForPrimary(ctx, announcement); err != nil {
		return storage.Lease{}, err
	}
	lease := storage.Lease{Holder: holder, Epoch: epoch}
	s.mu.Lock()
	s.lease = lease
	s.signalLocked()
	s.mu.Unlock()
	return lease, nil
}

// CatchUp waits for this store to observe the current read-committed end.
func (s *Store) CatchUp(ctx context.Context) error {
	end, err := s.committedEnd(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if end > s.end {
		s.end = end
	}
	s.signalLocked()
	s.mu.Unlock()
	for {
		s.mu.RLock()
		position, closed, changed := s.position, s.closed, s.changed
		s.mu.RUnlock()
		if position >= end {
			return s.validateReplay()
		}
		if closed {
			return os.ErrClosed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (s *Store) waitForPrimary(ctx context.Context, wanted primaryAnnouncement) error {
	for {
		s.mu.RLock()
		observed, closed, changed := s.primary, s.closed, s.changed
		s.mu.RUnlock()
		if observed == wanted {
			return nil
		}
		if observed.Epoch > wanted.Epoch {
			return storage.ErrNotPrimary
		}
		if closed {
			return os.ErrClosed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (s *Store) deactivatePrimary(lease storage.Lease) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	s.mu.Lock()
	if s.lease == lease {
		s.lease = storage.Lease{}
		s.signalLocked()
	}
	s.mu.Unlock()
}

func (s *Store) ownsLease(lease storage.Lease) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lease == lease && s.primary.Holder == lease.Holder && s.primary.Epoch == lease.Epoch
}

// Primary returns the last committed holder, epoch, and internal address.
func (s *Store) Primary() (storage.Lease, string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.primary.Holder == "" || s.primary.Epoch == 0 {
		return storage.Lease{}, "", false
	}
	return storage.Lease{Holder: s.primary.Holder, Epoch: s.primary.Epoch}, s.primary.Address, true
}
