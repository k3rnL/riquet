package kafka

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/storage"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Store persists the registry transition stream in one compacted Kafka topic.
// A read-committed consumer is retained for acknowledgement and follower replay.
type Store struct {
	options  Options
	admin    *kgo.Client
	producer *kgo.Client
	consumer *kgo.Client

	commitMu sync.Mutex
	mu       sync.RWMutex
	closed   bool
	ready    bool
	lastErr  error
	fatalErr error
	position int64
	end      int64
	items    map[domain.Sequence][]byte
	snapshot []byte
	primary  primaryAnnouncement
	lease    storage.Lease
	base     domain.Sequence
	changed  chan struct{}

	consumeCancel context.CancelFunc
	consumeDone   chan struct{}
	afterCommit   func()
}

type logicalBackup struct {
	FormatVersion int               `json:"formatVersion"`
	Topic         string            `json:"topic"`
	Snapshot      json.RawMessage   `json:"snapshot,omitempty"`
	Transitions   []json.RawMessage `json:"transitions"`
}

type restoreCheckpoint struct {
	FormatVersion int             `json:"formatVersion"`
	Sequence      domain.Sequence `json:"sequence"`
	Checksum      string          `json:"checksum"`
}

// Status is the secret-free HA state used by readiness and metrics.
type Status struct {
	Ready             bool   `json:"ready"`
	Role              string `json:"role"`
	Epoch             uint64 `json:"epoch"`
	AppliedPosition   int64  `json:"appliedPosition"`
	CommittedPosition int64  `json:"committedPosition"`
	Lag               int64  `json:"lag"`
	BackendHealthy    bool   `json:"backendHealthy"`
}

// Open validates/provisions the state topic and replays through the committed
// high-water position before returning a ready store.
func Open(ctx context.Context, raw Options) (*Store, error) {
	options, err := raw.normalized()
	if err != nil {
		return nil, err
	}
	common := commonClientOptions(options)
	admin, err := kgo.NewClient(common...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka administration client: %w", err)
	}
	fail := func(err error) (*Store, error) {
		admin.Close()
		return nil, err
	}
	if err := admin.Ping(ctx); err != nil {
		return fail(fmt.Errorf("connect to Kafka: %w", err))
	}
	if err := provisionAndValidateTopic(ctx, admin, options); err != nil {
		return fail(err)
	}

	producerOptions := append([]kgo.Opt{}, common...)
	producerOptions = append(producerOptions,
		kgo.TransactionalID(options.TransactionalID),
		kgo.TransactionTimeout(options.TransactionTimeout),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	producer, err := kgo.NewClient(producerOptions...)
	if err != nil {
		return fail(fmt.Errorf("create Kafka transactional producer: %w", err))
	}
	consumerOptions := append([]kgo.Opt{}, common...)
	consumerOptions = append(consumerOptions,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			options.Topic: {topicPartition: kgo.NewOffset().AtStart()},
		}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// Control markers carry offsets too. Keeping and skipping them lets
		// replay prove it reached the exact last-stable offset without
		// confusing a transaction marker gap with follower lag.
		kgo.KeepControlRecords(),
	)
	consumer, err := kgo.NewClient(consumerOptions...)
	if err != nil {
		producer.Close()
		return fail(fmt.Errorf("create Kafka replay consumer: %w", err))
	}

	store := &Store{
		options: options, admin: admin, producer: producer, consumer: consumer,
		items: make(map[domain.Sequence][]byte), changed: make(chan struct{}),
		consumeDone: make(chan struct{}),
	}
	end, err := store.committedEnd(ctx)
	if err != nil {
		_ = store.closeClients()
		return nil, err
	}
	store.mu.Lock()
	store.end = end
	store.mu.Unlock()
	if err := store.consumeThrough(ctx, end); err != nil {
		_ = store.closeClients()
		return nil, fmt.Errorf("replay Kafka state topic: %w", err)
	}
	if err := store.validateReplay(); err != nil {
		_ = store.closeClients()
		return nil, err
	}
	store.mu.Lock()
	store.ready = true
	store.signalLocked()
	store.mu.Unlock()

	consumeCtx, cancel := context.WithCancel(context.Background())
	store.consumeCancel = cancel
	go store.consumeLoop(consumeCtx)
	return store, nil
}

// Capabilities reports Kafka's distributed replay and leadership guarantees.
func (s *Store) Capabilities() storage.Capabilities {
	return storage.Capabilities{
		Writable: true, Replay: true, Snapshots: true, Backup: true,
		MultiReplica: true, Leadership: true, ConsistentSequence: true,
	}
}

// Commit transactionally writes the next transition and waits until it is
// visible at read-committed isolation. Exact sequence retries are idempotent.
func (s *Store) Commit(ctx context.Context, envelope domain.Envelope) error {
	if err := envelope.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if err := s.checkLeadership(); err != nil {
		return err
	}

	if retry, err := s.checkSequence(envelope.Sequence, encoded); retry || err != nil {
		return err
	}
	record := &kgo.Record{
		Topic: s.options.Topic, Partition: topicPartition,
		Key: []byte(transitionKey(envelope.Sequence)), Value: encoded,
	}
	if err := s.transaction(ctx, record); err != nil {
		// EndTransaction can fail after Kafka committed. Reconcile through the
		// authoritative read-committed stream before reporting failure.
		reconcileCtx, cancel := context.WithTimeout(context.Background(), s.options.OperationTimeout)
		defer cancel()
		if observed := s.waitForTransition(reconcileCtx, envelope.Sequence, encoded); observed == nil {
			return nil
		}
		s.recordError(err)
		return err
	}
	if s.afterCommit != nil {
		s.afterCommit()
	}
	if err := s.waitForTransition(ctx, envelope.Sequence, encoded); err != nil {
		s.recordError(err)
		return fmt.Errorf("observe committed Kafka transition: %w", err)
	}
	return nil
}

func (s *Store) checkSequence(sequence domain.Sequence, encoded []byte) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, os.ErrClosed
	}
	last := s.lastSequenceLocked()
	if sequence <= last {
		stored := s.items[sequence]
		if bytes.Equal(stored, encoded) {
			return true, nil
		}
		return false, fmt.Errorf("sequence %d conflicts with an existing transition", sequence)
	}
	if sequence != last+1 {
		return false, fmt.Errorf("sequence %d does not follow %d", sequence, last)
	}
	return false, nil
}

func (s *Store) transaction(ctx context.Context, records ...*kgo.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.initializeProducer(ctx); err != nil {
		return fmt.Errorf("initialize Kafka transactional producer: %w", err)
	}
	if err := s.producer.BeginTransaction(); err != nil {
		return fmt.Errorf("begin Kafka transaction: %w", err)
	}
	results := s.producer.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		abortCtx, cancel := context.WithTimeout(context.Background(), s.options.OperationTimeout)
		_ = s.producer.EndTransaction(abortCtx, kgo.TryAbort)
		cancel()
		return fmt.Errorf("produce Kafka transaction: %w", err)
	}
	if err := s.producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("commit Kafka transaction: %w", err)
	}
	return nil
}

func (s *Store) initializeProducer(ctx context.Context) error {
	for {
		_, _, err := s.producer.ProducerID(ctx)
		if err == nil {
			return nil
		}
		if !kerr.IsRetriable(err) && !errors.Is(err, kerr.ConcurrentTransactions) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Replay visits validated transitions after the requested logical sequence.
func (s *Store) Replay(ctx context.Context, after domain.Sequence, apply func(domain.Envelope) error) error {
	if apply == nil {
		return errors.New("replay callback is required")
	}
	s.mu.RLock()
	if after < s.base {
		s.mu.RUnlock()
		return fmt.Errorf("replay sequence %d predates restored checkpoint %d; load the snapshot first", after, s.base)
	}
	last := s.lastSequenceLocked()
	encoded := make([][]byte, 0, int(last-after))
	for sequence := after + 1; sequence <= last; sequence++ {
		encoded = append(encoded, bytes.Clone(s.items[sequence]))
	}
	s.mu.RUnlock()
	for _, raw := range encoded {
		if err := ctx.Err(); err != nil {
			return err
		}
		var envelope domain.Envelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return fmt.Errorf("decode Kafka transition: %w", err)
		}
		if err := envelope.Validate(); err != nil {
			return err
		}
		if err := apply(envelope); err != nil {
			return err
		}
	}
	return nil
}

// SaveSnapshot stores the optional replay accelerator transactionally.
func (s *Store) SaveSnapshot(ctx context.Context, snapshot domain.Snapshot) error {
	if _, err := domain.Restore(snapshot); err != nil {
		return err
	}
	s.mu.RLock()
	last := s.lastSequenceLocked()
	s.mu.RUnlock()
	if snapshot.Sequence > last {
		return fmt.Errorf("snapshot sequence %d exceeds committed sequence %d", snapshot.Sequence, last)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	record := &kgo.Record{Topic: s.options.Topic, Partition: topicPartition, Key: []byte(snapshotRecordKey), Value: encoded}
	if err := s.transaction(ctx, record); err != nil {
		s.recordError(err)
		return err
	}
	return s.waitForSnapshot(ctx, encoded)
}

// RestoreSnapshot transactionally initializes an empty Kafka stream from a
// portable snapshot and a compacted sequence checkpoint.
func (s *Store) RestoreSnapshot(ctx context.Context, snapshot domain.Snapshot) error {
	if _, err := domain.Restore(snapshot); err != nil {
		return err
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	s.mu.RLock()
	empty := s.lastSequenceLocked() == 0 && len(s.snapshot) == 0
	s.mu.RUnlock()
	if !empty {
		return errors.New("restore target is not empty")
	}
	checkpoint := restoreCheckpoint{FormatVersion: stateFormatVersion, Sequence: snapshot.Sequence, Checksum: snapshotChecksum(encoded)}
	checkpointBytes, _ := json.Marshal(checkpoint)
	if err := s.transaction(ctx,
		&kgo.Record{Topic: s.options.Topic, Partition: topicPartition, Key: []byte(snapshotRecordKey), Value: encoded},
		&kgo.Record{Topic: s.options.Topic, Partition: topicPartition, Key: []byte(restoreRecordKey), Value: checkpointBytes},
	); err != nil {
		return err
	}
	for {
		s.mu.RLock()
		observed := s.base == snapshot.Sequence && bytes.Equal(s.snapshot, encoded)
		changed := s.changed
		s.mu.RUnlock()
		if observed {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// LoadSnapshot returns the most recently observed committed snapshot.
func (s *Store) LoadSnapshot(ctx context.Context) (*domain.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	raw := bytes.Clone(s.snapshot)
	s.mu.RUnlock()
	if len(raw) == 0 {
		return nil, nil
	}
	var snapshot domain.Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, fmt.Errorf("decode Kafka snapshot: %w", err)
	}
	if _, err := domain.Restore(snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// Backup emits a backend-neutral, deterministic logical log image.
func (s *Store) Backup(ctx context.Context, destination io.Writer) error {
	if destination == nil {
		return errors.New("backup destination is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	sequences := make([]int, 0, len(s.items))
	for sequence := range s.items {
		sequences = append(sequences, int(sequence))
	}
	sort.Ints(sequences)
	backup := logicalBackup{FormatVersion: stateFormatVersion, Topic: s.options.Topic, Snapshot: bytes.Clone(s.snapshot)}
	for _, sequence := range sequences {
		backup.Transitions = append(backup.Transitions, bytes.Clone(s.items[domain.Sequence(sequence)]))
	}
	s.mu.RUnlock()
	encoder := json.NewEncoder(destination)
	return encoder.Encode(backup)
}

// Health reports replay position and readiness without rendering brokers or credentials.
func (s *Store) Health(ctx context.Context) storage.Health {
	health := storage.Health{CheckedAt: time.Now().UTC()}
	if ctx == nil {
		ctx = context.Background()
	}
	checkCtx, cancel := context.WithTimeout(ctx, time.Second)
	err := s.admin.Ping(checkCtx)
	if err == nil {
		if end, endErr := s.committedEnd(checkCtx); endErr == nil {
			s.mu.Lock()
			if end > s.end {
				s.end = end
			}
			if s.fatalErr == nil {
				s.lastErr = nil
				if s.position >= s.end {
					s.ready = true
				}
			}
			s.mu.Unlock()
		} else {
			err = endErr
		}
	}
	cancel()
	if err != nil {
		s.recordError(err)
	}
	s.mu.RLock()
	health.LastSequence = s.lastSequenceLocked()
	lag := s.end - s.position
	if lag < 0 {
		lag = 0
	}
	health.Healthy = !s.closed && s.ready && s.lastErr == nil && ctx.Err() == nil && lag <= s.options.MaxReadyLag
	health.Writable = health.Healthy
	switch {
	case s.closed:
		health.Detail = "closed"
	case ctx.Err() != nil:
		health.Detail = ctx.Err().Error()
	case s.lastErr != nil:
		health.Detail = s.lastErr.Error()
	case !s.ready:
		health.Detail = "replaying"
	case lag > s.options.MaxReadyLag:
		health.Detail = fmt.Sprintf("Kafka replay lag %d exceeds ready limit %d", lag, s.options.MaxReadyLag)
	}
	s.mu.RUnlock()
	return health
}

// Status reports local role, fenced epoch, and lag without connection details.
func (s *Store) Status() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	lag := s.end - s.position
	if lag < 0 {
		lag = 0
	}
	role := "follower"
	if s.lease != (storage.Lease{}) && s.primary.Holder == s.lease.Holder && s.primary.Epoch == s.lease.Epoch {
		role = "primary"
	}
	if !s.ready {
		role = "replaying"
	}
	backendHealthy := !s.closed && s.lastErr == nil
	return Status{
		Ready: backendHealthy && s.ready && lag <= s.options.MaxReadyLag,
		Role:  role, Epoch: s.primary.Epoch, AppliedPosition: s.position,
		CommittedPosition: s.end, Lag: lag, BackendHealthy: backendHealthy,
	}
}

// WriteMetrics emits the Kafka HA gauges in Prometheus text format.
func (s *Store) WriteMetrics(destination io.Writer) error {
	if destination == nil {
		return errors.New("metrics destination is required")
	}
	status := s.Status()
	primary := 0
	if status.Role == "primary" {
		primary = 1
	}
	ready := 0
	if status.Ready {
		ready = 1
	}
	_, err := fmt.Fprintf(destination,
		"# HELP riquet_kafka_primary Whether this replica is the fenced primary.\n"+
			"# TYPE riquet_kafka_primary gauge\nriquet_kafka_primary %d\n"+
			"# HELP riquet_kafka_epoch Last observed primary epoch.\n"+
			"# TYPE riquet_kafka_epoch gauge\nriquet_kafka_epoch %d\n"+
			"# HELP riquet_kafka_applied_position Next applied state-topic offset.\n"+
			"# TYPE riquet_kafka_applied_position gauge\nriquet_kafka_applied_position %d\n"+
			"# HELP riquet_kafka_committed_position Last stable state-topic offset.\n"+
			"# TYPE riquet_kafka_committed_position gauge\nriquet_kafka_committed_position %d\n"+
			"# HELP riquet_kafka_replay_lag Committed offsets not yet applied.\n"+
			"# TYPE riquet_kafka_replay_lag gauge\nriquet_kafka_replay_lag %d\n"+
			"# HELP riquet_kafka_ready Whether replay and backend state permit serving.\n"+
			"# TYPE riquet_kafka_ready gauge\nriquet_kafka_ready %d\n",
		primary, status.Epoch, status.AppliedPosition, status.CommittedPosition, status.Lag, ready,
	)
	return err
}

// Follow applies existing and newly observed transitions in sequence until
// cancellation. This is the follower materialized-view feed.
func (s *Store) Follow(ctx context.Context, after domain.Sequence, apply func(domain.Envelope) error) error {
	if apply == nil {
		return errors.New("follow callback is required")
	}
	for {
		s.mu.RLock()
		closed, changed := s.closed, s.changed
		s.mu.RUnlock()
		if closed {
			return os.ErrClosed
		}
		last := after
		if err := s.Replay(ctx, after, func(envelope domain.Envelope) error {
			if err := apply(envelope); err != nil {
				return err
			}
			last = envelope.Sequence
			return nil
		}); err != nil {
			return err
		}
		if last != after {
			after = last
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Position returns the next consumed Kafka offset and last observed committed end.
func (s *Store) Position() (applied, committed int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.position, s.end
}

// Close stops replay and closes all Kafka clients. It is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.ready = false
	if s.consumeCancel != nil {
		s.consumeCancel()
	}
	s.signalLocked()
	s.mu.Unlock()
	if s.consumeCancel != nil {
		<-s.consumeDone
	}
	return s.closeClients()
}

func (s *Store) closeClients() error {
	if s.consumer != nil {
		s.consumer.Close()
	}
	if s.producer != nil {
		s.producer.Close()
	}
	if s.admin != nil {
		s.admin.Close()
	}
	return nil
}

func (s *Store) committedEnd(ctx context.Context) (int64, error) {
	request := kmsg.NewPtrListOffsetsRequest()
	request.IsolationLevel = 1
	partition := kmsg.NewListOffsetsRequestTopicPartition()
	partition.Partition = topicPartition
	partition.Timestamp = -1
	request.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: s.options.Topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{partition}}}
	response, err := request.RequestWith(ctx, s.admin)
	if err != nil {
		return 0, fmt.Errorf("read Kafka committed end offset: %w", err)
	}
	if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 1 {
		return 0, errors.New("kafka returned no committed end offset")
	}
	result := response.Topics[0].Partitions[0]
	if err := kerr.ErrorForCode(result.ErrorCode); err != nil {
		return 0, fmt.Errorf("read Kafka committed end offset: %w", err)
	}
	return result.Offset, nil
}

func (s *Store) consumeThrough(ctx context.Context, target int64) error {
	if target == 0 {
		return nil
	}
	for {
		s.mu.RLock()
		position := s.position
		s.mu.RUnlock()
		if position >= target {
			return nil
		}
		fetches := s.consumer.PollFetches(ctx)
		if err := s.applyFetches(fetches); err != nil {
			return err
		}
	}
}

func (s *Store) consumeLoop(ctx context.Context) {
	defer close(s.consumeDone)
	for {
		fetches := s.consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if len(fetches.Errors()) > 0 {
			s.recordError(fetches.Errors()[0].Err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if err := s.applyFetches(fetches); err != nil {
			s.recordFatalError(err)
			return
		}
		s.mu.Lock()
		s.lastErr = nil
		s.ready = true
		s.signalLocked()
		s.mu.Unlock()
	}
}

func (s *Store) applyFetches(fetches kgo.Fetches) error {
	for _, fetchError := range fetches.Errors() {
		if fetchError.Err != nil {
			return fetchError.Err
		}
	}
	var applyErr error
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		if applyErr != nil || partition.Topic != s.options.Topic || partition.Partition != topicPartition {
			return
		}
		s.mu.Lock()
		if partition.LastStableOffset > s.end {
			s.end = partition.LastStableOffset
		}
		for _, record := range partition.Records {
			if !record.Attrs.IsControl() {
				if err := s.applyRecordLocked(record); err != nil {
					applyErr = err
					break
				}
			}
			if record.Offset+1 > s.position {
				s.position = record.Offset + 1
			}
		}
		// An empty read-committed fetch can advance beyond aborted records.
		if len(partition.Records) == 0 && partition.LastStableOffset > s.position {
			s.position = partition.LastStableOffset
		}
		s.signalLocked()
		s.mu.Unlock()
	})
	return applyErr
}

func (s *Store) applyRecordLocked(record *kgo.Record) error {
	key := string(record.Key)
	if sequence, ok := parseTransitionKey(key); ok {
		if record.Value == nil {
			return fmt.Errorf("transition %d was compacted or deleted", sequence)
		}
		var envelope domain.Envelope
		if err := json.Unmarshal(record.Value, &envelope); err != nil {
			return fmt.Errorf("decode transition %d: %w", sequence, err)
		}
		if envelope.Sequence != sequence {
			return fmt.Errorf("transition key sequence %d contains sequence %d", sequence, envelope.Sequence)
		}
		if err := envelope.Validate(); err != nil {
			return err
		}
		if previous, exists := s.items[sequence]; exists && !bytes.Equal(previous, record.Value) {
			return fmt.Errorf("conflicting Kafka records for transition sequence %d", sequence)
		}
		s.items[sequence] = bytes.Clone(record.Value)
		return nil
	}
	switch key {
	case snapshotRecordKey:
		if record.Value == nil {
			s.snapshot = nil
			return nil
		}
		var snapshot domain.Snapshot
		if err := json.Unmarshal(record.Value, &snapshot); err != nil {
			return fmt.Errorf("decode Kafka snapshot: %w", err)
		}
		if _, err := domain.Restore(snapshot); err != nil {
			return err
		}
		s.snapshot = bytes.Clone(record.Value)
		return nil
	case primaryRecordKey:
		if record.Value == nil {
			s.primary = primaryAnnouncement{}
			return nil
		}
		var primary primaryAnnouncement
		if err := json.Unmarshal(record.Value, &primary); err != nil {
			return fmt.Errorf("decode Kafka primary announcement: %w", err)
		}
		if err := primary.validate(); err != nil {
			return err
		}
		if primary.Epoch < s.primary.Epoch {
			return fmt.Errorf("kafka primary epoch regressed from %d to %d", s.primary.Epoch, primary.Epoch)
		}
		s.primary = primary
		return nil
	case restoreRecordKey:
		var checkpoint restoreCheckpoint
		if err := json.Unmarshal(record.Value, &checkpoint); err != nil {
			return fmt.Errorf("decode Kafka restore checkpoint: %w", err)
		}
		if checkpoint.FormatVersion != stateFormatVersion || checkpoint.Sequence == 0 || checkpoint.Checksum != snapshotChecksum(s.snapshot) {
			return errors.New("invalid Kafka restore checkpoint")
		}
		s.base = checkpoint.Sequence
		return nil
	default:
		return fmt.Errorf("unsupported Kafka state record key %q", key)
	}
}

func (s *Store) checkLeadership() error {
	if !s.options.RequireLeadership {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lease.Holder == "" || s.lease.Epoch == 0 || s.primary.Holder != s.lease.Holder || s.primary.Epoch != s.lease.Epoch {
		return storage.ErrNotPrimary
	}
	return nil
}

func (s *Store) validateReplay() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	last := s.lastSequenceLocked()
	for sequence := s.base + 1; sequence <= last; sequence++ {
		if len(s.items[sequence]) == 0 {
			return fmt.Errorf("kafka transition sequence %d is missing", sequence)
		}
	}
	return nil
}

func (s *Store) lastSequenceLocked() domain.Sequence {
	last := s.base
	for sequence := range s.items {
		if sequence > last {
			last = sequence
		}
	}
	return last
}

func snapshotChecksum(encoded []byte) string {
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func (s *Store) waitForTransition(ctx context.Context, sequence domain.Sequence, encoded []byte) error {
	for {
		s.mu.RLock()
		stored, exists := s.items[sequence]
		closed := s.closed
		changed := s.changed
		s.mu.RUnlock()
		if exists {
			if bytes.Equal(stored, encoded) {
				return nil
			}
			return fmt.Errorf("sequence %d conflicts with observed Kafka transition", sequence)
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

func (s *Store) waitForSnapshot(ctx context.Context, encoded []byte) error {
	for {
		s.mu.RLock()
		observed := bytes.Equal(s.snapshot, encoded)
		closed := s.closed
		changed := s.changed
		s.mu.RUnlock()
		if observed {
			return nil
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

func (s *Store) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *Store) recordError(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	s.mu.Lock()
	s.lastErr = err
	s.ready = false
	s.signalLocked()
	s.mu.Unlock()
}

func (s *Store) recordFatalError(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	s.mu.Lock()
	s.fatalErr = err
	s.lastErr = err
	s.ready = false
	s.signalLocked()
	s.mu.Unlock()
}
