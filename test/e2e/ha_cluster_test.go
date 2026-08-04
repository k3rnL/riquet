//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats/avro"
	jsonschemaformat "github.com/k3rnL/riquet/internal/formats/jsonschema"
	protobufformat "github.com/k3rnL/riquet/internal/formats/protobuf"
	"github.com/k3rnL/riquet/internal/ha"
	"github.com/k3rnL/riquet/internal/storage"
	kafkastore "github.com/k3rnL/riquet/internal/storage/kafka"
	"github.com/k3rnL/riquet/internal/transport/confluent"
)

type haReplica struct {
	id          string
	url         string
	store       *kafkastore.Store
	coordinator *kafkastore.Coordinator
	machine     *domain.Machine
	server      *http.Server
	listener    net.Listener
	followStop  context.CancelFunc

	mu      sync.RWMutex
	lease   storage.Lease
	acquire chan error
}

func TestKafkaHAConcurrentFailoverAndRollingRestart(t *testing.T) {
	fixture := provisionKafkaFixture(t)
	topic := fmt.Sprintf("riquet-ha-%d", time.Now().UnixNano())
	first := newHAReplica(t, fixture.broker, topic, "replica-1")
	defer first.close()
	first.acquirePrimary(t)
	second := newHAReplica(t, fixture.broker, topic, "replica-2")
	defer second.close()
	second.acquirePrimaryAsync()
	waitForPrimaryAnnouncement(t, second, first.id)

	const requests = 40
	responses := make(chan registryRegistration, requests)
	errorsFound := make(chan error, requests)
	var workers sync.WaitGroup
	for index := 0; index < requests; index++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			target := first.url
			if index%2 == 1 {
				target = second.url
			}
			operation := fmt.Sprintf("duplicate-%d", index%5)
			result, err := registerHTTP(target, "concurrent-value", operation, `{"type":"string"}`)
			if err != nil {
				errorsFound <- err
				return
			}
			responses <- result
		}(index)
	}
	workers.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
	close(responses)
	for response := range responses {
		if response.ID != 1 || response.Version != 1 {
			t.Fatalf("duplicate concurrent registration = %+v", response)
		}
	}
	waitForSequence(t, second, 1)
	assertSubjects(t, first.url, []string{"concurrent-value"})
	assertSubjects(t, second.url, []string{"concurrent-value"})

	// Fail over after an acknowledged write. The replacement must recover it
	// before accepting the next allocation.
	first.close()
	second.waitForAcquisition(t)
	if second.currentLease().Epoch != 2 {
		t.Fatalf("replacement epoch = %d, want 2", second.currentLease().Epoch)
	}
	result, err := registerHTTP(second.url, "after-failover", "after-failover", `{"type":"int"}`)
	if err != nil {
		t.Fatal(err)
	}
	if result.ID != 2 || result.Version != 1 {
		t.Fatalf("post-failover allocation = %+v", result)
	}

	// A fresh rolling replacement replays from no local state while reads and
	// writes remain available on the incumbent.
	third := newHAReplica(t, fixture.broker, topic, "replica-3")
	defer third.close()
	third.acquirePrimaryAsync()
	waitForSequence(t, third, 2)
	assertSubjects(t, third.url, []string{"after-failover", "concurrent-value"})
	assertSubjects(t, second.url, []string{"after-failover", "concurrent-value"})
	second.close()
	third.waitForAcquisition(t)
	if third.currentLease().Epoch != 3 {
		t.Fatalf("rolling replacement epoch = %d, want 3", third.currentLease().Epoch)
	}
	result, err = registerHTTP(third.url, "after-roll", "after-roll", `{"type":"long"}`)
	if err != nil {
		t.Fatal(err)
	}
	if result.ID != 3 {
		t.Fatalf("post-roll allocation = %+v", result)
	}
	assertSubjects(t, third.url, []string{"after-failover", "after-roll", "concurrent-value"})
}

func newHAReplica(t *testing.T, broker, topic, id string) *haReplica {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := "http://" + listener.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	store, err := kafkastore.Open(ctx, kafkastore.Options{
		Brokers: []string{broker}, Topic: topic, AutoCreateTopic: true,
		TransactionalID: "riquet-ha-txn-" + topic, RequireLeadership: true,
	})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	state := domain.NewState()
	if err := store.Replay(ctx, 0, func(envelope domain.Envelope) error {
		var applyErr error
		state, applyErr = state.Apply(envelope)
		return applyErr
	}); err != nil {
		_ = store.Close()
		_ = listener.Close()
		t.Fatal(err)
	}
	machine := domain.NewMachine(state, store, nil)
	coordinator, err := kafkastore.NewCoordinator(store, kafkastore.CoordinatorOptions{
		GroupID: "riquet-ha-group-" + topic, Advertisement: address,
	})
	if err != nil {
		t.Fatal(err)
	}
	api := confluent.NewServer(machine, avro.Engine{}, protobufformat.Engine{}, jsonschemaformat.Engine{})
	api.SetReadyFunc(func() bool { return store.Status().Ready })
	forwarded, err := (ha.Forwarder{
		Authority: coordinator, Token: "ha-test-secret", Retries: 4, Timeout: 3 * time.Second,
	}).Handler(api.Handler())
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: forwarded, ReadHeaderTimeout: 2 * time.Second}
	go func() { _ = server.Serve(listener) }()
	followCtx, followStop := context.WithCancel(context.Background())
	go func() { _ = store.Follow(followCtx, machine.State().Sequence(), machine.ApplyCommitted) }()
	return &haReplica{
		id: id, url: address, store: store, coordinator: coordinator, machine: machine,
		server: server, listener: listener, followStop: followStop,
	}
}

func (r *haReplica) acquirePrimary(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lease, err := r.coordinator.Acquire(ctx, r.id)
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.lease = lease
	r.mu.Unlock()
}

func (r *haReplica) acquirePrimaryAsync() {
	r.mu.Lock()
	if r.acquire != nil {
		r.mu.Unlock()
		return
	}
	r.acquire = make(chan error, 1)
	done := r.acquire
	r.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		lease, err := r.coordinator.Acquire(ctx, r.id)
		if err == nil {
			r.mu.Lock()
			r.lease = lease
			r.mu.Unlock()
		}
		done <- err
	}()
}

func (r *haReplica) waitForAcquisition(t testing.TB) {
	t.Helper()
	r.mu.RLock()
	done := r.acquire
	r.mu.RUnlock()
	if done == nil {
		t.Fatal("replica election was not started")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(35 * time.Second):
		t.Fatal("timed out waiting for primary acquisition")
	}
}

func (r *haReplica) currentLease() storage.Lease {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lease
}

func (r *haReplica) close() {
	r.mu.Lock()
	server, coordinator, store, listener := r.server, r.coordinator, r.store, r.listener
	r.server, r.coordinator, r.store, r.listener = nil, nil, nil, nil
	lease := r.lease
	r.mu.Unlock()
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	if r.followStop != nil {
		r.followStop()
	}
	_ = coordinator.Release(ctx, lease)
	_ = store.Close()
	_ = listener.Close()
}

type registryRegistration struct {
	ID      int `json:"id"`
	Version int `json:"version"`
}

func registerHTTP(baseURL, subject, operation, schema string) (registryRegistration, error) {
	body, _ := json.Marshal(map[string]string{"schema": schema})
	request, _ := http.NewRequest(http.MethodPost, baseURL+"/subjects/"+subject+"/versions", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	request.Header.Set("X-Request-ID", operation)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return registryRegistration{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(response.Body)
		return registryRegistration{}, fmt.Errorf("registration status %d: %s", response.StatusCode, raw)
	}
	var result registryRegistration
	err = json.NewDecoder(response.Body).Decode(&result)
	return result, err
}

func waitForSequence(t testing.TB, replica *haReplica, sequence domain.Sequence) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for replica.machine.State().Sequence() < sequence && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if replica.machine.State().Sequence() != sequence {
		t.Fatalf("replica %s sequence = %d, want %d", replica.id, replica.machine.State().Sequence(), sequence)
	}
}

func waitForPrimaryAnnouncement(t testing.TB, replica *haReplica, holder string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		lease, _, ok := replica.store.Primary()
		if ok && lease.Holder == holder {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replica %s did not observe primary %s", replica.id, holder)
}

func assertSubjects(t testing.TB, baseURL string, expected []string) {
	t.Helper()
	response, err := http.Get(baseURL + "/subjects")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var subjects []string
	if err := json.NewDecoder(response.Body).Decode(&subjects); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(subjects) != fmt.Sprint(expected) {
		t.Fatalf("subjects from %s = %v, want %v", baseURL, subjects, expected)
	}
}
