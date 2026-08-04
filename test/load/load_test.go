package load_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats/avro"
	jsonschemaformat "github.com/k3rnL/riquet/internal/formats/jsonschema"
	"github.com/k3rnL/riquet/internal/formats/protobuf"
	"github.com/k3rnL/riquet/internal/transport/confluent"
)

func TestRegistrationLookupLoad(t *testing.T) {
	count := loadCount(t)
	machine := domain.NewMachine(domain.NewState(), nil, nil)
	server := httptest.NewServer(confluent.NewServer(machine, avro.Engine{}, protobuf.Engine{}, jsonschemaformat.Engine{}).Handler())
	defer server.Close()
	started := time.Now()
	parallel(t, count, 8, func(index int) error {
		schema := fmt.Sprintf(`{"type":"record","name":"Load%06d","fields":[{"name":"value","type":"string"}]}`, index)
		body, _ := json.Marshal(map[string]string{"schema": schema})
		response, err := http.Post(server.URL+fmt.Sprintf("/subjects/load-%06d/versions", index), "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			payload, _ := io.ReadAll(response.Body)
			return fmt.Errorf("registration %d: %s: %s", index, response.Status, payload)
		}
		return nil
	})
	registerDuration := time.Since(started)
	started = time.Now()
	parallel(t, count*2, 16, func(index int) error {
		subject := index % count
		response, err := http.Get(server.URL + fmt.Sprintf("/subjects/load-%06d/versions/latest", subject))
		if err != nil {
			return err
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("lookup %d: %s", subject, response.Status)
		}
		return nil
	})
	lookupDuration := time.Since(started)
	t.Logf("registrations=%d duration=%s rate=%.1f/s lookups=%d duration=%s rate=%.1f/s",
		count, registerDuration, float64(count)/registerDuration.Seconds(),
		count*2, lookupDuration, float64(count*2)/lookupDuration.Seconds())
}

func TestReplayAndFollowerCatchUpLoad(t *testing.T) {
	count := loadCount(t)
	transitions := make([]domain.Envelope, 0, count)
	machine := domain.NewMachine(domain.NewState(), domain.CommitFunc(func(_ context.Context, envelope domain.Envelope) error {
		transitions = append(transitions, envelope)
		return nil
	}), nil)
	for index := 0; index < count; index++ {
		if _, err := machine.Register(context.Background(), domain.RegisterCommand{
			OperationID: domain.OperationID(fmt.Sprintf("load-%06d", index)), Subject: "replay",
			Identity: fmt.Sprintf("identity-%06d", index), Type: domain.SchemaTypeAvro,
			Definition: fmt.Sprintf(`{"type":"record","name":"Replay%06d","fields":[]}`, index),
		}); err != nil {
			t.Fatal(err)
		}
	}
	started := time.Now()
	replayed := domain.NewState()
	for _, envelope := range transitions {
		var err error
		replayed, err = replayed.Apply(envelope)
		if err != nil {
			t.Fatal(err)
		}
	}
	replayDuration := time.Since(started)
	started = time.Now()
	follower := domain.NewMachine(domain.NewState(), nil, nil)
	for _, envelope := range transitions {
		if err := follower.ApplyCommitted(envelope); err != nil {
			t.Fatal(err)
		}
	}
	catchUpDuration := time.Since(started)
	if replayed.Sequence() != domain.Sequence(count) || follower.State().Sequence() != domain.Sequence(count) {
		t.Fatal("load replay did not converge")
	}
	t.Logf("transitions=%d replay=%s rate=%.1f/s follower_catch_up=%s rate=%.1f/s",
		count, replayDuration, float64(count)/replayDuration.Seconds(), catchUpDuration, float64(count)/catchUpDuration.Seconds())
}

func parallel(t *testing.T, count, workers int, operation func(int) error) {
	t.Helper()
	jobs := make(chan int)
	errors := make(chan error, count)
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				if err := operation(index); err != nil {
					errors <- err
					continue
				}
			}
		}()
	}
	for index := 0; index < count; index++ {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
}

func loadCount(t *testing.T) int {
	t.Helper()
	value := 500
	if raw := os.Getenv("RIQUET_LOAD_SCHEMAS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 10000 {
			t.Fatalf("RIQUET_LOAD_SCHEMAS must be between 1 and 10000")
		}
		value = parsed
	}
	return value
}
