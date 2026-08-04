//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/contract"
)

func TestKafkaRuntimeForwardingAndFailover(t *testing.T) {
	broker := provisionKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "riquet")
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, "../../cmd/riquet").CombinedOutput(); err != nil {
		t.Fatalf("build Riquet: %v: %s", err, output)
	}
	topic := fmt.Sprintf("riquet-runtime-%d", time.Now().UnixNano())
	transactionalID := "riquet-runtime-txn-" + topic
	start := func(name string) contract.Target {
		publicPort, internalPort := availablePort(t), availablePort(t)
		healthPort, metricsPort := availablePort(t), availablePort(t)
		baseURL := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
		target, err := (contract.ProcessProvisioner{
			Name: name, Executable: binary, Args: []string{"--backend", "kafka", "--listen", fmt.Sprintf("127.0.0.1:%d", publicPort)},
			Environment: []string{
				"RIQUET_KAFKA_BROKERS=" + broker, "RIQUET_KAFKA_TOPIC=" + topic,
				"RIQUET_KAFKA_TRANSACTIONAL_ID=" + transactionalID, "RIQUET_KAFKA_AUTO_CREATE_TOPIC=true",
				"RIQUET_KAFKA_REPLICATION_FACTOR=1", "RIQUET_REPLICA_ID=" + name,
				fmt.Sprintf("RIQUET_INTERNAL_ADDRESS=127.0.0.1:%d", internalPort),
				fmt.Sprintf("RIQUET_INTERNAL_ADVERTISE_URL=http://127.0.0.1:%d", internalPort),
				fmt.Sprintf("RIQUET_HEALTH_ADDRESS=127.0.0.1:%d", healthPort),
				fmt.Sprintf("RIQUET_METRICS_ADDRESS=127.0.0.1:%d", metricsPort),
				"RIQUET_INTERNAL_TOKEN=runtime-test-token",
			},
			BaseURL: baseURL, ArtifactsDir: filepath.Join(t.TempDir(), "artifacts"), ReadyTimeout: time.Minute,
		}).Start(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return target
	}
	first := start("replica-1")
	t.Cleanup(func() { _ = first.Close(context.Background()) })
	registerEventually(t, ctx, first.BaseURL().String(), "before-follower")

	second := start("replica-2")
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	registerEventually(t, ctx, second.BaseURL().String(), "forwarded-through-follower")
	waitVersion(t, ctx, first.BaseURL().String(), "forwarded-through-follower")

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := first.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	closeCancel()
	registerEventually(t, ctx, second.BaseURL().String(), "after-failover")
	waitVersion(t, ctx, second.BaseURL().String(), "before-follower")
}

func registerEventually(t testing.TB, ctx context.Context, baseURL, subject string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/subjects/"+subject+"/versions", bytes.NewBufferString(`{"schema":"{\"type\":\"string\"}"}`))
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("registration for %s did not succeed", subject)
}

func waitVersion(t testing.TB, ctx context.Context, baseURL, subject string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/subjects/"+subject+"/versions/latest", nil)
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("subject %s did not converge", subject)
}
