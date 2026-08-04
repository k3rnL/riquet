package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/testkit"
)

func TestRunPersistsAcrossGracefulRestart(t *testing.T) {
	t.Parallel()

	dataPath := filepath.Join(t.TempDir(), "riquet.db")
	start := func() (string, context.CancelFunc, <-chan error) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- Run(ctx, Config{DataPath: dataPath, Listener: listener}) }()
		url := "http://" + listener.Addr().String()
		readyCtx, readyCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer readyCancel()
		if err := testkit.WaitHTTP(readyCtx, http.DefaultClient, url+"/health/ready", 10*time.Millisecond); err != nil {
			cancel()
			t.Fatal(err)
		}
		return url, cancel, done
	}

	url, cancel, done := start()
	register(t, url, "events-value")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	url, cancel, done = start()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url+"/subjects/events-value/versions/latest", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		t.Fatalf("restored lookup status = %d: %s", response.StatusCode, body)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}

func register(t testing.TB, baseURL, subject string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"schema": `{"type":"record","name":"Event","fields":[]}`})
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, baseURL+"/subjects/"+subject+"/versions", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		t.Fatalf("registration status = %d: %s", response.StatusCode, payload)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}
