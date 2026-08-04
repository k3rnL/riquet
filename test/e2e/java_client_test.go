//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/app"
	"github.com/k3rnL/riquet/internal/testkit"
)

func TestJavaConfluentSerializers(t *testing.T) {
	baseURL := startClientRegistry(t, "java-clients.db")
	clientDir, err := filepath.Abs("../clients/java")
	if err != nil {
		t.Fatal(err)
	}
	settings, err := filepath.Abs("../clients/maven-settings.xml")
	if err != nil {
		t.Fatal(err)
	}
	commandCtx, commandCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer commandCancel()
	command := exec.CommandContext(commandCtx, "mvn", "-s", settings, "-q", "compile", "exec:java")
	command.Dir = clientDir
	command.Env = append(os.Environ(), "RIQUET_REGISTRY_URL="+baseURL)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Confluent Java SerDes: %v\n%s", err, output)
	} else {
		t.Logf("%s", output)
	}
	response, err := httpGetSubjects(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	if len(response) < 3 {
		t.Fatalf("Java clients registered only %d subjects: %v", len(response), response)
	}
}

func startClientRegistry(t *testing.T, database string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, app.Config{Listener: listener, DataPath: filepath.Join(t.TempDir(), database)})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(20 * time.Second):
			t.Error("Riquet did not stop after Java client test")
		}
	})
	baseURL := "http://" + listener.Addr().String()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer readyCancel()
	if err := testkit.WaitHTTP(readyCtx, http.DefaultClient, baseURL+"/health/ready", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	return baseURL
}

func httpGetSubjects(baseURL string) ([]string, error) {
	response, err := http.Get(baseURL + "/subjects")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("subjects status %d", response.StatusCode)
	}
	var subjects []string
	err = json.NewDecoder(response.Body).Decode(&subjects)
	return subjects, err
}
