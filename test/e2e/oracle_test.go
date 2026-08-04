//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/contract"
)

func TestPinnedOracleDiscovery(t *testing.T) {
	port := availablePort(t)
	oracleVersion := os.Getenv("RIQUET_ORACLE_VERSION")
	if oracleVersion == "" {
		oracleVersion = "8.3.0"
	}
	artifacts := os.Getenv("RIQUET_TEST_ARTIFACTS")
	if artifacts == "" {
		artifacts = filepath.Join(t.TempDir(), "artifacts")
	}
	provisioner := contract.ComposeProvisioner{
		Name: "confluent-" + oracleVersion, File: "compose.oracle.yml", Project: fmt.Sprintf("riquet-oracle-%d", port),
		Environment: append(os.Environ(), fmt.Sprintf("RIQUET_ORACLE_PORT=%d", port)),
		BaseURL:     "http://127.0.0.1:" + fmt.Sprint(port), ArtifactsDir: artifacts,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	target, err := provisioner.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Minute)
		defer closeCancel()
		if err := target.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})
	files, err := filepath.Glob("scenarios/discovery/*.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		scenario, err := contract.LoadScenario(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := scenario.Run(ctx, target); err != nil {
			t.Fatalf("%s: %v", filepath.Base(file), err)
		}
	}
	_ = os.WriteFile(filepath.Join(artifacts, "oracle.ok"), []byte(oracleVersion+"\n"), 0o600)
}

func availablePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}
