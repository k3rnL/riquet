package testkit

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestArtifactsUsesRetainedRoot(t *testing.T) {
	root := t.TempDir()
	t.Setenv("RIQUET_TEST_ARTIFACTS", root)
	dir := Artifacts(t)
	if filepath.Dir(dir) != root {
		t.Fatalf("Artifacts() = %q, want child of %q", dir, root)
	}
}

func TestListenLoopbackAndWaitHTTP(t *testing.T) {
	listener := ListenLoopback(t)
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := WaitHTTP(ctx, http.DefaultClient, "http://"+listener.Addr().String(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

func TestContainerArgsAreStable(t *testing.T) {
	t.Parallel()

	got, err := ContainerArgs(ContainerSpec{
		Name:        "oracle",
		Image:       "example.invalid/oracle@sha256:abc",
		Environment: map[string]string{"Z": "last", "A": "first"},
		Ports:       map[string]string{"8081/tcp": "18081"},
		Args:        []string{"serve"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"run", "--detach", "--rm", "--name", "oracle",
		"--env", "A=first", "--env", "Z=last",
		"--publish", "18081:8081/tcp",
		"example.invalid/oracle@sha256:abc", "serve",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ContainerArgs() = %#v, want %#v", got, want)
	}
}

func TestStartProcessCapturesAndStops(t *testing.T) {
	if os.Getenv("RIQUET_HELPER_PROCESS") == "1" {
		fmt.Println("helper-ready")
		for {
			time.Sleep(time.Second)
		}
	}

	process := StartProcess(
		context.Background(),
		t,
		Artifacts(t),
		"helper",
		os.Args[0],
		[]string{"-test.run=TestStartProcessCapturesAndStops"},
		[]string{"RIQUET_HELPER_PROCESS=1"},
	)
	time.Sleep(25 * time.Millisecond)
	if err := process.Stop(time.Second); err != nil {
		t.Fatal(err)
	}
}
