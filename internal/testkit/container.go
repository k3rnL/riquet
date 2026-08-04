package testkit

import (
	"context"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"
)

// ContainerSpec is the portable subset needed by Riquet integration fixtures.
type ContainerSpec struct {
	Name        string
	Image       string
	Environment map[string]string
	Ports       map[string]string
	Args        []string
}

// Container is a test-owned container.
type Container struct {
	runtime string
	id      string
}

// ContainerArgs builds deterministic runtime arguments for a container spec.
func ContainerArgs(spec ContainerSpec) ([]string, error) {
	if spec.Name == "" || spec.Image == "" {
		return nil, fmt.Errorf("container name and image are required")
	}
	args := []string{"run", "--detach", "--rm", "--name", spec.Name}
	for _, key := range sortedKeys(spec.Environment) {
		args = append(args, "--env", key+"="+spec.Environment[key])
	}
	for _, containerPort := range sortedKeys(spec.Ports) {
		args = append(args, "--publish", spec.Ports[containerPort]+":"+containerPort)
	}
	args = append(args, spec.Image)
	args = append(args, spec.Args...)
	return args, nil
}

// StartContainer starts a test-owned container with docker or podman and
// registers cleanup for exactly the returned container ID.
func StartContainer(ctx context.Context, t testing.TB, runtime string, spec ContainerSpec) *Container {
	t.Helper()
	if runtime == "" {
		runtime = "docker"
	}
	args, err := ContainerArgs(spec)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, runtime, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("start container %s: %v: %s", spec.Name, err, output)
	}
	container := &Container{runtime: runtime, id: strings.TrimSpace(string(output))}
	if container.id == "" {
		t.Fatalf("runtime returned an empty container ID for %s", spec.Name)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		output, err := exec.CommandContext(cleanupCtx, runtime, "rm", "--force", container.id).CombinedOutput()
		if err != nil && cleanupCtx.Err() == nil {
			t.Errorf("remove container %s: %v: %s", container.id, err, output)
		}
	})
	return container
}

// ID returns the immutable runtime ID used for exact cleanup.
func (c *Container) ID() string { return c.id }

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
