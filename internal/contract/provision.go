package contract

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/k3rnL/riquet/internal/testkit"
)

// ProcessProvisioner starts a locally built registry executable.
type ProcessProvisioner struct {
	Name          string
	Executable    string
	Args          []string
	Environment   []string
	BaseURL       string
	ReadinessPath string
	ArtifactsDir  string
	ReadyTimeout  time.Duration
}

// Start launches the process and waits for readiness.
func (p ProcessProvisioner) Start(ctx context.Context) (Target, error) {
	if p.Name == "" || p.Executable == "" || p.BaseURL == "" {
		return nil, errors.New("process target name, executable, and base URL are required")
	}
	if p.ReadinessPath == "" {
		p.ReadinessPath = "/health/ready"
	}
	if p.ReadyTimeout <= 0 {
		p.ReadyTimeout = 30 * time.Second
	}
	if err := os.MkdirAll(p.ArtifactsDir, 0o750); err != nil {
		return nil, fmt.Errorf("create process artifacts: %w", err)
	}
	stdout, err := os.OpenFile(filepath.Join(p.ArtifactsDir, p.Name+".stdout.log"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	stderr, err := os.OpenFile(filepath.Join(p.ArtifactsDir, p.Name+".stderr.log"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		_ = stdout.Close()
		return nil, err
	}
	command := exec.CommandContext(ctx, p.Executable, p.Args...)
	command.Env = append(os.Environ(), p.Environment...)
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, fmt.Errorf("start process target: %w", err)
	}
	process := &managedProcess{command: command, done: make(chan error, 1), stdout: stdout, stderr: stderr}
	go func() { process.done <- command.Wait() }()
	readyCtx, cancel := context.WithTimeout(ctx, p.ReadyTimeout)
	defer cancel()
	if err := testkit.WaitHTTP(readyCtx, http.DefaultClient, p.BaseURL+p.ReadinessPath, 50*time.Millisecond); err != nil {
		_ = process.close(context.Background())
		return nil, err
	}
	return NewEndpointTarget(p.Name, p.BaseURL, nil, nil, process.close)
}

// ComposeProvisioner starts a Docker Compose fixture such as the pinned
// Confluent oracle and owns its complete lifecycle.
type ComposeProvisioner struct {
	Name          string
	File          string
	Project       string
	Environment   []string
	BaseURL       string
	ReadinessPath string
	ArtifactsDir  string
	ReadyTimeout  time.Duration
}

// Start runs compose up and waits for the configured endpoint.
func (p ComposeProvisioner) Start(ctx context.Context) (Target, error) {
	if p.Name == "" || p.File == "" || p.Project == "" || p.BaseURL == "" {
		return nil, errors.New("compose target name, file, project, and base URL are required")
	}
	if p.ReadinessPath == "" {
		p.ReadinessPath = "/schemas/types"
	}
	if p.ReadyTimeout <= 0 {
		p.ReadyTimeout = 2 * time.Minute
	}
	compose := func(commandCtx context.Context, args ...string) ([]byte, error) {
		base := []string{"compose", "--file", p.File, "--project-name", p.Project}
		cmd := exec.CommandContext(commandCtx, "docker", append(base, args...)...)
		cmd.Env = append(os.Environ(), p.Environment...)
		return cmd.CombinedOutput()
	}
	if output, err := compose(ctx, "up", "--detach", "--wait"); err != nil {
		return nil, fmt.Errorf("start compose target: %w: %s", err, output)
	}
	closeFn := func(closeCtx context.Context) error {
		if p.ArtifactsDir != "" {
			if err := os.MkdirAll(p.ArtifactsDir, 0o750); err == nil {
				if logs, logErr := compose(closeCtx, "logs", "--no-color"); logErr == nil {
					_ = os.WriteFile(filepath.Join(p.ArtifactsDir, p.Name+".compose.log"), logs, 0o600)
				}
			}
		}
		output, err := compose(closeCtx, "down", "--volumes", "--remove-orphans")
		if err != nil {
			return fmt.Errorf("stop compose target: %w: %s", err, output)
		}
		return nil
	}
	readyCtx, cancel := context.WithTimeout(ctx, p.ReadyTimeout)
	defer cancel()
	if err := testkit.WaitHTTP(readyCtx, http.DefaultClient, p.BaseURL+p.ReadinessPath, 100*time.Millisecond); err != nil {
		_ = closeFn(context.Background())
		return nil, err
	}
	resetFn := func(resetCtx context.Context) error {
		if output, err := compose(resetCtx, "down", "--volumes", "--remove-orphans"); err != nil {
			return fmt.Errorf("reset compose target down: %w: %s", err, output)
		}
		if output, err := compose(resetCtx, "up", "--detach", "--wait"); err != nil {
			return fmt.Errorf("reset compose target up: %w: %s", err, output)
		}
		return nil
	}
	return NewEndpointTarget(p.Name, p.BaseURL, nil, resetFn, closeFn)
}

type managedProcess struct {
	command *exec.Cmd
	done    chan error
	stdout  *os.File
	stderr  *os.File
	once    sync.Once
	err     error
}

func (p *managedProcess) close(ctx context.Context) error {
	p.once.Do(func() {
		if p.command.Process != nil {
			_ = p.command.Process.Signal(os.Interrupt)
		}
		select {
		case <-p.done:
		case <-ctx.Done():
			if p.command.Process != nil {
				p.err = p.command.Process.Kill()
			}
		case <-time.After(5 * time.Second):
			if p.command.Process != nil {
				p.err = p.command.Process.Kill()
			}
			<-p.done
		}
		p.err = errors.Join(p.err, p.stdout.Close(), p.stderr.Close())
	})
	return p.err
}
