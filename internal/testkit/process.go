package testkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Process is an owned child process with captured output and bounded cleanup.
type Process struct {
	cmd     *exec.Cmd
	done    chan error
	stdout  *os.File
	stderr  *os.File
	stopErr error
	stop    sync.Once
}

// StartProcess starts a child process, captures stdout/stderr in artifactsDir,
// and registers deterministic cleanup with the test.
func StartProcess(
	ctx context.Context,
	t testing.TB,
	artifactsDir string,
	name string,
	executable string,
	args []string,
	env []string,
) *Process {
	t.Helper()

	stdout := openLog(t, artifactsDir, name+".stdout.log")
	stderr := openLog(t, artifactsDir, name+".stderr.log")
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		t.Fatalf("start %s: %v", name, err)
	}
	process := &Process{
		cmd:    cmd,
		done:   make(chan error, 1),
		stdout: stdout,
		stderr: stderr,
	}
	go func() { process.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if err := process.Stop(5 * time.Second); err != nil {
			t.Errorf("stop %s: %v", name, err)
		}
	})
	return process
}

// Stop interrupts the process, waits for it, and kills it after grace.
func (p *Process) Stop(grace time.Duration) error {
	p.stop.Do(func() {
		if grace <= 0 {
			grace = time.Second
		}
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Signal(os.Interrupt)
		}

		select {
		case err := <-p.done:
			if err != nil && !isExpectedExit(err) {
				p.stopErr = err
			}
		case <-time.After(grace):
			if p.cmd.Process != nil {
				if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					p.stopErr = fmt.Errorf("kill process: %w", err)
				}
			}
			<-p.done
		}
		if err := p.stdout.Close(); err != nil && p.stopErr == nil {
			p.stopErr = fmt.Errorf("close stdout: %w", err)
		}
		if err := p.stderr.Close(); err != nil && p.stopErr == nil {
			p.stopErr = fmt.Errorf("close stderr: %w", err)
		}
	})
	return p.stopErr
}

func openLog(t testing.TB, dir, name string) *os.File {
	t.Helper()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("create artifact directory: %v", err)
	}
	file, err := os.OpenFile(filepath.Join(dir, safeName(name)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open process log: %v", err)
	}
	return file
}

func isExpectedExit(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ProcessState != nil
}
