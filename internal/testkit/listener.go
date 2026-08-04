package testkit

import (
	"net"
	"testing"
)

// ListenLoopback reserves an isolated TCP listener and closes it when the test
// finishes. Callers should pass the listener directly to their server instead
// of releasing and racing to reclaim its port.
func ListenLoopback(t testing.TB) net.Listener {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}
