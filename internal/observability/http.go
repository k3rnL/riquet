// Package observability provides bounded health and Prometheus HTTP surfaces.
package observability

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type State struct {
	Started           bool   `json:"started"`
	Ready             bool   `json:"ready"`
	BackendHealthy    bool   `json:"backendHealthy"`
	Role              string `json:"role,omitempty"`
	Epoch             uint64 `json:"epoch,omitempty"`
	AppliedPosition   int64  `json:"appliedPosition,omitempty"`
	CommittedPosition int64  `json:"committedPosition,omitempty"`
	Lag               int64  `json:"lag,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

// LogMiddleware emits bounded structured request outcomes. It intentionally
// excludes URLs, subjects, bodies, authorization, and backend configuration.
func LogMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		capture := &statusWriter{ResponseWriter: writer, status: http.StatusOK}
		next.ServeHTTP(capture, request)
		logger.InfoContext(request.Context(), "HTTP request completed",
			"method", request.Method, "status", capture.status,
			"duration_ms", time.Since(started).Milliseconds())
	})
}

type StateProvider func() State

func HealthHandler(provider StateProvider) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(writer http.ResponseWriter, _ *http.Request) {
		writeHealth(writer, http.StatusOK, map[string]any{"status": "live"})
	})
	mux.HandleFunc("GET /health/startup", func(writer http.ResponseWriter, _ *http.Request) {
		state := provider()
		status := http.StatusOK
		if !state.Started {
			status = http.StatusServiceUnavailable
		}
		writeHealth(writer, status, state)
	})
	mux.HandleFunc("GET /health/ready", func(writer http.ResponseWriter, _ *http.Request) {
		state := provider()
		status := http.StatusOK
		if !state.Ready {
			status = http.StatusServiceUnavailable
		}
		writeHealth(writer, status, state)
	})
	return mux
}

func writeHealth(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

type HTTPMetrics struct {
	requests atomic.Uint64
	inflight atomic.Int64
	nanos    atomic.Uint64
	mu       sync.Mutex
	results  map[int]uint64
	extra    func(io.Writer) error
}

func NewHTTPMetrics(extra func(io.Writer) error) *HTTPMetrics {
	return &HTTPMetrics{results: make(map[int]uint64), extra: extra}
}

func (m *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		m.inflight.Add(1)
		capture := &statusWriter{ResponseWriter: writer, status: http.StatusOK}
		next.ServeHTTP(capture, request)
		m.inflight.Add(-1)
		m.requests.Add(1)
		m.nanos.Add(uint64(time.Since(started)))
		m.mu.Lock()
		m.results[capture.status]++
		m.mu.Unlock()
	})
}

func (m *HTTPMetrics) Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(writer,
			"# TYPE riquet_http_requests_total counter\nriquet_http_requests_total %d\n"+
				"# TYPE riquet_http_inflight gauge\nriquet_http_inflight %d\n"+
				"# TYPE riquet_http_request_duration_seconds_sum counter\nriquet_http_request_duration_seconds_sum %f\n",
			m.requests.Load(), m.inflight.Load(), float64(m.nanos.Load())/float64(time.Second))
		m.mu.Lock()
		for status, count := range m.results {
			_, _ = fmt.Fprintf(writer, "riquet_http_responses_total{status=\"%d\"} %d\n", status, count)
		}
		m.mu.Unlock()
		if m.extra != nil {
			_ = m.extra(writer)
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
