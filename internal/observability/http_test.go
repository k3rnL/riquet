package observability

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthReflectsStartupAndReadiness(t *testing.T) {
	state := State{Started: true, Ready: false, BackendHealthy: true, Role: "follower", Lag: 4, Reason: "lagging"}
	handler := HealthHandler(func() State { return state })
	for path, want := range map[string]int{"/health/live": 200, "/health/startup": 200, "/health/ready": 503} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != want {
			t.Fatalf("%s status = %d, want %d", path, response.Code, want)
		}
	}
}

func TestStructuredRequestLogExcludesPathBodyAndCredentials(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	handler := LogMiddleware(logger, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
	}))
	request := httptest.NewRequest(http.MethodPost, "/subjects/schema-secret/versions", strings.NewReader("body-secret"))
	request.Header.Set("Authorization", "Bearer token-secret")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	for _, secret := range []string{"schema-secret", "body-secret", "token-secret", "Authorization"} {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("structured log exposed %q: %s", secret, output.String())
		}
	}
	if !strings.Contains(output.String(), `"status":201`) {
		t.Fatalf("structured log lacks outcome: %s", output.String())
	}
}

func TestMetricsCountsResultsWithoutRequestContent(t *testing.T) {
	metrics := NewHTTPMetrics(nil)
	handler := metrics.Middleware(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTeapot)
		_, _ = writer.Write([]byte("schema-secret"))
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/subjects/private/versions", strings.NewReader("token-secret")))
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(response.Body.String(), `riquet_http_responses_total{status="418"} 1`) {
		t.Fatalf("metrics = %s", response.Body.String())
	}
	for _, secret := range []string{"schema-secret", "token-secret", "private"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Fatalf("metrics exposed %q", secret)
		}
	}
}
