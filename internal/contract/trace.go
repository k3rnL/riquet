package contract

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultTraceBodyLimit = 1 << 20

// Trace is a serializable sequence of observed HTTP exchanges.
type Trace struct {
	Exchanges []Exchange `json:"exchanges"`
}

// Exchange is the bounded, redacted representation of one request/response.
type Exchange struct {
	Method          string              `json:"method"`
	URL             string              `json:"url"`
	RequestHeaders  map[string][]string `json:"requestHeaders,omitempty"`
	RequestBody     []byte              `json:"requestBody,omitempty"`
	Status          int                 `json:"status,omitempty"`
	ResponseHeaders map[string][]string `json:"responseHeaders,omitempty"`
	ResponseBody    []byte              `json:"responseBody,omitempty"`
	Duration        time.Duration       `json:"duration"`
	Error           string              `json:"error,omitempty"`
}

// Recorder is an HTTP transport that records bounded exchanges.
type Recorder struct {
	next     http.RoundTripper
	maxBody  int64
	mu       sync.Mutex
	exchange []Exchange
}

// NewRecorder wraps next, or http.DefaultTransport when next is nil.
func NewRecorder(next http.RoundTripper) *Recorder {
	if next == nil {
		next = http.DefaultTransport
	}
	return &Recorder{next: next, maxBody: defaultTraceBodyLimit}
}

// RoundTrip records and delegates one HTTP exchange.
func (r *Recorder) RoundTrip(request *http.Request) (*http.Response, error) {
	started := time.Now()
	exchange := Exchange{
		Method:         request.Method,
		URL:            request.URL.String(),
		RequestHeaders: redactedHeaders(request.Header),
	}
	if request.Body != nil {
		body, replacement, err := copyBody(request.Body, r.maxBody)
		if err != nil {
			return nil, fmt.Errorf("capture request body: %w", err)
		}
		exchange.RequestBody = body
		request.Body = replacement
	}

	response, err := r.next.RoundTrip(request)
	exchange.Duration = time.Since(started)
	if err != nil {
		exchange.Error = err.Error()
		r.append(exchange)
		return nil, err
	}
	exchange.Status = response.StatusCode
	exchange.ResponseHeaders = redactedHeaders(response.Header)
	if response.Body != nil {
		body, replacement, copyErr := copyBody(response.Body, r.maxBody)
		if copyErr != nil {
			_ = response.Body.Close()
			return nil, fmt.Errorf("capture response body: %w", copyErr)
		}
		exchange.ResponseBody = body
		response.Body = replacement
	}
	r.append(exchange)
	return response, nil
}

func (r *Recorder) append(exchange Exchange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exchange = append(r.exchange, exchange)
}

// Trace returns a deep copy of recorded exchanges.
func (r *Recorder) Trace() Trace {
	r.mu.Lock()
	defer r.mu.Unlock()
	exchanges := make([]Exchange, len(r.exchange))
	copy(exchanges, r.exchange)
	return Trace{Exchanges: exchanges}
}

// Reset clears all exchanges.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exchange = nil
}

func copyBody(source io.ReadCloser, limit int64) ([]byte, io.ReadCloser, error) {
	data, err := io.ReadAll(io.LimitReader(source, limit+1))
	if err != nil {
		_ = source.Close()
		return nil, nil, err
	}
	captured := data
	if int64(len(captured)) > limit {
		captured = captured[:limit]
	}
	replacement := &joinedReadCloser{Reader: io.MultiReader(bytes.NewReader(data), source), source: source}
	return captured, replacement, nil
}

type joinedReadCloser struct {
	io.Reader
	source io.Closer
}

func (r *joinedReadCloser) Close() error { return r.source.Close() }

func redactedHeaders(headers http.Header) map[string][]string {
	result := make(map[string][]string, len(headers))
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values := headers.Values(key)
		if strings.EqualFold(key, "Authorization") || strings.EqualFold(key, "Proxy-Authorization") || strings.EqualFold(key, "Cookie") {
			values = []string{"[REDACTED]"}
		}
		result[http.CanonicalHeaderKey(key)] = append([]string(nil), values...)
	}
	return result
}
