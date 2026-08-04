// Package ha implements authenticated internal mutation forwarding.
package ha

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/k3rnL/riquet/internal/storage"
)

const (
	protocolVersion = 1
	internalPath    = "/internal/v1/mutations"
	forwardError    = "X-Riquet-Forward-Error"
	internalHop     = "X-Riquet-Internal-Hop"
	maxMutationBody = 2 << 20
)

// Authority supplies both the committed primary announcement and this
// process's currently fenced local lease.
type Authority interface {
	Primary() (storage.Lease, string, bool)
	LocalLease() (storage.Lease, bool)
}

// Forwarder routes public mutations to the fenced primary and exposes the
// authenticated internal receiver on the same handler tree.
type Forwarder struct {
	Authority Authority
	Token     string
	Client    *http.Client
	Retries   int
	Timeout   time.Duration
}

type mutationRequest struct {
	Version     int               `json:"version"`
	Lease       storage.Lease     `json:"lease"`
	OperationID string            `json:"operationId"`
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	Query       string            `json:"query,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        []byte            `json:"body,omitempty"`
}

type mutationResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"body,omitempty"`
}

// Handler wraps a public API and adds the internal receiver.
func (f Forwarder) Handler(public http.Handler) (http.Handler, error) {
	if public == nil || f.Authority == nil {
		return nil, errors.New("public handler and HA authority are required")
	}
	if f.Token == "" {
		return nil, errors.New("internal forwarding token is required")
	}
	if f.Client == nil {
		f.Client = &http.Client{}
	}
	if f.Retries < 0 {
		return nil, errors.New("forward retries cannot be negative")
	}
	if f.Retries == 0 {
		f.Retries = 2
	}
	if f.Timeout <= 0 {
		f.Timeout = 5 * time.Second
	}
	mux := http.NewServeMux()
	mux.Handle(internalPath, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		f.receive(writer, request, public)
	}))
	mux.Handle("/", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		f.servePublic(writer, request, public)
	}))
	return mux, nil
}

func (f Forwarder) servePublic(writer http.ResponseWriter, request *http.Request, public http.Handler) {
	if !isMutation(request.Method) {
		public.ServeHTTP(writer, request)
		return
	}
	if request.Header.Get(internalHop) != "" {
		writeUnavailable(writer, "Internal mutation forwarding loop rejected")
		return
	}
	if local, ok := f.Authority.LocalLease(); ok {
		if primary, _, found := f.Authority.Primary(); found && primary == local {
			public.ServeHTTP(writer, request)
			return
		}
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxMutationBody+1))
	if err != nil || len(body) > maxMutationBody {
		writeUnavailable(writer, "Mutation could not be forwarded")
		return
	}
	operationID := request.Header.Get("X-Request-ID")
	if operationID == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			writeUnavailable(writer, "Mutation operation identifier is unavailable")
			return
		}
		operationID = hex.EncodeToString(random[:])
		request.Header.Set("X-Request-ID", operationID)
	}
	for attempt := 0; attempt < f.Retries; attempt++ {
		lease, address, ok := f.Authority.Primary()
		if !ok || address == "" {
			continue
		}
		response, retry, err := f.forward(request.Context(), lease, address, operationID, request, body)
		if err != nil || retry {
			continue
		}
		for name, value := range response.Headers {
			writer.Header().Set(name, value)
		}
		writer.WriteHeader(response.Status)
		_, _ = writer.Write(response.Body)
		return
	}
	writeUnavailable(writer, "Primary registry is unavailable")
}

func (f Forwarder) forward(parent context.Context, lease storage.Lease, address, operationID string, original *http.Request, body []byte) (mutationResponse, bool, error) {
	payload := mutationRequest{
		Version: protocolVersion, Lease: lease, OperationID: operationID,
		Method: original.Method, Path: original.URL.Path, Query: original.URL.RawQuery,
		Headers: selectHeaders(original.Header), Body: body,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return mutationResponse{}, false, err
	}
	target, err := url.JoinPath(strings.TrimRight(address, "/"), internalPath)
	if err != nil {
		return mutationResponse{}, false, err
	}
	ctx, cancel := context.WithTimeout(parent, f.Timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(encoded))
	if err != nil {
		return mutationResponse{}, false, err
	}
	request.Header.Set("Authorization", "Bearer "+f.Token)
	request.Header.Set("Content-Type", "application/json")
	response, err := f.Client.Do(request)
	if err != nil {
		return mutationResponse{}, true, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.Header.Get(forwardError) != "" {
		return mutationResponse{}, true, nil
	}
	var decoded mutationResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, maxMutationBody+4096)).Decode(&decoded); err != nil {
		return mutationResponse{}, true, err
	}
	if decoded.Status < 100 || decoded.Status > 599 {
		return mutationResponse{}, true, errors.New("primary returned an invalid forwarded status")
	}
	return decoded, false, nil
}

func (f Forwarder) receive(writer http.ResponseWriter, request *http.Request, public http.Handler) {
	if request.Method != http.MethodPost || !validBearer(request.Header.Get("Authorization"), f.Token) {
		writer.Header().Set(forwardError, "authentication")
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	var input mutationRequest
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxMutationBody+4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.Version != protocolVersion || !isMutation(input.Method) || !validForwardPath(input.Path) {
		writer.Header().Set(forwardError, "invalid")
		http.Error(writer, "invalid internal mutation", http.StatusBadRequest)
		return
	}
	local, ok := f.Authority.LocalLease()
	if !ok || local != input.Lease {
		writer.Header().Set(forwardError, "stale-epoch")
		http.Error(writer, "stale primary epoch", http.StatusConflict)
		return
	}
	target := input.Path
	if input.Query != "" {
		target += "?" + input.Query
	}
	forwarded, err := http.NewRequestWithContext(request.Context(), input.Method, target, bytes.NewReader(input.Body))
	if err != nil {
		writer.Header().Set(forwardError, "invalid")
		http.Error(writer, "invalid internal mutation", http.StatusBadRequest)
		return
	}
	for name, value := range input.Headers {
		forwarded.Header.Set(name, value)
	}
	if input.OperationID != "" {
		forwarded.Header.Set("X-Request-ID", input.OperationID)
	}
	forwarded.Header.Set(internalHop, "1")
	recorder := newResponseRecorder()
	public.ServeHTTP(recorder, forwarded)
	response := mutationResponse{Status: recorder.status, Headers: selectResponseHeaders(recorder.header), Body: recorder.body.Bytes()}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(response)
}

func validBearer(header, token string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided := []byte(strings.TrimPrefix(header, prefix))
	expected := []byte(token)
	return len(provided) == len(expected) && subtle.ConstantTimeCompare(provided, expected) == 1
}

func isMutation(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodDelete
}

func validForwardPath(path string) bool {
	return strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "/internal/") && !strings.Contains(path, "\\")
}

func selectHeaders(headers http.Header) map[string]string {
	result := make(map[string]string)
	for _, name := range []string{"Accept", "Content-Type"} {
		if value := headers.Get(name); value != "" {
			result[name] = value
		}
	}
	return result
}

func selectResponseHeaders(headers http.Header) map[string]string {
	result := make(map[string]string)
	for _, name := range []string{"Content-Type", "X-Request-ID"} {
		if value := headers.Get(name); value != "" {
			result[name] = value
		}
	}
	return result
}

func writeUnavailable(writer http.ResponseWriter, message string) {
	writer.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	writer.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(writer).Encode(map[string]any{"error_code": 50001, "message": message})
}

type responseRecorder struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newResponseRecorder() *responseRecorder {
	return &responseRecorder{header: make(http.Header), status: http.StatusOK}
}

func (r *responseRecorder) Header() http.Header { return r.header }

func (r *responseRecorder) WriteHeader(status int) { r.status = status }

func (r *responseRecorder) Write(value []byte) (int, error) { return r.body.Write(value) }
