package ha

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/storage"
)

type authorityStub struct {
	mu      sync.RWMutex
	primary storage.Lease
	address string
	local   storage.Lease
}

func (a *authorityStub) Primary() (storage.Lease, string, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.primary, a.address, a.primary != (storage.Lease{}) && a.address != ""
}

func (a *authorityStub) LocalLease() (storage.Lease, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.local, a.local != (storage.Lease{})
}

func TestFollowerForwardsMutationAndPreservesResponse(t *testing.T) {
	lease := storage.Lease{Holder: "one", Epoch: 7}
	primaryAuthority := &authorityStub{primary: lease, address: "pending", local: lease}
	var gotPath, gotOperation string
	public := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotPath = request.URL.RequestURI()
		gotOperation = request.Header.Get("X-Request-ID")
		writer.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"error_code":409,"message":"conflict"}`))
	})
	primaryHandler, err := (Forwarder{Authority: primaryAuthority, Token: "secret"}).Handler(public)
	if err != nil {
		t.Fatal(err)
	}
	primaryServer := httptest.NewServer(primaryHandler)
	defer primaryServer.Close()
	primaryAuthority.address = primaryServer.URL

	followerAuthority := &authorityStub{primary: lease, address: primaryServer.URL}
	followerHandler, err := (Forwarder{Authority: followerAuthority, Token: "secret", Timeout: time.Second}).Handler(public)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/subjects/example/versions?normalize=true", bytes.NewBufferString(`{"schema":"string"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	followerHandler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict || response.Body.String() != `{"error_code":409,"message":"conflict"}` {
		t.Fatalf("forwarded response = %d %q", response.Code, response.Body.String())
	}
	if gotPath != "/subjects/example/versions?normalize=true" || gotOperation == "" {
		t.Fatalf("forwarded path/operation = %q/%q", gotPath, gotOperation)
	}
}

func TestInternalReceiverRejectsAuthenticationAndStaleEpoch(t *testing.T) {
	lease := storage.Lease{Holder: "one", Epoch: 2}
	authority := &authorityStub{primary: lease, address: "http://primary", local: lease}
	handler, err := (Forwarder{Authority: authority, Token: "secret"}).Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("rejected mutation reached public handler")
	}))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(mutationRequest{
		Version: protocolVersion, Lease: storage.Lease{Holder: "one", Epoch: 1},
		Method: http.MethodDelete, Path: "/subjects/example",
	})
	for name, authorization := range map[string]string{"missing": "", "wrong": "Bearer wrong"} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, internalPath, bytes.NewReader(payload))
			request.Header.Set("Authorization", authorization)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized || response.Header().Get(forwardError) == "" {
				t.Fatalf("auth response = %d, %q", response.Code, response.Header().Get(forwardError))
			}
		})
	}
	request := httptest.NewRequest(http.MethodPost, internalPath, bytes.NewReader(payload))
	request.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict || response.Header().Get(forwardError) != "stale-epoch" {
		t.Fatalf("stale response = %d, %q", response.Code, response.Header().Get(forwardError))
	}
}

func TestForwardingRejectsLoopsAndMapsUnavailablePrimary(t *testing.T) {
	authority := &authorityStub{}
	handler, err := (Forwarder{Authority: authority, Token: "secret", Retries: 1}).Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("mutation reached public handler")
	}))
	if err != nil {
		t.Fatal(err)
	}
	for name, hop := range map[string]string{"unavailable": "", "loop": "1"} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/config", bytes.NewBufferString(`{}`))
			request.Header.Set(internalHop, hop)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d", response.Code)
			}
			var apiError struct {
				Code int `json:"error_code"`
			}
			if err := json.NewDecoder(response.Body).Decode(&apiError); err != nil || apiError.Code != 50001 {
				t.Fatalf("error body = %+v, %v", apiError, err)
			}
		})
	}
}

type retryTransport struct {
	mu    sync.Mutex
	calls int
}

func (r *retryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.calls == 1 {
		return nil, errors.New("temporary network failure")
	}
	body, _ := json.Marshal(mutationResponse{Status: http.StatusOK, Body: []byte(`{"ok":true}`)})
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), Request: request}, nil
}

func TestForwardingRetriesTransportFailureWithSameOperation(t *testing.T) {
	lease := storage.Lease{Holder: "one", Epoch: 1}
	authority := &authorityStub{primary: lease, address: "http://primary.invalid"}
	transport := &retryTransport{}
	handler, err := (Forwarder{
		Authority: authority, Token: "secret", Retries: 2, Timeout: time.Second,
		Client: &http.Client{Transport: transport},
	}).Handler(http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/subjects/a/versions", bytes.NewBufferString(`{}`)))
	if response.Code != http.StatusOK || response.Body.String() != `{"ok":true}` || transport.calls != 2 {
		t.Fatalf("retry response/calls = %d %q / %d", response.Code, response.Body.String(), transport.calls)
	}
}

func TestForwarderConfigurationValidation(t *testing.T) {
	_, err := (Forwarder{}).Handler(nil)
	if err == nil {
		t.Fatal("missing dependencies accepted")
	}
	_, err = (Forwarder{Authority: &authorityStub{}}).Handler(http.NotFoundHandler())
	if err == nil {
		t.Fatal("missing token accepted")
	}
}
