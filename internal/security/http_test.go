package security

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticationModes(t *testing.T) {
	ok := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNoContent) })
	tests := []struct {
		name   string
		config AuthConfig
		header string
		want   int
	}{
		{"anonymous", AuthConfig{Mode: "anonymous"}, "", 204},
		{"basic", AuthConfig{Mode: "basic", Username: "user", Password: "pass"}, "Basic dXNlcjpwYXNz", 204},
		{"basic wrong", AuthConfig{Mode: "basic", Username: "user", Password: "pass"}, "Basic dXNlcjpiYWQ=", 401},
		{"bearer", AuthConfig{Mode: "bearer", BearerToken: "token"}, "Bearer token", 204},
		{"bearer wrong", AuthConfig{Mode: "bearer", BearerToken: "token"}, "Bearer nope", 401},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/subjects", nil)
			request.Header.Set("Authorization", test.header)
			response := httptest.NewRecorder()
			Authenticate(test.config, ok).ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d", response.Code, test.want)
			}
		})
	}
}

func TestAdministrativeProtection(t *testing.T) {
	next := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNoContent) })
	handler := ProtectAdministration("admin", next)
	for _, test := range []struct {
		method, path string
		token        string
		want         int
	}{
		{http.MethodPost, "/subjects/a/versions", "", 204},
		{http.MethodPut, "/config", "", 403},
		{http.MethodDelete, "/subjects/a", "wrong", 403},
		{http.MethodPut, "/mode/a", "admin", 204},
	} {
		request := httptest.NewRequest(test.method, test.path, nil)
		request.Header.Set("X-Riquet-Admin-Token", test.token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.want {
			t.Fatalf("%s %s status = %d, want %d", test.method, test.path, response.Code, test.want)
		}
	}
}

func TestEmptyAdministrativeTokenPreservesConfluentContract(t *testing.T) {
	next := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNoContent) })
	request := httptest.NewRequest(http.MethodDelete, "/subjects/a", nil)
	response := httptest.NewRecorder()
	ProtectAdministration("", next).ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("empty-token administrative status = %d, want %d", response.Code, http.StatusNoContent)
	}
}

func TestForwardedAddressRequiresTrustedPeer(t *testing.T) {
	proxies, err := NewTrustedProxySet([]string{"10.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.RemoteAddr = "192.0.2.1:1234"
	request.Header.Set("X-Forwarded-For", "203.0.113.9")
	if got := proxies.ClientIP(request).String(); got != "192.0.2.1" {
		t.Fatalf("untrusted proxy client IP = %s", got)
	}
	request.RemoteAddr = "10.1.2.3:1234"
	if got := proxies.ClientIP(request).String(); got != "203.0.113.9" {
		t.Fatalf("trusted proxy client IP = %s", got)
	}
}
