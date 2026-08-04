// Package security provides public and administrative HTTP access controls.
package security

import (
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"strings"
)

type AuthConfig struct {
	Mode        string
	Username    string
	Password    string
	BearerToken string
}

// Authenticate enforces anonymous, Basic, or bearer authentication using
// constant-time credential comparisons.
func Authenticate(config AuthConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		authorized := false
		switch config.Mode {
		case "", "anonymous":
			authorized = true
		case "basic":
			username, password, ok := request.BasicAuth()
			authorized = ok && constantEqual(username, config.Username) && constantEqual(password, config.Password)
		case "bearer":
			value := request.Header.Get("Authorization")
			authorized = strings.HasPrefix(value, "Bearer ") && constantEqual(strings.TrimPrefix(value, "Bearer "), config.BearerToken)
		}
		if !authorized {
			writer.Header().Set("WWW-Authenticate", `Basic realm="riquet", Bearer`)
			writeAuthError(writer, http.StatusUnauthorized, 40101, "Unauthorized")
			return
		}
		next.ServeHTTP(writer, request)
	})
}

// ProtectAdministration optionally requires a scoped token for configuration,
// mode, and deletion mutations. An empty token preserves Confluent's default
// unauthenticated administrative contract.
func ProtectAdministration(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if token == "" || !isAdministrativeMutation(request) {
			next.ServeHTTP(writer, request)
			return
		}
		provided := request.Header.Get("X-Riquet-Admin-Token")
		if token == "" || !constantEqual(provided, token) {
			writeAuthError(writer, http.StatusForbidden, 40301, "Administrative mutation is forbidden")
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func isAdministrativeMutation(request *http.Request) bool {
	if request.Method == http.MethodPut && (request.URL.Path == "/config" || strings.HasPrefix(request.URL.Path, "/config/") || request.URL.Path == "/mode" || strings.HasPrefix(request.URL.Path, "/mode/")) {
		return true
	}
	if request.Method == http.MethodDelete {
		return true
	}
	return false
}

func constantEqual(left, right string) bool {
	leftBytes, rightBytes := []byte(left), []byte(right)
	return len(leftBytes) == len(rightBytes) && subtle.ConstantTimeCompare(leftBytes, rightBytes) == 1
}

func writeAuthError(writer http.ResponseWriter, status, code int, message string) {
	writer.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"error_code": code, "message": message})
}

// TrustedProxySet validates proxy CIDRs and resolves a client address only
// when the direct peer is trusted.
type TrustedProxySet struct{ networks []*net.IPNet }

func NewTrustedProxySet(cidrs []string) (TrustedProxySet, error) {
	result := TrustedProxySet{}
	for _, cidr := range cidrs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			return TrustedProxySet{}, err
		}
		result.networks = append(result.networks, network)
	}
	return result, nil
}

func (p TrustedProxySet) ClientIP(request *http.Request) net.IP {
	host, _, err := net.SplitHostPort(request.RemoteAddr)
	if err != nil {
		host = request.RemoteAddr
	}
	peer := net.ParseIP(host)
	trusted := false
	for _, network := range p.networks {
		trusted = trusted || network.Contains(peer)
	}
	if trusted {
		parts := strings.Split(request.Header.Get("X-Forwarded-For"), ",")
		if len(parts) > 0 {
			if forwarded := net.ParseIP(strings.TrimSpace(parts[0])); forwarded != nil {
				return forwarded
			}
		}
	}
	return peer
}
