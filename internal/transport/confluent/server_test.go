package confluent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
	avroformat "github.com/k3rnL/riquet/internal/formats/avro"
)

func TestCoreRegistryLifecycle(t *testing.T) {
	t.Parallel()

	server := newTestServer(t)
	schema := `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}`
	registered := requestJSON(t, server, http.MethodPost, "/subjects/events-value/versions", map[string]any{"schema": schema})
	assertStatus(t, registered, http.StatusOK)
	assertJSON(t, registered, map[string]any{
		"id": float64(1), "version": float64(1), "guid": "902c836b-a083-8202-41f2-b49049fb4337",
		"schemaType": "AVRO", "schema": `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}`,
	})

	duplicate := requestJSON(t, server, http.MethodPost, "/subjects/events-value/versions", map[string]any{"schema": schema})
	assertStatus(t, duplicate, http.StatusOK)
	assertJSONField(t, duplicate, "id", float64(1))
	assertJSONField(t, duplicate, "version", float64(1))
	assertJSONField(t, duplicate, "guid", "902c836b-a083-8202-41f2-b49049fb4337")

	versions := requestJSON(t, server, http.MethodGet, "/subjects/events-value/versions", nil)
	assertStatus(t, versions, http.StatusOK)
	assertJSON(t, versions, []any{float64(1)})

	version := requestJSON(t, server, http.MethodGet, "/subjects/events-value/versions/latest", nil)
	assertStatus(t, version, http.StatusOK)
	assertJSONField(t, version, "subject", "events-value")
	assertJSONField(t, version, "version", float64(1))
	assertJSONField(t, version, "id", float64(1))

	byID := requestJSON(t, server, http.MethodGet, "/schemas/ids/1", nil)
	assertStatus(t, byID, http.StatusOK)
	assertJSONField(t, byID, "schema", `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}`)
	rawByID := requestJSON(t, server, http.MethodGet, "/schemas/ids/1/schema", nil)
	assertStatus(t, rawByID, http.StatusOK)
	if got := strings.TrimSpace(rawByID.Body.String()); got != `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}` {
		t.Fatalf("raw schema = %s", got)
	}

	lookup := requestJSON(t, server, http.MethodPost, "/subjects/events-value", map[string]any{"schema": schema})
	assertStatus(t, lookup, http.StatusOK)
	assertJSONField(t, lookup, "id", float64(1))

	subjects := requestJSON(t, server, http.MethodGet, "/subjects", nil)
	assertStatus(t, subjects, http.StatusOK)
	assertJSON(t, subjects, []any{"events-value"})

	deleted := requestJSON(t, server, http.MethodDelete, "/subjects/events-value", nil)
	assertStatus(t, deleted, http.StatusOK)
	assertJSON(t, deleted, []any{float64(1)})
	missing := requestJSON(t, server, http.MethodGet, "/subjects/events-value/versions", nil)
	assertError(t, missing, http.StatusNotFound, 40401)
	withDeleted := requestJSON(t, server, http.MethodGet, "/subjects/events-value/versions?deleted=true", nil)
	assertStatus(t, withDeleted, http.StatusOK)
}

func TestConfigModeCompatibilityAndErrors(t *testing.T) {
	t.Parallel()

	server := newTestServer(t)
	invalid := requestJSON(t, server, http.MethodPost, "/subjects/bad/versions", map[string]any{"schema": `{"type":"record"}`})
	assertError(t, invalid, http.StatusUnprocessableEntity, 42201)
	missing := requestJSON(t, server, http.MethodGet, "/subjects/missing/versions/latest", nil)
	assertError(t, missing, http.StatusNotFound, 40401)

	config := requestJSON(t, server, http.MethodPut, "/config/events-value", map[string]any{"compatibility": "NONE"})
	assertStatus(t, config, http.StatusOK)
	gotConfig := requestJSON(t, server, http.MethodGet, "/config/events-value", nil)
	assertJSONField(t, gotConfig, "compatibilityLevel", "NONE")

	schema := `{"type":"record","name":"Event","fields":[]}`
	assertStatus(t, requestJSON(t, server, http.MethodPost, "/subjects/events-value/versions", map[string]any{"schema": schema}), http.StatusOK)
	compatible := requestJSON(t, server, http.MethodPost, "/compatibility/subjects/events-value/versions/latest", map[string]any{"schema": schema})
	assertStatus(t, compatible, http.StatusOK)
	assertJSONField(t, compatible, "is_compatible", true)

	mode := requestJSON(t, server, http.MethodPut, "/mode", map[string]any{"mode": "READONLY"})
	assertStatus(t, mode, http.StatusOK)
	blocked := requestJSON(t, server, http.MethodPost, "/subjects/blocked/versions", map[string]any{"schema": schema})
	assertError(t, blocked, http.StatusUnprocessableEntity, 42205)
	assertStatus(t, requestJSON(t, server, http.MethodPut, "/mode", map[string]any{"mode": "IMPORT"}), http.StatusOK)
	imported := requestJSON(t, server, http.MethodPost, "/subjects/imported/versions", map[string]any{
		"schema": `{"type":"record","name":"Imported","fields":[]}`, "id": 42, "version": 9,
	})
	assertStatus(t, imported, http.StatusOK)
	assertJSONField(t, imported, "id", float64(42))
}

func TestReferencesAndMediaType(t *testing.T) {
	t.Parallel()

	server := newTestServer(t)
	base := `{"type":"record","name":"Common","namespace":"example","fields":[]}`
	assertStatus(t, requestJSON(t, server, http.MethodPost, "/subjects/common/versions", map[string]any{"schema": base}), http.StatusOK)
	dependent := `{"type":"record","name":"Event","namespace":"example","fields":[{"name":"common","type":"example.Common"}]}`
	response := requestJSON(t, server, http.MethodPost, "/subjects/dependent/versions", map[string]any{
		"schema":     dependent,
		"references": []map[string]any{{"name": "example.Common", "subject": "common", "version": 1}},
	})
	assertStatus(t, response, http.StatusOK)
	if got := response.Header().Get("Content-Type"); got != vendorContentType {
		t.Fatalf("content type = %q", got)
	}
	referenced := requestJSON(t, server, http.MethodGet, "/subjects/common/versions/1/referencedby", nil)
	assertStatus(t, referenced, http.StatusOK)
	assertJSON(t, referenced, []any{float64(2)})
}

func TestHealthReadiness(t *testing.T) {
	t.Parallel()

	machine := domain.NewMachine(domain.NewState(), nil, nil)
	server := NewServer(machine, avroformat.Engine{})
	server.SetReadyFunc(func() bool { return false })
	recorder := requestJSON(t, server, http.MethodGet, "/health/ready", nil)
	assertStatus(t, recorder, http.StatusServiceUnavailable)
}

func newTestServer(t testing.TB) *Server {
	t.Helper()
	engine := avroformat.Engine{}
	checker := func(_ domain.State, level domain.CompatibilityLevel, candidate domain.Schema, previous []domain.Schema) (bool, []string) {
		if level == domain.CompatibilityNone {
			return true, nil
		}
		parsedCandidate, err := engine.Parse(context.Background(), formats.ParseRequest{Definition: candidate.Definition}, nil)
		if err != nil {
			return false, []string{err.Error()}
		}
		parsedPrevious := make([]formats.Parsed, 0, len(previous))
		for _, schema := range previous {
			parsed, parseErr := engine.Parse(context.Background(), formats.ParseRequest{Definition: schema.Definition}, nil)
			if parseErr != nil {
				return false, []string{parseErr.Error()}
			}
			parsedPrevious = append(parsedPrevious, parsed)
		}
		compatible, messages, compatibleErr := engine.Compatible(context.Background(), level, parsedCandidate, parsedPrevious)
		if compatibleErr != nil {
			return false, []string{compatibleErr.Error()}
		}
		return compatible, messages
	}
	machine := domain.NewMachine(domain.NewState(), nil, checker)
	return NewServer(machine, engine)
}

func requestJSON(t testing.TB, server *Server, method, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	request := httptest.NewRequest(method, target, reader)
	request.Header.Set("Accept", vendorContentType)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func assertStatus(t testing.TB, recorder *httptest.ResponseRecorder, want int) {
	t.Helper()
	if recorder.Code != want {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, want, recorder.Body.String())
	}
}

func assertError(t testing.TB, recorder *httptest.ResponseRecorder, status, code int) {
	t.Helper()
	assertStatus(t, recorder, status)
	assertJSONField(t, recorder, "error_code", float64(code))
}

func assertJSON(t testing.TB, recorder *httptest.ResponseRecorder, want any) {
	t.Helper()
	var got any
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JSON = %#v, want %#v", got, want)
	}
}

func assertJSONField(t testing.TB, recorder *httptest.ResponseRecorder, field string, want any) {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got[field], want) {
		t.Fatalf("JSON field %s = %#v, want %#v (body: %s)", field, got[field], want, recorder.Body.String())
	}
}
