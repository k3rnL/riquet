package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/k3rnL/riquet/internal/backup"
	"github.com/k3rnL/riquet/internal/domain"
)

func TestExportConfluentPreservesObservableState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer migration-secret" {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		response := func(value any) { _ = json.NewEncoder(writer).Encode(value) }
		switch request.URL.Path {
		case "/subjects":
			response([]string{"events", "deleted", "address"})
		case "/contexts":
			response([]string{":."})
		case "/config":
			response(map[string]any{"compatibilityLevel": "FULL"})
		case "/mode":
			response(map[string]any{"mode": "READONLY"})
		case "/subjects/address/versions":
			response([]int{1})
		case "/subjects/events/versions":
			response([]int{1})
		case "/subjects/deleted/versions":
			if request.URL.Query().Get("deleted") == "true" {
				response([]int{2})
			} else {
				http.Error(writer, `{"error_code":40401}`, http.StatusNotFound)
			}
		case "/subjects/address/versions/1":
			response(map[string]any{
				"subject": "address", "version": 1, "id": 7, "schemaType": "AVRO",
				"schema": `{"type":"record","name":"Address","fields":[{"name":"city","type":"string"}]}`,
				"ts":     1000, "deleted": false,
			})
		case "/subjects/events/versions/1":
			response(map[string]any{
				"subject": "events", "version": 1, "id": 9, "schemaType": "AVRO",
				"references": []map[string]any{{"name": "Address", "subject": "address", "version": 1}},
				"schema":     `{"type":"record","name":"Event","fields":[{"name":"address","type":"Address"}]}`,
				"ts":         2000, "deleted": false,
			})
		case "/subjects/deleted/versions/2":
			response(map[string]any{
				"subject": "deleted", "version": 2, "id": 12, "schemaType": "JSON",
				"schema": `{"type":"string"}`, "ts": 3000, "deleted": true,
			})
		case "/config/events":
			response(map[string]any{"compatibilityLevel": "NONE"})
		case "/mode/events":
			response(map[string]any{"mode": "IMPORT"})
		case "/config/address", "/config/deleted", "/mode/address", "/mode/deleted":
			http.Error(writer, `{"error_code":40409}`, http.StatusNotFound)
		default:
			http.Error(writer, request.URL.String(), http.StatusNotFound)
		}
	}))
	defer server.Close()

	var output bytes.Buffer
	report, err := ExportConfluent(context.Background(), &output, ConfluentOptions{
		BaseURL: server.URL, BearerToken: "migration-secret",
		Now: func() time.Time { return time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Subjects != 3 || report.Versions != 3 || report.SoftDeletedVersions != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	envelope, err := backup.Decode(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	state, err := domain.Restore(envelope.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if state.GlobalCompatibility() != domain.CompatibilityFull || state.GlobalMode() != domain.ModeReadOnly {
		t.Fatalf("global settings not preserved: %+v", state.Snapshot())
	}
	if level, ok := state.SubjectCompatibility("events"); !ok || level != domain.CompatibilityNone {
		t.Fatalf("subject compatibility = %q, %v", level, ok)
	}
	if mode, ok := state.SubjectMode("events"); !ok || mode != domain.ModeImport {
		t.Fatalf("subject mode = %q, %v", mode, ok)
	}
	item, schema, ok := state.Lookup("events", 1, false)
	if !ok || item.SchemaID != 9 || len(schema.References) != 1 || schema.References[0].Subject != "address" {
		t.Fatalf("referenced schema not preserved: %+v %+v", item, schema)
	}
	deleted, _, ok := state.Lookup("deleted", 2, true)
	if !ok || deleted.SchemaID != 12 || !deleted.Deleted {
		t.Fatalf("soft deletion not preserved: %+v, %v", deleted, ok)
	}
	if envelope.Snapshot.NextSchemaID != 13 {
		t.Fatalf("next schema ID = %d", envelope.Snapshot.NextSchemaID)
	}
}

func TestExportConfluentFailsClosedOnUnsupportedFeatures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		response := func(value any) { _ = json.NewEncoder(writer).Encode(value) }
		switch request.URL.Path {
		case "/subjects":
			response([]string{"governed"})
		case "/contexts":
			response([]string{":.", ":.other:"})
		case "/config":
			response(map[string]any{"compatibilityLevel": "BACKWARD", "normalize": true})
		case "/mode":
			response(map[string]any{"mode": "READONLY"})
		case "/subjects/governed/versions":
			response([]int{1})
		case "/subjects/governed/versions/1":
			response(map[string]any{
				"subject": "governed", "version": 1, "id": 1,
				"schema": `{"type":"string"}`, "metadata": map[string]any{"properties": map[string]string{"owner": "team"}},
			})
		case "/config/governed", "/mode/governed":
			http.Error(writer, "not found", http.StatusNotFound)
		default:
			http.Error(writer, request.URL.String(), http.StatusNotFound)
		}
	}))
	defer server.Close()

	var output bytes.Buffer
	report, err := ExportConfluent(context.Background(), &output, ConfluentOptions{BaseURL: server.URL})
	var unsupported *UnsupportedFeaturesError
	if !errors.As(err, &unsupported) {
		t.Fatalf("error = %v", err)
	}
	if len(report.Unsupported) != 3 {
		t.Fatalf("unsupported findings = %+v", report.Unsupported)
	}
	if output.Len() != 0 {
		t.Fatalf("partial snapshot emitted: %s", output.String())
	}
}

func TestConfluentURLPreservesBasePathAndEscapesSubject(t *testing.T) {
	var requested *url.URL
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		copyURL := *request.URL
		requested = &copyURL
		_ = json.NewEncoder(writer).Encode([]int{1})
	}))
	defer server.Close()
	client, err := newConfluentClient(ConfluentOptions{BaseURL: server.URL + "/registry"})
	if err != nil {
		t.Fatal(err)
	}
	var versions []domain.Version
	if _, err := client.get(context.Background(), []string{"subjects", "path/to.proto", "versions"}, nil, &versions, false); err != nil {
		t.Fatal(err)
	}
	if requested == nil || requested.EscapedPath() != "/registry/subjects/path%2Fto.proto/versions" {
		t.Fatalf("requested path = %v", requested)
	}
}
