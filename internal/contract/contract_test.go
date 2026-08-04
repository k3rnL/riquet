package contract

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestScenarioRunRecordsTraceAndRedactsCredentials(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/subjects/a/versions" || request.URL.Query().Get("normalize") != "true" {
			t.Errorf("request URL = %s", request.URL.String())
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"id":7}`)
	}))
	t.Cleanup(server.Close)
	target, err := NewEndpointTarget("fixture", server.URL, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	scenario := Scenario{Name: "register", Steps: []Step{{
		Name: "register", Method: http.MethodPost, Path: "/subjects/a/versions?normalize=true",
		Headers: http.Header{"Authorization": {"Bearer secret"}},
		Body:    []byte(`{"schema":"string"}`),
		Expect:  Expectation{Status: http.StatusOK, JSON: []byte(`{"id":7}`)},
	}}}
	trace, err := scenario.Run(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if got := trace.Exchanges[0].RequestHeaders["Authorization"]; len(got) != 1 || got[0] != "[REDACTED]" {
		t.Fatalf("authorization was not redacted: %v", got)
	}
}

func TestCompareTracesMapsSymbolicIDs(t *testing.T) {
	t.Parallel()

	reference := Trace{Exchanges: []Exchange{
		{Method: "POST", Status: 200, ResponseBody: []byte(`{"id":1}`)},
		{Method: "GET", Status: 200, ResponseBody: []byte(`{"id":1}`)},
	}}
	candidate := Trace{Exchanges: []Exchange{
		{Method: "POST", Status: 200, ResponseBody: []byte(`{"id":91}`)},
		{Method: "GET", Status: 200, ResponseBody: []byte(`{"id":91}`)},
	}}
	if difference := CompareTraces(reference, candidate, CompareOptions{SymbolicFields: map[string]string{"id": "schema-id"}}); difference != nil {
		t.Fatalf("unexpected difference: %+v", difference)
	}
	candidate.Exchanges[1].ResponseBody = []byte(`{"id":92}`)
	if difference := CompareTraces(reference, candidate, CompareOptions{SymbolicFields: map[string]string{"id": "schema-id"}}); difference == nil {
		t.Fatal("inconsistent symbolic ID was accepted")
	}
}

func TestCompareTracesCanTreatDiagnosticValuesAsOpaque(t *testing.T) {
	t.Parallel()

	reference := Trace{Exchanges: []Exchange{{
		Method: http.MethodPost, Status: http.StatusUnprocessableEntity,
		ResponseBody: []byte(`{"error_code":42201,"message":"Java parser detail"}`),
	}}}
	candidate := Trace{Exchanges: []Exchange{{
		Method: http.MethodPost, Status: http.StatusUnprocessableEntity,
		ResponseBody: []byte(`{"error_code":42201,"message":"Go parser detail"}`),
	}}}
	if difference := CompareTraces(reference, candidate, CompareOptions{OpaqueFields: map[string]bool{"message": true}}); difference != nil {
		t.Fatal(difference)
	}
	delete(candidate.Exchanges[0].ResponseHeaders, "unused")
	candidate.Exchanges[0].ResponseBody = []byte(`{"error_code":42202,"message":"Go parser detail"}`)
	if difference := CompareTraces(reference, candidate, CompareOptions{OpaqueFields: map[string]bool{"message": true}}); difference == nil {
		t.Fatal("numeric error-code drift was hidden")
	}
}

func TestWriteReportContainsOnlyMismatch(t *testing.T) {
	t.Parallel()

	left := Trace{Exchanges: []Exchange{{Method: "GET", Status: 200}}}
	right := Trace{Exchanges: []Exchange{{Method: "GET", Status: 404}}}
	difference := CompareTraces(left, right, CompareOptions{})
	filename := filepath.Join(t.TempDir(), "report.json")
	if err := WriteReport(filename, NewReport("lookup", "confluent", "riquet", "8.3.0", "compatibility/manifest.json", difference, left, right)); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filename); err != nil || !bytes.Contains(data, []byte(`"difference"`)) {
		t.Fatalf("invalid report: %s, %v", data, err)
	}
}
