package confluent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
)

const (
	vendorContentType = "application/vnd.schemaregistry.v1+json"
	maxRequestBytes   = 2 << 20
)

// Server implements the in-scope Confluent Schema Registry v1 REST adapter.
type Server struct {
	machine *domain.Machine
	engines map[domain.SchemaType]formats.Engine
	ready   func() bool
}

// NewServer creates an HTTP adapter around a committed domain machine.
func NewServer(machine *domain.Machine, engines ...formats.Engine) *Server {
	byType := make(map[domain.SchemaType]formats.Engine, len(engines))
	for _, engine := range engines {
		byType[engine.Type()] = engine
	}
	return &Server{machine: machine, engines: byType, ready: func() bool { return true }}
}

// SetReadyFunc supplies operational readiness without coupling transport to storage.
func (s *Server) SetReadyFunc(ready func() bool) {
	if ready != nil {
		s.ready = ready
	}
}

// Handler returns the complete public HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", s.healthLive)
	mux.HandleFunc("GET /health/ready", s.healthReady)
	mux.HandleFunc("GET /schemas/types", s.schemaTypes)
	mux.HandleFunc("GET /schemas/ids/{id}", s.schemaByID)
	mux.HandleFunc("GET /schemas/ids/{id}/schema", s.schemaTextByID)
	mux.HandleFunc("GET /schemas/ids/{id}/versions", s.versionsByID)
	mux.HandleFunc("GET /subjects", s.subjects)
	mux.HandleFunc("POST /subjects/{subject}", s.lookupSchema)
	mux.HandleFunc("GET /subjects/{subject}/versions", s.versions)
	mux.HandleFunc("POST /subjects/{subject}/versions", s.register)
	mux.HandleFunc("GET /subjects/{subject}/versions/{version}", s.version)
	mux.HandleFunc("DELETE /subjects/{subject}/versions/{version}", s.deleteVersion)
	mux.HandleFunc("GET /subjects/{subject}/versions/{version}/referencedby", s.referencedBy)
	mux.HandleFunc("DELETE /subjects/{subject}", s.deleteSubject)
	mux.HandleFunc("GET /config", s.getGlobalConfig)
	mux.HandleFunc("PUT /config", s.putGlobalConfig)
	mux.HandleFunc("GET /config/{subject}", s.getSubjectConfig)
	mux.HandleFunc("PUT /config/{subject}", s.putSubjectConfig)
	mux.HandleFunc("DELETE /config/{subject}", s.deleteSubjectConfig)
	mux.HandleFunc("GET /mode", s.getGlobalMode)
	mux.HandleFunc("PUT /mode", s.putGlobalMode)
	mux.HandleFunc("GET /mode/{subject}", s.getSubjectMode)
	mux.HandleFunc("PUT /mode/{subject}", s.putSubjectMode)
	mux.HandleFunc("DELETE /mode/{subject}", s.deleteSubjectMode)
	mux.HandleFunc("POST /compatibility/subjects/{subject}/versions/{version}", s.compatibility)
	return s.middleware(mux)
}

type schemaRequest struct {
	Schema     string             `json:"schema"`
	SchemaType domain.SchemaType  `json:"schemaType,omitempty"`
	References []domain.Reference `json:"references,omitempty"`
	ID         domain.SchemaID    `json:"id,omitempty"`
	Version    domain.Version     `json:"version,omitempty"`
	Metadata   json.RawMessage    `json:"metadata,omitempty"`
	RuleSet    json.RawMessage    `json:"ruleSet,omitempty"`
}

type schemaResponse struct {
	Subject    string             `json:"subject,omitempty"`
	Version    domain.Version     `json:"version,omitempty"`
	ID         domain.SchemaID    `json:"id,omitempty"`
	GUID       domain.SchemaGUID  `json:"guid,omitempty"`
	SchemaType domain.SchemaType  `json:"schemaType,omitempty"`
	References []domain.Reference `json:"references,omitempty"`
	Schema     string             `json:"schema"`
	Timestamp  int64              `json:"ts,omitempty"`
	Deleted    *bool              `json:"deleted,omitempty"`
}

func (s *Server) register(writer http.ResponseWriter, request *http.Request) {
	subject := request.PathValue("subject")
	input, parsed, ok := s.parseRequest(writer, request)
	if !ok {
		return
	}
	result, err := s.machine.Register(request.Context(), domain.RegisterCommand{
		OperationID: operationID(request), Subject: subject, Identity: parsed.Identity,
		Type: parsed.Type, Definition: parsed.Definition, References: parsed.References,
		RequestedID: input.ID, RequestedVersion: input.Version,
		Timestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		writeDomainError(writer, err)
		return
	}
	item, schema, found := s.machine.State().Lookup(subject, result.Version, true)
	if !found {
		writeAPIError(writer, http.StatusInternalServerError, 50001, "Registered schema could not be read")
		return
	}
	writeJSON(writer, http.StatusOK, responseForRegistration(item, schema))
}

func (s *Server) lookupSchema(writer http.ResponseWriter, request *http.Request) {
	_, parsed, ok := s.parseRequest(writer, request)
	if !ok {
		return
	}
	item, schema, found := s.machine.State().FindSubjectSchema(request.PathValue("subject"), parsed.Identity)
	if !found {
		writeAPIError(writer, http.StatusNotFound, 40403, "Schema not found")
		return
	}
	writeJSON(writer, http.StatusOK, responseForLookup(item, schema))
}

func (s *Server) parseRequest(writer http.ResponseWriter, request *http.Request) (schemaRequest, formats.Parsed, bool) {
	var input schemaRequest
	if err := decodeJSON(request.Body, &input); err != nil {
		writeAPIError(writer, http.StatusUnprocessableEntity, 42201, "Invalid schema: "+err.Error())
		return schemaRequest{}, formats.Parsed{}, false
	}
	if input.SchemaType == "" {
		input.SchemaType = domain.SchemaTypeAvro
	}
	if !emptyAdvancedField(input.Metadata) || !emptyAdvancedField(input.RuleSet) {
		writeAPIError(writer, http.StatusUnprocessableEntity, 42201, "Schema metadata and rule sets are not supported in compatibility v1")
		return schemaRequest{}, formats.Parsed{}, false
	}
	engine, ok := s.engines[input.SchemaType]
	if !ok {
		writeAPIError(writer, http.StatusUnprocessableEntity, 42201, "Invalid schema type "+string(input.SchemaType))
		return schemaRequest{}, formats.Parsed{}, false
	}
	parsed, err := engine.Parse(request.Context(), formats.ParseRequest{
		Definition: input.Schema, References: input.References,
		Normalize: queryBool(request, "normalize"), Limits: formats.DefaultLimits(),
	}, stateResolver{s.machine.State()})
	if err != nil {
		writeAPIError(writer, http.StatusUnprocessableEntity, 42201, "Invalid schema: "+err.Error())
		return schemaRequest{}, formats.Parsed{}, false
	}
	return input, parsed, true
}

func emptyAdvancedField(value json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(value))
	return trimmed == "" || trimmed == "null" || trimmed == "{}"
}

func (s *Server) schemaTypes(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, []domain.SchemaType{domain.SchemaTypeJSON, domain.SchemaTypeProtobuf, domain.SchemaTypeAvro})
}

func (s *Server) schemaByID(writer http.ResponseWriter, request *http.Request) {
	id, ok := parseID(writer, request.PathValue("id"))
	if !ok {
		return
	}
	schema, found := s.machine.State().SchemaByID(id)
	if !found {
		writeAPIError(writer, http.StatusNotFound, 40403, fmt.Sprintf("Schema %d not found", id))
		return
	}
	response := schemaResponse{GUID: schema.GUID, SchemaType: schema.Type, References: schema.References, Schema: schema.Definition}
	if item, ok := s.machine.State().LatestForID(id, request.URL.Query().Get("subject")); ok {
		response.Subject = item.Subject
		response.Version = item.Version
		response.Timestamp = item.Timestamp
		response.Deleted = boolPointer(item.Deleted)
	}
	writeJSON(writer, http.StatusOK, response)
}

func (s *Server) schemaTextByID(writer http.ResponseWriter, request *http.Request) {
	id, ok := parseID(writer, request.PathValue("id"))
	if !ok {
		return
	}
	schema, found := s.machine.State().SchemaByID(id)
	if !found {
		writeAPIError(writer, http.StatusNotFound, 40403, fmt.Sprintf("Schema %d not found", id))
		return
	}
	writer.Header().Set("Content-Type", vendorContentType)
	writer.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(writer, schema.Definition)
}

func (s *Server) versionsByID(writer http.ResponseWriter, request *http.Request) {
	id, ok := parseID(writer, request.PathValue("id"))
	if !ok {
		return
	}
	if _, found := s.machine.State().SchemaByID(id); !found {
		writeAPIError(writer, http.StatusNotFound, 40403, fmt.Sprintf("Schema %d not found", id))
		return
	}
	items := s.machine.State().VersionsForID(id)
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		result = append(result, map[string]any{"subject": item.Subject, "version": item.Version})
	}
	writeJSON(writer, http.StatusOK, result)
}

func (s *Server) subjects(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, s.machine.State().Subjects(queryBool(request, "deleted")))
}

func (s *Server) versions(writer http.ResponseWriter, request *http.Request) {
	subject := request.PathValue("subject")
	versions := s.machine.State().Versions(subject, queryBool(request, "deleted"))
	if len(versions) == 0 {
		writeAPIError(writer, http.StatusNotFound, 40401, fmt.Sprintf("Subject '%s' not found.", subject))
		return
	}
	writeJSON(writer, http.StatusOK, versions)
}

func (s *Server) version(writer http.ResponseWriter, request *http.Request) {
	item, schema, ok := s.lookupVersion(writer, request, queryBool(request, "deleted"))
	if !ok {
		return
	}
	writeJSON(writer, http.StatusOK, responseFor(item, schema))
}

func (s *Server) deleteVersion(writer http.ResponseWriter, request *http.Request) {
	version, ok := parseVersionPath(writer, s.machine.State(), request.PathValue("subject"), request.PathValue("version"), true)
	if !ok {
		return
	}
	result, err := s.machine.DeleteVersion(request.Context(), domain.DeleteVersionCommand{
		OperationID: operationID(request), Subject: request.PathValue("subject"), Version: version,
		Permanent: queryBool(request, "permanent"),
	})
	if err != nil {
		writeDomainError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result.Version)
}

func (s *Server) deleteSubject(writer http.ResponseWriter, request *http.Request) {
	result, err := s.machine.DeleteSubject(request.Context(), domain.DeleteSubjectCommand{
		OperationID: operationID(request), Subject: request.PathValue("subject"), Permanent: queryBool(request, "permanent"),
	})
	if err != nil {
		writeDomainError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result.Versions)
}

func (s *Server) referencedBy(writer http.ResponseWriter, request *http.Request) {
	item, _, ok := s.lookupVersion(writer, request, true)
	if !ok {
		return
	}
	references := s.machine.State().ReferencedBy(item.Subject, item.Version)
	ids := make([]domain.SchemaID, 0, len(references))
	for _, reference := range references {
		ids = append(ids, reference.SchemaID)
	}
	writeJSON(writer, http.StatusOK, ids)
}

func (s *Server) getGlobalConfig(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]domain.CompatibilityLevel{"compatibilityLevel": s.machine.State().GlobalCompatibility()})
}

func (s *Server) getSubjectConfig(writer http.ResponseWriter, request *http.Request) {
	subject := request.PathValue("subject")
	level, ok := s.machine.State().SubjectCompatibility(subject)
	if !ok && !queryBool(request, "defaultToGlobal") {
		writeAPIError(writer, http.StatusNotFound, 40408, fmt.Sprintf("Subject '%s' does not have subject-level compatibility configured", subject))
		return
	}
	if !ok {
		level = s.machine.State().GlobalCompatibility()
	}
	writeJSON(writer, http.StatusOK, map[string]domain.CompatibilityLevel{"compatibilityLevel": level})
}

func (s *Server) putGlobalConfig(writer http.ResponseWriter, request *http.Request) {
	s.putConfig(writer, request, "")
}

func (s *Server) putSubjectConfig(writer http.ResponseWriter, request *http.Request) {
	s.putConfig(writer, request, request.PathValue("subject"))
}

func (s *Server) putConfig(writer http.ResponseWriter, request *http.Request, subject string) {
	var body struct {
		Compatibility domain.CompatibilityLevel `json:"compatibility"`
	}
	if err := decodeJSON(request.Body, &body); err != nil || !body.Compatibility.Valid() {
		writeAPIError(writer, http.StatusUnprocessableEntity, 42203, "Invalid compatibility level")
		return
	}
	if err := s.machine.SetCompatibility(request.Context(), operationID(request), domain.Scope{Subject: subject}, body.Compatibility); err != nil {
		writeDomainError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]domain.CompatibilityLevel{"compatibility": body.Compatibility})
}

func (s *Server) deleteSubjectConfig(writer http.ResponseWriter, request *http.Request) {
	subject := request.PathValue("subject")
	level, ok := s.machine.State().SubjectCompatibility(subject)
	if !ok {
		writeAPIError(writer, http.StatusNotFound, 40408, fmt.Sprintf("Subject '%s' does not have subject-level compatibility configured", subject))
		return
	}
	if err := s.machine.DeleteCompatibility(request.Context(), operationID(request), subject); err != nil {
		writeDomainError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]domain.CompatibilityLevel{"compatibilityLevel": level})
}

func (s *Server) getGlobalMode(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]domain.Mode{"mode": s.machine.State().GlobalMode()})
}

func (s *Server) getSubjectMode(writer http.ResponseWriter, request *http.Request) {
	subject := request.PathValue("subject")
	mode, ok := s.machine.State().SubjectMode(subject)
	if !ok {
		writeAPIError(writer, http.StatusNotFound, 40409, fmt.Sprintf("Subject '%s' does not have subject-level mode configured", subject))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]domain.Mode{"mode": mode})
}

func (s *Server) putGlobalMode(writer http.ResponseWriter, request *http.Request) {
	s.putMode(writer, request, "")
}

func (s *Server) putSubjectMode(writer http.ResponseWriter, request *http.Request) {
	s.putMode(writer, request, request.PathValue("subject"))
}

func (s *Server) putMode(writer http.ResponseWriter, request *http.Request, subject string) {
	var body struct {
		Mode domain.Mode `json:"mode"`
	}
	if err := decodeJSON(request.Body, &body); err != nil || !body.Mode.Valid() {
		writeAPIError(writer, http.StatusUnprocessableEntity, 42204, "Invalid mode")
		return
	}
	if err := s.machine.SetMode(request.Context(), operationID(request), domain.Scope{Subject: subject}, body.Mode); err != nil {
		writeDomainError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]domain.Mode{"mode": body.Mode})
}

func (s *Server) deleteSubjectMode(writer http.ResponseWriter, request *http.Request) {
	subject := request.PathValue("subject")
	mode, ok := s.machine.State().SubjectMode(subject)
	if !ok {
		writeAPIError(writer, http.StatusNotFound, 40409, fmt.Sprintf("Subject '%s' does not have subject-level mode configured", subject))
		return
	}
	if err := s.machine.DeleteMode(request.Context(), operationID(request), subject); err != nil {
		writeDomainError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]domain.Mode{"mode": mode})
}

func (s *Server) compatibility(writer http.ResponseWriter, request *http.Request) {
	_, candidate, ok := s.parseRequest(writer, request)
	if !ok {
		return
	}
	subject := request.PathValue("subject")
	version, ok := parseVersionPath(writer, s.machine.State(), subject, request.PathValue("version"), false)
	if !ok {
		return
	}
	engine := s.engines[candidate.Type]
	state := s.machine.State()
	level := state.EffectiveCompatibility(subject)
	_, previousSchema, found := state.Lookup(subject, version, false)
	if !found {
		writeAPIError(writer, http.StatusNotFound, 40402, "Version not found")
		return
	}
	previous, parseErr := engine.Parse(request.Context(), formats.ParseRequest{
		Definition: previousSchema.Definition, References: previousSchema.References, Limits: formats.DefaultLimits(),
	}, stateResolver{state})
	if parseErr != nil {
		writeAPIError(writer, http.StatusInternalServerError, 50001, parseErr.Error())
		return
	}
	compatible, messages, err := engine.Compatible(request.Context(), level, candidate, []formats.Parsed{previous})
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, 50001, err.Error())
		return
	}
	response := map[string]any{"is_compatible": compatible}
	if queryBool(request, "verbose") {
		response["messages"] = append([]string{}, messages...)
	}
	writeJSON(writer, http.StatusOK, response)
}

func (s *Server) lookupVersion(writer http.ResponseWriter, request *http.Request, includeDeleted bool) (domain.SubjectVersion, domain.Schema, bool) {
	subject := request.PathValue("subject")
	version, ok := parseVersionPath(writer, s.machine.State(), subject, request.PathValue("version"), includeDeleted)
	if !ok {
		return domain.SubjectVersion{}, domain.Schema{}, false
	}
	item, schema, found := s.machine.State().Lookup(subject, version, includeDeleted)
	if !found {
		writeAPIError(writer, http.StatusNotFound, 40402, fmt.Sprintf("Version %d not found", version))
		return domain.SubjectVersion{}, domain.Schema{}, false
	}
	return item, schema, true
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
		requestID := operationID(request)
		request.Header.Set("X-Request-ID", string(requestID))
		writer.Header().Set("X-Request-ID", string(requestID))
		next.ServeHTTP(writer, request)
	})
}

func (s *Server) healthLive(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "live"})
}

func (s *Server) healthReady(writer http.ResponseWriter, _ *http.Request) {
	if !s.ready() {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
}

type stateResolver struct{ state domain.State }

func (r stateResolver) Resolve(_ context.Context, reference domain.Reference) (domain.Schema, error) {
	_, schema, ok := r.state.Lookup(reference.Subject, reference.Version, false)
	if !ok {
		return domain.Schema{}, errors.New("referenced schema not found")
	}
	return schema, nil
}

func responseFor(item domain.SubjectVersion, schema domain.Schema) schemaResponse {
	return schemaResponse{
		Subject: item.Subject, Version: item.Version, ID: item.SchemaID, GUID: schema.GUID,
		SchemaType: schema.Type, References: schema.References, Schema: schema.Definition,
		Timestamp: item.Timestamp, Deleted: boolPointer(item.Deleted),
	}
}

func responseForRegistration(item domain.SubjectVersion, schema domain.Schema) schemaResponse {
	response := responseForLookup(item, schema)
	response.Subject = ""
	return response
}

func responseForLookup(item domain.SubjectVersion, schema domain.Schema) schemaResponse {
	response := responseFor(item, schema)
	response.Timestamp = 0
	response.Deleted = nil
	return response
}

func boolPointer(value bool) *bool { return &value }

func parseVersionPath(writer http.ResponseWriter, state domain.State, subject, value string, includeDeleted bool) (domain.Version, bool) {
	if value == "latest" {
		versions := state.Versions(subject, includeDeleted)
		if len(versions) == 0 {
			writeAPIError(writer, http.StatusNotFound, 40401, fmt.Sprintf("Subject '%s' not found.", subject))
			return 0, false
		}
		return versions[len(versions)-1], true
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 1 {
		writeAPIError(writer, http.StatusUnprocessableEntity, 42202, "Invalid version")
		return 0, false
	}
	return domain.Version(parsed), true
}

func parseID(writer http.ResponseWriter, value string) (domain.SchemaID, bool) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 1 {
		writeAPIError(writer, http.StatusNotFound, 40403, "Schema not found")
		return 0, false
	}
	return domain.SchemaID(parsed), true
}

func decodeJSON(reader io.Reader, destination any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("request must contain one JSON value")
	}
	return nil
}

func queryBool(request *http.Request, key string) bool {
	value, _ := strconv.ParseBool(request.URL.Query().Get(key))
	return value
}

func operationID(request *http.Request) domain.OperationID {
	if value := request.Header.Get("X-Request-ID"); value != "" {
		return domain.OperationID(value)
	}
	var value [16]byte
	if _, err := rand.Read(value[:]); err == nil {
		return domain.OperationID(hex.EncodeToString(value[:]))
	}
	return domain.OperationID(strings.ReplaceAll(request.RemoteAddr, ":", "-"))
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", vendorContentType)
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return
	}
}

func writeDomainError(writer http.ResponseWriter, err error) {
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) {
		writeAPIError(writer, http.StatusInternalServerError, 50001, "Internal server error")
		return
	}
	switch domainErr.Category {
	case domain.ErrorInvalid:
		writeAPIError(writer, http.StatusUnprocessableEntity, 42201, domainErr.Error())
	case domain.ErrorNotFound:
		code := 40401
		if domainErr.Resource == "version" || domainErr.Resource == "reference" {
			code = 40402
		}
		writeAPIError(writer, http.StatusNotFound, code, domainErr.Error())
	case domain.ErrorConflict, domain.ErrorIncompatible:
		writeAPIError(writer, http.StatusConflict, 409, domainErr.Error())
	case domain.ErrorReadOnly:
		writeAPIError(writer, http.StatusUnprocessableEntity, 42205, domainErr.Error())
	case domain.ErrorStorage, domain.ErrorCorrupt:
		writeAPIError(writer, http.StatusInternalServerError, 50001, domainErr.Error())
	default:
		writeAPIError(writer, http.StatusInternalServerError, 50001, domainErr.Error())
	}
}

func writeAPIError(writer http.ResponseWriter, status, code int, message string) {
	writeJSON(writer, status, map[string]any{"error_code": code, "message": message})
}
