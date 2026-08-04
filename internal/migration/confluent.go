// Package migration imports registry state from externally observable APIs.
package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/k3rnL/riquet/internal/backup"
	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
	"github.com/k3rnL/riquet/internal/formats/avro"
	jsonschemaformat "github.com/k3rnL/riquet/internal/formats/jsonschema"
	protobufformat "github.com/k3rnL/riquet/internal/formats/protobuf"
)

const maxResponseBytes = 64 << 20

// ConfluentOptions configures a read-only Confluent REST API export.
type ConfluentOptions struct {
	BaseURL       string
	HTTPClient    *http.Client
	BasicUsername string
	BasicPassword string
	BearerToken   string
	Now           func() time.Time
}

// UnsupportedFeature identifies source state that Riquet v1 cannot preserve.
type UnsupportedFeature struct {
	Feature  string `json:"feature"`
	Resource string `json:"resource"`
	Detail   string `json:"detail"`
}

// Report summarizes an export and all pre-cutover findings.
type Report struct {
	Source              string               `json:"source"`
	Subjects            int                  `json:"subjects"`
	Versions            int                  `json:"versions"`
	SoftDeletedVersions int                  `json:"softDeletedVersions"`
	Unsupported         []UnsupportedFeature `json:"unsupported,omitempty"`
	Warnings            []string             `json:"warnings,omitempty"`
}

// UnsupportedFeaturesError means no snapshot was emitted because the source
// uses features outside Riquet's v1 compatibility contract.
type UnsupportedFeaturesError struct{ Features []UnsupportedFeature }

func (e *UnsupportedFeaturesError) Error() string {
	return fmt.Sprintf("source uses %d unsupported feature(s)", len(e.Features))
}

// ExportConfluent reads the default Confluent registry context and emits one
// checksummed Riquet logical snapshot only after the complete source validates.
func ExportConfluent(ctx context.Context, destination io.Writer, options ConfluentOptions) (Report, error) {
	if destination == nil {
		return Report{}, errors.New("export destination is required")
	}
	client, err := newConfluentClient(options)
	if err != nil {
		return Report{}, err
	}
	inventory, report, err := client.inventory(ctx)
	if err != nil {
		return report, err
	}
	if len(report.Unsupported) > 0 {
		return report, &UnsupportedFeaturesError{Features: slices.Clone(report.Unsupported)}
	}
	state, err := buildState(ctx, inventory)
	if err != nil {
		return report, fmt.Errorf("build logical registry state: %w", err)
	}
	now := time.Now
	if options.Now != nil {
		now = options.Now
	}
	var encoded bytes.Buffer
	if err := backup.Export(&encoded, state, "confluent-api:"+client.base.Redacted(), now()); err != nil {
		return report, err
	}
	if _, err := io.Copy(destination, &encoded); err != nil {
		return report, fmt.Errorf("write logical snapshot: %w", err)
	}
	return report, nil
}

type sourceSchema struct {
	Subject    string
	Version    domain.Version
	ID         domain.SchemaID
	SchemaType domain.SchemaType
	References []domain.Reference
	Schema     string
	Timestamp  int64
	Deleted    bool
	Metadata   json.RawMessage
	RuleSet    json.RawMessage
	SchemaTags json.RawMessage
}

type sourceConfig struct {
	CompatibilityLevel domain.CompatibilityLevel `json:"compatibilityLevel"`
	Compatibility      domain.CompatibilityLevel `json:"compatibility"`
	Alias              string                    `json:"alias"`
	Normalize          *bool                     `json:"normalize"`
	CompatibilityGroup string                    `json:"compatibilityGroup"`
	DefaultMetadata    json.RawMessage           `json:"defaultMetadata"`
	OverrideMetadata   json.RawMessage           `json:"overrideMetadata"`
	DefaultRuleSet     json.RawMessage           `json:"defaultRuleSet"`
	OverrideRuleSet    json.RawMessage           `json:"overrideRuleSet"`
}

func (c sourceConfig) level() domain.CompatibilityLevel {
	if c.CompatibilityLevel != "" {
		return c.CompatibilityLevel
	}
	return c.Compatibility
}

type inventory struct {
	schemas              []sourceSchema
	globalCompatibility  domain.CompatibilityLevel
	subjectCompatibility map[string]domain.CompatibilityLevel
	globalMode           domain.Mode
	subjectModes         map[string]domain.Mode
}

type confluentClient struct {
	base        *url.URL
	http        *http.Client
	username    string
	password    string
	bearerToken string
}

func newConfluentClient(options ConfluentOptions) (*confluentClient, error) {
	base, err := url.Parse(options.BaseURL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, errors.New("confluent source must be an absolute HTTP(S) URL")
	}
	if base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("confluent source URL cannot contain a query or fragment")
	}
	if base.User != nil {
		return nil, errors.New("confluent source URL cannot contain credentials; use explicit authentication options")
	}
	if options.BearerToken != "" && (options.BasicUsername != "" || options.BasicPassword != "") {
		return nil, errors.New("choose either Basic or bearer authentication")
	}
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &confluentClient{
		base: base, http: httpClient, username: options.BasicUsername,
		password: options.BasicPassword, bearerToken: options.BearerToken,
	}, nil
}

func (c *confluentClient) inventory(ctx context.Context) (inventory, Report, error) {
	report := Report{Source: c.base.Redacted()}
	var subjects []string
	if _, err := c.get(ctx, []string{"subjects"}, url.Values{"deleted": {"true"}, "subjectPrefix": {""}}, &subjects, false); err != nil {
		return inventory{}, report, err
	}
	sort.Strings(subjects)
	subjects = slices.Compact(subjects)
	report.Subjects = len(subjects)

	var contexts []string
	if found, err := c.get(ctx, []string{"contexts"}, nil, &contexts, true); err != nil {
		return inventory{}, report, err
	} else if found {
		for _, registryContext := range contexts {
			if registryContext != ":." && registryContext != "." && registryContext != "" {
				report.Unsupported = append(report.Unsupported, UnsupportedFeature{
					Feature: "context", Resource: registryContext, Detail: "Riquet v1 exports only the default context",
				})
			}
		}
	}

	var globalConfig sourceConfig
	if _, err := c.get(ctx, []string{"config"}, nil, &globalConfig, false); err != nil {
		return inventory{}, report, err
	}
	checkConfigFeatures(&report, "global config", globalConfig)
	globalLevel := globalConfig.level()
	if !globalLevel.Valid() {
		return inventory{}, report, fmt.Errorf("source global compatibility level %q is unsupported", globalLevel)
	}

	globalMode := domain.ModeReadWrite
	var modeResponse struct {
		Mode domain.Mode `json:"mode"`
	}
	if found, err := c.get(ctx, []string{"mode"}, nil, &modeResponse, true); err != nil {
		return inventory{}, report, err
	} else if found {
		globalMode = modeResponse.Mode
	}
	if !globalMode.Valid() {
		report.Unsupported = append(report.Unsupported, UnsupportedFeature{
			Feature: "mode", Resource: "global mode", Detail: fmt.Sprintf("mode %q is not supported by Riquet v1", globalMode),
		})
	}
	if globalMode != domain.ModeReadOnly {
		report.Warnings = append(report.Warnings, "source global mode is not READONLY; freeze writes and repeat the export before cutover")
	}

	result := inventory{
		globalCompatibility: globalLevel, globalMode: globalMode,
		subjectCompatibility: make(map[string]domain.CompatibilityLevel),
		subjectModes:         make(map[string]domain.Mode),
	}
	for _, subject := range subjects {
		active, all, err := c.versions(ctx, subject)
		if err != nil {
			return inventory{}, report, err
		}
		activeSet := make(map[domain.Version]bool, len(active))
		for _, version := range active {
			activeSet[version] = true
		}
		for _, version := range all {
			var response struct {
				Subject    string             `json:"subject"`
				Version    domain.Version     `json:"version"`
				ID         domain.SchemaID    `json:"id"`
				SchemaType domain.SchemaType  `json:"schemaType"`
				References []domain.Reference `json:"references"`
				Schema     string             `json:"schema"`
				Timestamp  int64              `json:"ts"`
				Deleted    *bool              `json:"deleted"`
				Metadata   json.RawMessage    `json:"metadata"`
				RuleSet    json.RawMessage    `json:"ruleSet"`
				SchemaTags json.RawMessage    `json:"schemaTags"`
			}
			if _, err := c.get(ctx, []string{"subjects", subject, "versions", fmt.Sprint(version)}, url.Values{"deleted": {"true"}}, &response, false); err != nil {
				return inventory{}, report, err
			}
			if response.Subject == "" {
				response.Subject = subject
			}
			if response.Version == 0 {
				response.Version = version
			}
			if response.SchemaType == "" {
				response.SchemaType = domain.SchemaTypeAvro
			}
			deleted := !activeSet[version]
			if response.Deleted != nil {
				deleted = *response.Deleted
			}
			schema := sourceSchema{
				Subject: response.Subject, Version: response.Version, ID: response.ID,
				SchemaType: response.SchemaType, References: response.References,
				Schema: response.Schema, Timestamp: response.Timestamp, Deleted: deleted,
				Metadata: response.Metadata, RuleSet: response.RuleSet, SchemaTags: response.SchemaTags,
			}
			checkSchemaFeatures(&report, schema)
			result.schemas = append(result.schemas, schema)
			report.Versions++
			if deleted {
				report.SoftDeletedVersions++
			}
		}

		var subjectConfig sourceConfig
		if found, err := c.get(ctx, []string{"config", subject}, url.Values{"defaultToGlobal": {"false"}}, &subjectConfig, true); err != nil {
			return inventory{}, report, err
		} else if found {
			checkConfigFeatures(&report, "subject "+subject+" config", subjectConfig)
			level := subjectConfig.level()
			if !level.Valid() {
				return inventory{}, report, fmt.Errorf("source subject %q compatibility level %q is unsupported", subject, level)
			}
			result.subjectCompatibility[subject] = level
		}

		modeResponse.Mode = ""
		if found, err := c.get(ctx, []string{"mode", subject}, url.Values{"defaultToGlobal": {"false"}}, &modeResponse, true); err != nil {
			return inventory{}, report, err
		} else if found {
			if !modeResponse.Mode.Valid() {
				report.Unsupported = append(report.Unsupported, UnsupportedFeature{
					Feature: "mode", Resource: "subject " + subject, Detail: fmt.Sprintf("mode %q is not supported by Riquet v1", modeResponse.Mode),
				})
			} else {
				result.subjectModes[subject] = modeResponse.Mode
			}
		}
	}

	// A second listing catches additions, deletions, and version churn during the
	// multi-request export. Existing schema versions themselves are immutable.
	var finalSubjects []string
	if _, err := c.get(ctx, []string{"subjects"}, url.Values{"deleted": {"true"}, "subjectPrefix": {""}}, &finalSubjects, false); err != nil {
		return inventory{}, report, err
	}
	sort.Strings(finalSubjects)
	finalSubjects = slices.Compact(finalSubjects)
	if !slices.Equal(subjects, finalSubjects) {
		return inventory{}, report, errors.New("source changed during export: subject listing differs; freeze writes and retry")
	}
	for _, subject := range subjects {
		_, finalVersions, err := c.versions(ctx, subject)
		if err != nil {
			return inventory{}, report, err
		}
		var expected []domain.Version
		for _, schema := range result.schemas {
			if schema.Subject == subject {
				expected = append(expected, schema.Version)
			}
		}
		if !slices.Equal(expected, finalVersions) {
			return inventory{}, report, fmt.Errorf("source changed during export: versions for subject %q differ; freeze writes and retry", subject)
		}
	}
	report.Warnings = append(report.Warnings, "permanently deleted schema associations are not observable through the Confluent API and are not included")
	return result, report, nil
}

func (c *confluentClient) versions(ctx context.Context, subject string) ([]domain.Version, []domain.Version, error) {
	var active []domain.Version
	if _, err := c.get(ctx, []string{"subjects", subject, "versions"}, nil, &active, true); err != nil {
		return nil, nil, err
	}
	var all []domain.Version
	if _, err := c.get(ctx, []string{"subjects", subject, "versions"}, url.Values{"deleted": {"true"}}, &all, false); err != nil {
		return nil, nil, err
	}
	sort.Slice(active, func(i, j int) bool { return active[i] < active[j] })
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return slices.Compact(active), slices.Compact(all), nil
}

func (c *confluentClient) get(ctx context.Context, segments []string, query url.Values, destination any, allowNotFound bool) (bool, error) {
	endpoint := *c.base
	rawPath := strings.TrimSuffix(c.base.EscapedPath(), "/")
	for _, segment := range segments {
		rawPath += "/" + url.PathEscape(segment)
	}
	path, err := url.PathUnescape(rawPath)
	if err != nil {
		return false, err
	}
	endpoint.Path, endpoint.RawPath, endpoint.RawQuery = path, rawPath, query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return false, err
	}
	request.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if c.bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+c.bearerToken)
	} else if c.username != "" || c.password != "" {
		request.SetBasicAuth(c.username, c.password)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return false, fmt.Errorf("GET %s: %w", endpoint.Redacted(), err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusNotFound && allowNotFound {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 16<<10))
		return false, nil
	}
	limited := io.LimitReader(response.Body, maxResponseBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return false, fmt.Errorf("read GET %s: %w", endpoint.Redacted(), err)
	}
	if len(body) > maxResponseBytes {
		return false, fmt.Errorf("GET %s response exceeds %d bytes", endpoint.Redacted(), maxResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return false, fmt.Errorf("GET %s returned %s: %s", endpoint.Redacted(), response.Status, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, destination); err != nil {
		return false, fmt.Errorf("decode GET %s: %w", endpoint.Redacted(), err)
	}
	return true, nil
}

func checkSchemaFeatures(report *Report, schema sourceSchema) {
	resource := fmt.Sprintf("%s/%d", schema.Subject, schema.Version)
	for _, item := range []struct {
		name string
		raw  json.RawMessage
	}{{"metadata", schema.Metadata}, {"ruleSet", schema.RuleSet}, {"schemaTags", schema.SchemaTags}} {
		if !emptyJSON(item.raw) {
			report.Unsupported = append(report.Unsupported, UnsupportedFeature{
				Feature: item.name, Resource: resource, Detail: "schema governance data cannot be preserved by Riquet v1",
			})
		}
	}
}

func checkConfigFeatures(report *Report, resource string, config sourceConfig) {
	if config.Alias != "" {
		report.Unsupported = append(report.Unsupported, UnsupportedFeature{Feature: "subject alias", Resource: resource, Detail: config.Alias})
	}
	if config.Normalize != nil && *config.Normalize {
		report.Unsupported = append(report.Unsupported, UnsupportedFeature{Feature: "configured normalization", Resource: resource, Detail: "normalize=true"})
	}
	if config.CompatibilityGroup != "" {
		report.Unsupported = append(report.Unsupported, UnsupportedFeature{Feature: "compatibility group", Resource: resource, Detail: config.CompatibilityGroup})
	}
	for _, item := range []struct {
		name string
		raw  json.RawMessage
	}{{"default metadata", config.DefaultMetadata}, {"override metadata", config.OverrideMetadata}, {"default rule set", config.DefaultRuleSet}, {"override rule set", config.OverrideRuleSet}} {
		if !emptyJSON(item.raw) {
			report.Unsupported = append(report.Unsupported, UnsupportedFeature{Feature: item.name, Resource: resource, Detail: "configuration cannot be preserved by Riquet v1"})
		}
	}
}

func emptyJSON(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed == "" || trimmed == "null" || trimmed == "{}" || trimmed == "[]"
}

func buildState(ctx context.Context, source inventory) (domain.State, error) {
	machine := domain.NewMachine(domain.NewState(), nil, nil)
	operation := 0
	nextOperation := func(kind string) domain.OperationID {
		operation++
		return domain.OperationID(fmt.Sprintf("confluent-export-%s-%06d", kind, operation))
	}
	if err := machine.SetMode(ctx, nextOperation("mode"), domain.Scope{}, domain.ModeImport); err != nil {
		return domain.State{}, err
	}

	engines := map[domain.SchemaType]formats.Engine{
		domain.SchemaTypeAvro: avro.Engine{}, domain.SchemaTypeProtobuf: protobufformat.Engine{}, domain.SchemaTypeJSON: jsonschemaformat.Engine{},
	}
	pending := slices.Clone(source.schemas)
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].Subject == pending[j].Subject {
			return pending[i].Version < pending[j].Version
		}
		return pending[i].Subject < pending[j].Subject
	})
	for len(pending) > 0 {
		progress := false
		remaining := pending[:0]
		for _, item := range pending {
			if !referencesAvailable(machine.State(), item.References) {
				remaining = append(remaining, item)
				continue
			}
			if item.ID < 1 || item.Version < 1 || item.Subject == "" || item.Schema == "" {
				return domain.State{}, fmt.Errorf("invalid source schema %q/%d (id %d)", item.Subject, item.Version, item.ID)
			}
			engine := engines[item.SchemaType]
			if engine == nil {
				return domain.State{}, fmt.Errorf("unsupported schema type %q at %s/%d", item.SchemaType, item.Subject, item.Version)
			}
			state := machine.State()
			parsed, err := engine.Parse(ctx, formats.ParseRequest{
				Definition: item.Schema, References: item.References, Limits: formats.DefaultLimits(),
			}, stateResolver{state: state})
			if err != nil {
				return domain.State{}, fmt.Errorf("parse %s/%d: %w", item.Subject, item.Version, err)
			}
			if _, err := machine.Register(ctx, domain.RegisterCommand{
				OperationID: nextOperation("schema"), Subject: item.Subject,
				Identity: parsed.Identity, Type: parsed.Type, Definition: parsed.Definition,
				References: parsed.References, RequestedID: item.ID, RequestedVersion: item.Version,
				Timestamp: item.Timestamp,
			}); err != nil {
				return domain.State{}, fmt.Errorf("import %s/%d: %w", item.Subject, item.Version, err)
			}
			progress = true
		}
		pending = remaining
		if !progress {
			blocked := make([]string, 0, len(pending))
			for _, item := range pending {
				blocked = append(blocked, fmt.Sprintf("%s/%d", item.Subject, item.Version))
			}
			return domain.State{}, fmt.Errorf("unresolved or cyclic schema references block: %s", strings.Join(blocked, ", "))
		}
	}

	for _, item := range source.schemas {
		if !item.Deleted {
			continue
		}
		if _, err := machine.DeleteVersion(ctx, domain.DeleteVersionCommand{
			OperationID: nextOperation("delete"), Subject: item.Subject, Version: item.Version,
		}); err != nil {
			return domain.State{}, fmt.Errorf("preserve soft deletion %s/%d: %w", item.Subject, item.Version, err)
		}
	}
	if err := machine.SetCompatibility(ctx, nextOperation("config"), domain.Scope{}, source.globalCompatibility); err != nil {
		return domain.State{}, err
	}
	for _, subject := range sortedKeys(source.subjectCompatibility) {
		if err := machine.SetCompatibility(ctx, nextOperation("config"), domain.Scope{Subject: subject}, source.subjectCompatibility[subject]); err != nil {
			return domain.State{}, err
		}
	}
	for _, subject := range sortedKeys(source.subjectModes) {
		if err := machine.SetMode(ctx, nextOperation("mode"), domain.Scope{Subject: subject}, source.subjectModes[subject]); err != nil {
			return domain.State{}, err
		}
	}
	if err := machine.SetMode(ctx, nextOperation("mode"), domain.Scope{}, source.globalMode); err != nil {
		return domain.State{}, err
	}
	return machine.State(), nil
}

func referencesAvailable(state domain.State, references []domain.Reference) bool {
	for _, reference := range references {
		if _, _, ok := state.Lookup(reference.Subject, reference.Version, false); !ok {
			return false
		}
	}
	return true
}

type stateResolver struct{ state domain.State }

func (r stateResolver) Resolve(_ context.Context, reference domain.Reference) (domain.Schema, error) {
	_, schema, ok := r.state.Lookup(reference.Subject, reference.Version, false)
	if !ok {
		return domain.Schema{}, errors.New("referenced schema not found")
	}
	return schema, nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
