// Package jsonschema implements Confluent JSON Schema handling.
package jsonschema

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
	validator "github.com/santhosh-tekuri/jsonschema/v6"
)

const rootURL = "schema.json"

// Engine validates, identifies, resolves, and compares JSON Schemas.
type Engine struct{}

func (Engine) Type() domain.SchemaType { return domain.SchemaTypeJSON }

func (Engine) Parse(ctx context.Context, request formats.ParseRequest, resolver formats.Resolver) (formats.Parsed, error) {
	limits := normalizeLimits(request.Limits)
	if request.Definition == "" {
		return formats.Parsed{}, formatError("invalid_json_schema", "schema is empty", nil)
	}
	if len(request.Definition) > limits.MaxSchemaBytes {
		return formats.Parsed{}, formatError("schema_too_large", fmt.Sprintf("schema exceeds %d bytes", limits.MaxSchemaBytes), nil)
	}
	root, err := decodeDocument(request.Definition)
	if err != nil {
		return formats.Parsed{}, formatError("invalid_json_schema", err.Error(), err)
	}
	resources := map[string]any{rootURL: root}
	loader := referenceLoader{resolver: resolver, limits: limits, resources: resources, active: make(map[string]bool)}
	for _, reference := range request.References {
		if err := loader.load(ctx, reference, 1); err != nil {
			return formats.Parsed{}, err
		}
	}
	compiler := validator.NewCompiler()
	compiler.DefaultDraft(validator.Draft7)
	for resourceURL, document := range resources {
		if err := compiler.AddResource(resourceURL, document); err != nil {
			return formats.Parsed{}, formatError("invalid_json_schema", err.Error(), err)
		}
	}
	compiled, err := compiler.Compile(rootURL)
	if err != nil {
		return formats.Parsed{}, formatError("invalid_json_schema", err.Error(), err)
	}
	compact, err := compactJSON(request.Definition)
	if err != nil {
		return formats.Parsed{}, formatError("invalid_json_schema", err.Error(), err)
	}
	normalized, err := sortedJSON(root)
	if err != nil {
		return formats.Parsed{}, formatError("invalid_json_schema", err.Error(), err)
	}
	definition := compact
	if request.Normalize {
		definition = normalized
	}
	identity, err := schemaIdentity(definition, request.References)
	if err != nil {
		return formats.Parsed{}, err
	}
	return formats.Parsed{
		Type: domain.SchemaTypeJSON, Definition: definition, Canonical: normalized,
		Identity: identity, References: append([]domain.Reference(nil), request.References...),
		Native: &nativeSchema{Document: root, Compiled: compiled},
	}, nil
}

func (Engine) Compatible(ctx context.Context, level domain.CompatibilityLevel, candidate formats.Parsed, previous []formats.Parsed) (bool, []string, error) {
	if !level.Valid() {
		return false, nil, formatError("invalid_compatibility", fmt.Sprintf("unsupported level %q", level), nil)
	}
	if level == domain.CompatibilityNone {
		return true, nil, nil
	}
	candidateNative, ok := candidate.Native.(*nativeSchema)
	if !ok {
		return false, nil, formatError("invalid_compatibility", "candidate is not parsed JSON Schema", nil)
	}
	var messages []string
	for index, item := range previous {
		if err := ctx.Err(); err != nil {
			return false, nil, err
		}
		previousNative, previousOK := item.Native.(*nativeSchema)
		if !previousOK {
			return false, nil, formatError("invalid_compatibility", "previous schema is not parsed JSON Schema", nil)
		}
		switch level {
		case domain.CompatibilityNone:
			continue
		case domain.CompatibilityBackward, domain.CompatibilityBackwardTransitive:
			messages = append(messages, compareDocuments(candidateNative.Document, previousNative.Document, index, "$")...)
		case domain.CompatibilityForward, domain.CompatibilityForwardTransitive:
			messages = append(messages, compareDocuments(previousNative.Document, candidateNative.Document, index, "$")...)
		case domain.CompatibilityFull, domain.CompatibilityFullTransitive:
			messages = append(messages, compareDocuments(candidateNative.Document, previousNative.Document, index, "$")...)
			messages = append(messages, compareDocuments(previousNative.Document, candidateNative.Document, index, "$")...)
		}
	}
	return len(messages) == 0, messages, nil
}

type nativeSchema struct {
	Document any
	Compiled *validator.Schema
}

type referenceLoader struct {
	resolver  formats.Resolver
	limits    formats.Limits
	resources map[string]any
	active    map[string]bool
	count     int
}

func (l *referenceLoader) load(ctx context.Context, reference domain.Reference, depth int) error {
	if l.resolver == nil {
		return formatError("missing_reference", reference.Subject, nil)
	}
	if depth > l.limits.MaxReferenceDepth {
		return formatError("reference_depth", fmt.Sprintf("reference depth exceeds %d", l.limits.MaxReferenceDepth), nil)
	}
	l.count++
	if l.count > l.limits.MaxReferences {
		return formatError("too_many_references", fmt.Sprintf("resolved reference count exceeds %d", l.limits.MaxReferences), nil)
	}
	key := fmt.Sprintf("%s/%d", reference.Subject, reference.Version)
	if l.active[key] {
		return formatError("reference_cycle", "cycle at "+key, nil)
	}
	l.active[key] = true
	defer delete(l.active, key)
	schema, err := l.resolver.Resolve(ctx, reference)
	if err != nil {
		return formatError("missing_reference", key, err)
	}
	if schema.Type != domain.SchemaTypeJSON {
		return formatError("reference_type", fmt.Sprintf("%s is %s, expected JSON", key, schema.Type), nil)
	}
	if len(schema.Definition) > l.limits.MaxSchemaBytes {
		return formatError("schema_too_large", key, nil)
	}
	for _, nested := range schema.References {
		if err := l.load(ctx, nested, depth+1); err != nil {
			return err
		}
	}
	document, err := decodeDocument(schema.Definition)
	if err != nil {
		return formatError("invalid_reference", key, err)
	}
	l.resources[reference.Name] = document
	return nil
}

func compareDocuments(reader, writer any, previousIndex int, path string) []string {
	readerObject, readerOK := reader.(map[string]any)
	writerObject, writerOK := writer.(map[string]any)
	if !readerOK || !writerOK {
		if fmt.Sprint(reader) != fmt.Sprint(writer) {
			return []string{fmt.Sprintf("previous schema %d: %s changed", previousIndex+1, path)}
		}
		return nil
	}
	if !typeSuperset(readerObject["type"], writerObject["type"]) {
		return []string{fmt.Sprintf("previous schema %d: %s narrows type", previousIndex+1, path)}
	}
	var messages []string
	readerRequired := stringSet(readerObject["required"])
	writerRequired := stringSet(writerObject["required"])
	for name := range readerRequired {
		if !writerRequired[name] {
			messages = append(messages, fmt.Sprintf("previous schema %d: %s requires new property %s", previousIndex+1, path, name))
		}
	}
	readerProperties, _ := readerObject["properties"].(map[string]any)
	writerProperties, _ := writerObject["properties"].(map[string]any)
	writerOpen := true
	if additional, ok := writerObject["additionalProperties"].(bool); ok {
		writerOpen = additional
	}
	for name := range readerProperties {
		if _, exists := writerProperties[name]; !exists && writerOpen {
			messages = append(messages, fmt.Sprintf("previous schema %d: %s adds property %s to an open content model", previousIndex+1, path, name))
		}
	}
	for name, writerProperty := range writerProperties {
		if readerProperty, exists := readerProperties[name]; exists {
			messages = append(messages, compareDocuments(readerProperty, writerProperty, previousIndex, path+"/properties/"+name)...)
		}
	}
	if readerAdditional, ok := readerObject["additionalProperties"].(bool); ok && !readerAdditional {
		for name := range writerProperties {
			if _, exists := readerProperties[name]; !exists {
				messages = append(messages, fmt.Sprintf("previous schema %d: %s writer adds property %s to closed reader", previousIndex+1, path, name))
			}
		}
		if writerAdditional, writerHas := writerObject["additionalProperties"].(bool); !writerHas || writerAdditional {
			messages = append(messages, fmt.Sprintf("previous schema %d: %s closes an open object", previousIndex+1, path))
		}
	}
	return messages
}

func typeSuperset(reader, writer any) bool {
	if reader == nil || writer == nil {
		return true
	}
	readerTypes := stringSet(reader)
	writerTypes := stringSet(writer)
	for item := range writerTypes {
		if !readerTypes[item] {
			return false
		}
	}
	return true
}

func stringSet(value any) map[string]bool {
	result := make(map[string]bool)
	switch typed := value.(type) {
	case string:
		result[typed] = true
	case []any:
		for _, item := range typed {
			if text, ok := item.(string); ok {
				result[text] = true
			}
		}
	}
	return result
}

func decodeDocument(definition string) (any, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(definition))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if _, ok := document.(map[string]any); !ok {
		if _, booleanSchema := document.(bool); !booleanSchema {
			return nil, fmt.Errorf("schema must be an object or boolean")
		}
	}
	return document, nil
}

func compactJSON(definition string) (string, error) {
	var output bytes.Buffer
	if err := json.Compact(&output, []byte(definition)); err != nil {
		return "", err
	}
	return output.String(), nil
}

func sortedJSON(value any) (string, error) {
	var output bytes.Buffer
	if err := writeSortedJSON(&output, value); err != nil {
		return "", err
	}
	return output.String(), nil
}

func writeSortedJSON(output *bytes.Buffer, value any) error {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		output.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				output.WriteByte(',')
			}
			encodedKey, _ := json.Marshal(key)
			output.Write(encodedKey)
			output.WriteByte(':')
			if err := writeSortedJSON(output, typed[key]); err != nil {
				return err
			}
		}
		output.WriteByte('}')
	case []any:
		output.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				output.WriteByte(',')
			}
			if err := writeSortedJSON(output, item); err != nil {
				return err
			}
		}
		output.WriteByte(']')
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return err
		}
		output.Write(encoded)
	}
	return nil
}

func normalizeLimits(limits formats.Limits) formats.Limits {
	defaults := formats.DefaultLimits()
	if limits.MaxSchemaBytes <= 0 {
		limits.MaxSchemaBytes = defaults.MaxSchemaBytes
	}
	if limits.MaxReferences <= 0 {
		limits.MaxReferences = defaults.MaxReferences
	}
	if limits.MaxReferenceDepth <= 0 {
		limits.MaxReferenceDepth = defaults.MaxReferenceDepth
	}
	return limits
}

func schemaIdentity(canonical string, references []domain.Reference) (string, error) {
	encodedReferences, err := json.Marshal(references)
	if err != nil {
		return "", formatError("identity", err.Error(), err)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain.SchemaTypeJSON))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(canonical))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(encodedReferences)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func formatError(kind, detail string, cause error) error {
	return &formats.Error{Kind: kind, Detail: detail, Cause: cause}
}
