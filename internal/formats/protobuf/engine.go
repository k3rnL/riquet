// Package protobuf implements Confluent Protocol Buffer schema handling.
package protobuf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/jhump/protoreflect/v2/protoprint"
	"github.com/k3rnL/riquet/internal/domain"
	"github.com/k3rnL/riquet/internal/formats"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const rootFilename = "schema.proto"

// Engine validates, identifies, resolves, and compares Protobuf schemas.
type Engine struct{}

func (Engine) Type() domain.SchemaType { return domain.SchemaTypeProtobuf }

func (Engine) Parse(ctx context.Context, request formats.ParseRequest, resolver formats.Resolver) (formats.Parsed, error) {
	limits := normalizeLimits(request.Limits)
	if request.Definition == "" {
		return formats.Parsed{}, formatError("invalid_protobuf", "schema is empty", nil)
	}
	if len(request.Definition) > limits.MaxSchemaBytes {
		return formats.Parsed{}, formatError("schema_too_large", fmt.Sprintf("schema exceeds %d bytes", limits.MaxSchemaBytes), nil)
	}
	if len(request.References) > limits.MaxReferences {
		return formats.Parsed{}, formatError("too_many_references", fmt.Sprintf("reference count exceeds %d", limits.MaxReferences), nil)
	}
	sources := map[string]string{rootFilename: request.Definition}
	state := referenceLoader{resolver: resolver, limits: limits, sources: sources, active: make(map[string]bool)}
	for _, reference := range request.References {
		if err := state.load(ctx, reference, 1); err != nil {
			return formats.Parsed{}, err
		}
	}
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(sources),
		}),
		MaxParallelism: 1,
	}
	files, err := compiler.Compile(ctx, rootFilename)
	if err != nil {
		return formats.Parsed{}, formatError("invalid_protobuf", err.Error(), err)
	}
	printer := protoprint.Printer{OmitComments: protoprint.CommentsAll, SortElements: request.Normalize}
	canonical, err := printer.PrintProtoToString(files[0])
	if err != nil {
		return formats.Parsed{}, formatError("invalid_protobuf", err.Error(), err)
	}
	canonical = strings.Replace(canonical, ";\n\npackage ", ";\npackage ", 1)
	identity, err := schemaIdentity(canonical, request.References)
	if err != nil {
		return formats.Parsed{}, err
	}
	return formats.Parsed{
		Type: domain.SchemaTypeProtobuf, Definition: canonical, Canonical: canonical,
		Identity: identity, References: append([]domain.Reference(nil), request.References...), Native: files[0],
	}, nil
}

func (Engine) Compatible(ctx context.Context, level domain.CompatibilityLevel, candidate formats.Parsed, previous []formats.Parsed) (bool, []string, error) {
	if !level.Valid() {
		return false, nil, formatError("invalid_compatibility", fmt.Sprintf("unsupported level %q", level), nil)
	}
	if level == domain.CompatibilityNone {
		return true, nil, nil
	}
	candidateFile, ok := candidate.Native.(protoreflect.FileDescriptor)
	if !ok {
		return false, nil, formatError("invalid_compatibility", "candidate is not parsed Protobuf", nil)
	}
	var messages []string
	for index, item := range previous {
		if err := ctx.Err(); err != nil {
			return false, nil, err
		}
		previousFile, previousOK := item.Native.(protoreflect.FileDescriptor)
		if !previousOK {
			return false, nil, formatError("invalid_compatibility", "previous schema is not parsed Protobuf", nil)
		}
		switch level {
		case domain.CompatibilityNone:
			continue
		case domain.CompatibilityBackward, domain.CompatibilityBackwardTransitive:
			messages = append(messages, compareFiles(candidateFile, previousFile, index)...)
		case domain.CompatibilityForward, domain.CompatibilityForwardTransitive:
			messages = append(messages, compareFiles(previousFile, candidateFile, index)...)
		case domain.CompatibilityFull, domain.CompatibilityFullTransitive:
			messages = append(messages, compareFiles(candidateFile, previousFile, index)...)
			messages = append(messages, compareFiles(previousFile, candidateFile, index)...)
		}
	}
	return len(messages) == 0, messages, nil
}

type referenceLoader struct {
	resolver formats.Resolver
	limits   formats.Limits
	sources  map[string]string
	active   map[string]bool
	count    int
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
	if schema.Type != domain.SchemaTypeProtobuf {
		return formatError("reference_type", fmt.Sprintf("%s is %s, expected PROTOBUF", key, schema.Type), nil)
	}
	if len(schema.Definition) > l.limits.MaxSchemaBytes {
		return formatError("schema_too_large", key, nil)
	}
	for _, nested := range schema.References {
		if err := l.load(ctx, nested, depth+1); err != nil {
			return err
		}
	}
	l.sources[reference.Name] = schema.Definition
	return nil
}

func compareFiles(reader, writer protoreflect.FileDescriptor, previousIndex int) []string {
	var messages []string
	writers := writer.Messages()
	readers := reader.Messages()
	for index := 0; index < writers.Len(); index++ {
		writerMessage := writers.Get(index)
		readerMessage := readers.ByName(writerMessage.Name())
		if readerMessage == nil {
			continue
		}
		writerFields := writerMessage.Fields()
		readerFields := readerMessage.Fields()
		for fieldIndex := 0; fieldIndex < writerFields.Len(); fieldIndex++ {
			writerField := writerFields.Get(fieldIndex)
			readerField := readerFields.ByNumber(writerField.Number())
			if readerField == nil {
				continue
			}
			if wireClass(readerField) != wireClass(writerField) {
				messages = append(messages, fmt.Sprintf("previous schema %d: field %s number %d changed wire type", previousIndex+1, writerField.FullName(), writerField.Number()))
			}
		}
	}
	return messages
}

func wireClass(field protoreflect.FieldDescriptor) string {
	if field.IsMap() {
		return "map:" + field.MapKey().Kind().String() + ":" + field.MapValue().Kind().String()
	}
	switch field.Kind() {
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.BoolKind, protoreflect.EnumKind:
		return "varint"
	case protoreflect.Sint32Kind, protoreflect.Sint64Kind:
		return "zigzag"
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		return "fixed32-int"
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		return "fixed64-int"
	case protoreflect.FloatKind:
		return "float"
	case protoreflect.DoubleKind:
		return "double"
	case protoreflect.StringKind, protoreflect.BytesKind:
		return "length-scalar"
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return "message:" + string(field.Message().FullName())
	default:
		return field.Kind().String()
	}
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
	_, _ = hash.Write([]byte(domain.SchemaTypeProtobuf))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(canonical))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(encodedReferences)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func formatError(kind, detail string, cause error) error {
	return &formats.Error{Kind: kind, Detail: detail, Cause: cause}
}
