package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// canonicalDefinition renders the stable JSON ordering emitted by Apache
// Avro's Schema.toString(). Hamba's Schema.String is the parsing canonical
// form and intentionally uses a different property order and omits metadata.
func canonicalDefinition(raw string) (string, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	shortenNamedReferences(value, "")
	var output bytes.Buffer
	if err := writeCanonicalJSON(&output, value, false); err != nil {
		return "", err
	}
	return output.String(), nil
}

func shortenNamedReferences(value any, parentNamespace string) {
	object, ok := value.(map[string]any)
	if !ok {
		return
	}
	namespace := parentNamespace
	if explicit, ok := object["namespace"].(string); ok {
		namespace = explicit
	} else if name, ok := object["name"].(string); ok {
		if separator := strings.LastIndexByte(name, '.'); separator >= 0 {
			namespace = name[:separator]
		}
	}
	fields, _ := object["fields"].([]any)
	for _, item := range fields {
		field, ok := item.(map[string]any)
		if !ok {
			continue
		}
		field["type"] = shortenTypeValue(field["type"], namespace)
	}
	if nested, ok := object["items"]; ok {
		object["items"] = shortenTypeValue(nested, namespace)
	}
	if nested, ok := object["values"]; ok {
		object["values"] = shortenTypeValue(nested, namespace)
	}
}

func shortenTypeValue(value any, namespace string) any {
	switch typed := value.(type) {
	case string:
		prefix := namespace + "."
		if namespace != "" && strings.HasPrefix(typed, prefix) {
			return strings.TrimPrefix(typed, prefix)
		}
		return typed
	case []any:
		for index, item := range typed {
			typed[index] = shortenTypeValue(item, namespace)
		}
		return typed
	case map[string]any:
		shortenNamedReferences(typed, namespace)
		return typed
	default:
		return value
	}
}

func writeCanonicalJSON(output *bytes.Buffer, value any, field bool) error {
	switch typed := value.(type) {
	case map[string]any:
		if primitive, ok := primitiveObject(typed); ok && !field {
			encoded, _ := json.Marshal(primitive)
			output.Write(encoded)
			return nil
		}
		output.WriteByte('{')
		keys := orderedKeys(typed, field)
		for index, key := range keys {
			if index > 0 {
				output.WriteByte(',')
			}
			encodedKey, _ := json.Marshal(key)
			output.Write(encodedKey)
			output.WriteByte(':')
			childIsField := key == "fields"
			if childIsField {
				items, ok := typed[key].([]any)
				if !ok {
					return fmt.Errorf("fields is not an array")
				}
				output.WriteByte('[')
				for itemIndex, item := range items {
					if itemIndex > 0 {
						output.WriteByte(',')
					}
					if err := writeCanonicalJSON(output, item, true); err != nil {
						return err
					}
				}
				output.WriteByte(']')
				continue
			}
			if err := writeCanonicalJSON(output, typed[key], false); err != nil {
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
			if err := writeCanonicalJSON(output, item, false); err != nil {
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

func primitiveObject(value map[string]any) (string, bool) {
	if len(value) != 1 {
		return "", false
	}
	typeName, ok := value["type"].(string)
	if !ok {
		return "", false
	}
	switch typeName {
	case "null", "boolean", "int", "long", "float", "double", "bytes", "string":
		return typeName, true
	default:
		return "", false
	}
}

func orderedKeys(value map[string]any, field bool) []string {
	priority := []string{"type", "name", "namespace", "doc", "aliases", "fields", "symbols", "default", "size", "items", "values", "logicalType", "precision", "scale"}
	if field {
		priority = []string{"name", "type", "doc", "default", "order", "aliases"}
	}
	result := make([]string, 0, len(value))
	seen := make(map[string]bool, len(value))
	for _, key := range priority {
		if _, ok := value[key]; ok {
			result = append(result, key)
			seen[key] = true
		}
	}
	remaining := make([]string, 0, len(value)-len(result))
	for key := range value {
		if !seen[key] {
			remaining = append(remaining, key)
		}
	}
	sort.Strings(remaining)
	return append(result, remaining...)
}
