package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// CompareOptions controls semantic trace comparison.
type CompareOptions struct {
	RelevantHeaders []string
	SymbolicFields  map[string]string
	OpaqueFields    map[string]bool
	ExactPaths      map[string]bool
}

// Difference identifies the first observable mismatch.
type Difference struct {
	Exchange int    `json:"exchange"`
	Path     string `json:"path"`
	Message  string `json:"message"`
}

// CompareTraces compares reference and candidate traces semantically.
func CompareTraces(reference, candidate Trace, options CompareOptions) *Difference {
	if len(reference.Exchanges) != len(candidate.Exchanges) {
		return &Difference{Path: "$.exchanges", Message: fmt.Sprintf("length %d != %d", len(reference.Exchanges), len(candidate.Exchanges))}
	}
	symbols := newSymbolTable()
	for index := range reference.Exchanges {
		left := reference.Exchanges[index]
		right := candidate.Exchanges[index]
		if left.Method != right.Method {
			return &Difference{Exchange: index, Path: "$.method", Message: left.Method + " != " + right.Method}
		}
		if left.Status != right.Status {
			return &Difference{Exchange: index, Path: "$.status", Message: fmt.Sprintf("%d != %d", left.Status, right.Status)}
		}
		for _, header := range options.RelevantHeaders {
			leftValues := canonicalHeader(left.ResponseHeaders, header)
			rightValues := canonicalHeader(right.ResponseHeaders, header)
			if !reflect.DeepEqual(leftValues, rightValues) {
				return &Difference{Exchange: index, Path: "$.headers." + header, Message: fmt.Sprintf("%v != %v", leftValues, rightValues)}
			}
		}
		if difference := compareBodies(left.ResponseBody, right.ResponseBody, options, symbols); difference != nil {
			difference.Exchange = index
			return difference
		}
	}
	return nil
}

func compareBodies(leftRaw, rightRaw []byte, options CompareOptions, symbols *symbolTable) *Difference {
	if len(bytes.TrimSpace(leftRaw)) == 0 && len(bytes.TrimSpace(rightRaw)) == 0 {
		return nil
	}
	var left any
	var right any
	if json.Unmarshal(leftRaw, &left) != nil || json.Unmarshal(rightRaw, &right) != nil {
		if !bytes.Equal(leftRaw, rightRaw) {
			return &Difference{Path: "$.body", Message: fmt.Sprintf("%q != %q", leftRaw, rightRaw)}
		}
		return nil
	}
	if difference := compareSemantic(left, right, "$", "", options, symbols); difference != "" {
		parts := strings.SplitN(difference, ": ", 2)
		result := &Difference{Path: parts[0], Message: difference}
		if len(parts) == 2 {
			result.Message = parts[1]
		}
		return result
	}
	return nil
}

func compareSemantic(left, right any, currentPath, field string, options CompareOptions, symbols *symbolTable) string {
	if options.OpaqueFields[field] && !options.ExactPaths[currentPath] {
		return ""
	}
	if symbol, ok := options.SymbolicFields[field]; ok && !options.ExactPaths[currentPath] {
		if !symbols.match(symbol, scalar(left), scalar(right)) {
			return currentPath + ": symbolic values are inconsistent"
		}
		return ""
	}
	switch typedLeft := left.(type) {
	case map[string]any:
		typedRight, ok := right.(map[string]any)
		if !ok || len(typedLeft) != len(typedRight) {
			return currentPath + ": object shapes differ"
		}
		keys := make([]string, 0, len(typedLeft))
		for key := range typedLeft {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			rightValue, exists := typedRight[key]
			if !exists {
				return currentPath + "." + key + ": missing from candidate"
			}
			if difference := compareSemantic(typedLeft[key], rightValue, currentPath+"."+key, key, options, symbols); difference != "" {
				return difference
			}
		}
	case []any:
		typedRight, ok := right.([]any)
		if !ok || len(typedLeft) != len(typedRight) {
			return currentPath + ": array lengths differ"
		}
		for index := range typedLeft {
			if difference := compareSemantic(typedLeft[index], typedRight[index], currentPath+"["+strconv.Itoa(index)+"]", field, options, symbols); difference != "" {
				return difference
			}
		}
	default:
		if !reflect.DeepEqual(left, right) {
			return currentPath + ": " + scalar(left) + " != " + scalar(right)
		}
	}
	return ""
}

func compareJSONExact(left, right any, currentPath string) string {
	return compareSemantic(left, right, currentPath, "", CompareOptions{}, newSymbolTable())
}

func canonicalHeader(headers map[string][]string, key string) []string {
	values := append([]string(nil), headers[http.CanonicalHeaderKey(key)]...)
	sort.Strings(values)
	return values
}

func scalar(value any) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

type symbolTable struct {
	forward map[string]map[string]string
	reverse map[string]map[string]string
}

func newSymbolTable() *symbolTable {
	return &symbolTable{forward: make(map[string]map[string]string), reverse: make(map[string]map[string]string)}
}

func (s *symbolTable) match(namespace, left, right string) bool {
	if s.forward[namespace] == nil {
		s.forward[namespace] = make(map[string]string)
		s.reverse[namespace] = make(map[string]string)
	}
	if known, ok := s.forward[namespace][left]; ok {
		return known == right
	}
	if known, ok := s.reverse[namespace][right]; ok {
		return known == left
	}
	s.forward[namespace][left] = right
	s.reverse[namespace][right] = left
	return true
}
