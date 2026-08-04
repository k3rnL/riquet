package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
)

// Scenario is an ordered black-box interaction with one registry.
type Scenario struct {
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}

// Step describes one HTTP request and its immediate expectations.
type Step struct {
	Name    string              `json:"name"`
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    json.RawMessage     `json:"body,omitempty"`
	Expect  Expectation         `json:"expect"`
}

// Expectation contains assertions that are independent of implementation.
type Expectation struct {
	Status int             `json:"status"`
	JSON   json.RawMessage `json:"json,omitempty"`
}

// LoadScenario loads and validates one declarative JSON scenario.
func LoadScenario(filename string) (Scenario, error) {
	raw, err := os.ReadFile(filename)
	if err != nil {
		return Scenario{}, err
	}
	var scenario Scenario
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&scenario); err != nil {
		return Scenario{}, fmt.Errorf("decode scenario %s: %w", filename, err)
	}
	if scenario.Name == "" || len(scenario.Steps) == 0 {
		return Scenario{}, fmt.Errorf("scenario name and steps are required")
	}
	return scenario, nil
}

// Run executes all steps and returns the target trace.
func (s Scenario) Run(ctx context.Context, target Target) (Trace, error) {
	if err := target.Reset(ctx); err != nil {
		return Trace{}, fmt.Errorf("reset %s: %w", target.Name(), err)
	}
	return s.RunPrepared(ctx, target)
}

// RunPrepared executes against a freshly provisioned target without resetting
// its lifecycle. It is useful when target construction itself provides isolation.
func (s Scenario) RunPrepared(ctx context.Context, target Target) (Trace, error) {
	for index, step := range s.Steps {
		if err := runStep(ctx, target, step); err != nil {
			return target.Trace(), fmt.Errorf("scenario %q step %d (%s): %w", s.Name, index+1, step.Name, err)
		}
	}
	return target.Trace(), nil
}

func runStep(ctx context.Context, target Target, step Step) error {
	base := target.BaseURL()
	requestURL := cloneURL(base)
	parsed, err := url.Parse(step.Path)
	if err != nil {
		return fmt.Errorf("parse step path: %w", err)
	}
	requestURL.Path = path.Join(base.Path, parsed.Path)
	requestURL.RawQuery = parsed.RawQuery
	request, err := http.NewRequestWithContext(ctx, step.Method, requestURL.String(), bytes.NewReader(step.Body))
	if err != nil {
		return err
	}
	request.Header = make(http.Header, len(step.Headers)+1)
	for key, values := range step.Headers {
		request.Header[key] = append([]string(nil), values...)
	}
	if len(step.Body) > 0 && request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := target.Client().Do(request)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, defaultTraceBodyLimit+1))
	closeErr := response.Body.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if response.StatusCode != step.Expect.Status {
		return fmt.Errorf("status = %d, want %d: %s", response.StatusCode, step.Expect.Status, body)
	}
	if len(step.Expect.JSON) > 0 {
		var got any
		var want any
		if err := json.Unmarshal(body, &got); err != nil {
			return fmt.Errorf("decode response JSON: %w", err)
		}
		if err := json.Unmarshal(step.Expect.JSON, &want); err != nil {
			return fmt.Errorf("decode expected JSON: %w", err)
		}
		if difference := compareJSONExact(want, got, "$"); difference != "" {
			return fmt.Errorf("response JSON mismatch: %s", difference)
		}
	}
	return nil
}
