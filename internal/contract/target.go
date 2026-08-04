// Package contract provides black-box compatibility scenarios and differential
// comparison without depending on either registry implementation.
package contract

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Target is one isolated running registry instance.
type Target interface {
	Name() string
	BaseURL() *url.URL
	Client() *http.Client
	Reset(context.Context) error
	Close(context.Context) error
	Trace() Trace
}

// Provisioner starts a fresh registry target.
type Provisioner interface {
	Start(context.Context) (Target, error)
}

type endpointTarget struct {
	name    string
	baseURL *url.URL
	client  *http.Client
	trace   *Recorder
	reset   func(context.Context) error
	close   func(context.Context) error
}

// NewEndpointTarget wraps an already running endpoint. The optional reset and
// close functions let callers integrate externally managed fixtures.
func NewEndpointTarget(
	name string,
	baseURL string,
	client *http.Client,
	reset func(context.Context) error,
	closeFn func(context.Context) error,
) (Target, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("parse target URL: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("target URL must be absolute: %q", baseURL)
	}
	if client == nil {
		client = &http.Client{}
	}
	recorder := NewRecorder(client.Transport)
	copyClient := *client
	copyClient.Transport = recorder
	return &endpointTarget{
		name:    name,
		baseURL: parsed,
		client:  &copyClient,
		trace:   recorder,
		reset:   reset,
		close:   closeFn,
	}, nil
}

func (t *endpointTarget) Name() string         { return t.name }
func (t *endpointTarget) BaseURL() *url.URL    { return cloneURL(t.baseURL) }
func (t *endpointTarget) Client() *http.Client { return t.client }
func (t *endpointTarget) Trace() Trace         { return t.trace.Trace() }

func (t *endpointTarget) Reset(ctx context.Context) error {
	t.trace.Reset()
	if t.reset == nil {
		return nil
	}
	return t.reset(ctx)
}

func (t *endpointTarget) Close(ctx context.Context) error {
	if t.close == nil {
		return nil
	}
	return t.close(ctx)
}

func cloneURL(value *url.URL) *url.URL {
	copyValue := *value
	return &copyValue
}
