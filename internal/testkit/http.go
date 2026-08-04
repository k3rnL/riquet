package testkit

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// WaitHTTP polls a readiness URL until it returns a 2xx response or the context
// expires. Response bodies are always closed and bounded.
func WaitHTTP(ctx context.Context, client *http.Client, url string, interval time.Duration) error {
	if client == nil {
		client = http.DefaultClient
	}
	if interval <= 0 {
		interval = 25 * time.Millisecond
	}

	timer := time.NewTimer(0)
	defer timer.Stop()
	var last string
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s: %w (last result: %s)", url, ctx.Err(), last)
		case <-timer.C:
		}

		request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("create readiness request: %w", err)
		}
		response, err := client.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
			_ = response.Body.Close()
			last = response.Status
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				return nil
			}
		} else {
			last = err.Error()
		}

		timer.Reset(interval)
	}
}
