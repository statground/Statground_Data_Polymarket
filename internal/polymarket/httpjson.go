package polymarket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type FetchMeta struct {
	HTTPStatus int    `json:"http_status"`
	ResponseMS int64  `json:"response_ms"`
	FullURL    string `json:"full_url"`
}

type HTTPJSONClient struct {
	client    *http.Client
	userAgent string
}

func NewHTTPJSONClient(timeout time.Duration, connectTimeout time.Duration, userAgent string) *HTTPJSONClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: timeout,
	}
	return &HTTPJSONClient{
		client:    &http.Client{Timeout: timeout, Transport: tr},
		userAgent: userAgent,
	}
}

func (c *HTTPJSONClient) DoJSON(ctx context.Context, method, rawURL string, query map[string]string, headers map[string]string, body []byte) (any, int, error) {
	fullURL, err := buildURL(rawURL, query)
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, method, fullURL, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	req.Header.Set("Accept", "application/json")
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, resp.StatusCode, fmt.Errorf("http status=%d body=%s", resp.StatusCode, string(raw))
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, resp.StatusCode, err
	}
	return v, resp.StatusCode, nil
}

func (c *HTTPJSONClient) GetJSONWithMeta(ctx context.Context, rawURL string, query map[string]string) (any, FetchMeta, error) {
	fullURL, err := buildURL(rawURL, query)
	if err != nil {
		return nil, FetchMeta{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, FetchMeta{}, err
	}
	req.Header.Set("Accept", "application/json")
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, FetchMeta{FullURL: fullURL}, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, FetchMeta{HTTPStatus: resp.StatusCode, FullURL: fullURL}, err
	}
	meta := FetchMeta{
		HTTPStatus: resp.StatusCode,
		ResponseMS: time.Since(start).Milliseconds(),
		FullURL:    fullURL,
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, meta, fmt.Errorf("http status=%d body=%s", resp.StatusCode, string(raw))
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, meta, err
	}
	return v, meta, nil
}

func (c *HTTPJSONClient) SafeGetJSON(ctx context.Context, rawURL string, query map[string]string, maxRetries int, baseSleep time.Duration) (any, FetchMeta, error) {
	var lastMeta FetchMeta
	var lastErr error
	for attempt := 1; attempt <= maxInt(1, maxRetries); attempt++ {
		data, meta, err := c.GetJSONWithMeta(ctx, rawURL, query)
		if err == nil {
			return data, meta, nil
		}
		lastMeta = meta
		lastErr = err

		retryable := RetryableHTTPStatus(meta.HTTPStatus) || IsRetryableInsertError(err)
		if !retryable || attempt >= maxRetries {
			return nil, lastMeta, err
		}
		sleepFor := RetryBackoff(baseSleep, attempt)
		fmt.Printf("[HTTP RETRY] status=%d attempt=%d/%d sleep=%s url=%s\n", meta.HTTPStatus, attempt, maxRetries, sleepFor, meta.FullURL)
		if err := SleepContext(ctx, sleepFor); err != nil {
			return nil, lastMeta, err
		}
	}
	return nil, lastMeta, lastErr
}

func buildURL(rawURL string, query map[string]string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for k, v := range query {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func ExtractItems(data any, entity string) []map[string]any {
	switch v := data.(type) {
	case []any:
		return normalizeItems(v)
	case map[string]any:
		for _, key := range []string{"data", entity, "results"} {
			if arr, ok := v[key].([]any); ok {
				return normalizeItems(arr)
			}
		}
	}
	return nil
}

func normalizeItems(arr []any) []map[string]any {
	out := make([]map[string]any, 0, len(arr))
	for _, item := range arr {
		if obj, ok := item.(map[string]any); ok {
			out = append(out, obj)
		}
	}
	return out
}

func StringifyJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func TrimBody(body string, maxLen int) string {
	body = strings.TrimSpace(body)
	if len(body) <= maxLen {
		return body
	}
	return body[:maxLen]
}
