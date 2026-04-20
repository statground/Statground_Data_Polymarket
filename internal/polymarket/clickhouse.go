package polymarket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ClickHouseClient struct {
	cfg         *Config
	client      *http.Client
	baseURL     string
	schemaMu    sync.RWMutex
	schemaCache map[string]map[string]string
}

type clickHouseJSONResponse struct {
	Data []map[string]any `json:"data"`
}

func NewClickHouseClient(cfg *Config) (*ClickHouseClient, error) {
	if strings.TrimSpace(cfg.CHHost) == "" {
		return nil, fmt.Errorf("CLICKHOUSE_HOST is required")
	}
	if cfg.CHInterface != "http" {
		return nil, fmt.Errorf("CLICKHOUSE_INTERFACE=%s is not supported by this pure-Go conversion; use http (port 8123/8443)", cfg.CHInterface)
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: cfg.CHConnectTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: cfg.CHSendReceiveTimeout,
	}
	return &ClickHouseClient{
		cfg:         cfg,
		client:      &http.Client{Timeout: cfg.CHSendReceiveTimeout, Transport: tr},
		baseURL:     fmt.Sprintf("%s://%s:%d", cfg.CHScheme, cfg.CHHost, cfg.CHPort),
		schemaCache: make(map[string]map[string]string),
	}, nil
}

func (c *ClickHouseClient) Reset() (*ClickHouseClient, error) {
	return NewClickHouseClient(c.cfg)
}

func (c *ClickHouseClient) qualifiedTable(table string) string {
	return fmt.Sprintf("%s.%s", c.cfg.CHDatabase, table)
}

func (c *ClickHouseClient) doRequest(ctx context.Context, sql string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.cfg.CHUser, c.cfg.CHPassword)
	q := req.URL.Query()
	q.Set("database", c.cfg.CHDatabase)
	q.Set("query", sql)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("User-Agent", "statground-polymarket-go")
	if body != nil {
		req.Header.Set("Content-Type", "application/x-ndjson")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("clickhouse status=%d sql=%s body=%s", resp.StatusCode, TrimBody(sql, 180), TrimBody(string(raw), 600))
	}
	return raw, nil
}

func ensureFormatJSON(sql string) string {
	upper := strings.ToUpper(sql)
	if strings.Contains(upper, "FORMAT JSON") {
		return sql
	}
	return strings.TrimSpace(sql) + "\nFORMAT JSON"
}

func (c *ClickHouseClient) QueryRows(ctx context.Context, sql string) ([]map[string]any, error) {
	raw, err := c.doRequest(ctx, ensureFormatJSON(sql), nil)
	if err != nil {
		return nil, err
	}
	var resp clickHouseJSONResponse
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (c *ClickHouseClient) QueryOneRow(ctx context.Context, sql string) (map[string]any, error) {
	rows, err := c.QueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return map[string]any{}, nil
	}
	return rows[0], nil
}

func (c *ClickHouseClient) Exec(ctx context.Context, sql string) error {
	_, err := c.doRequest(ctx, sql, nil)
	return err
}

func (c *ClickHouseClient) DescribeTable(ctx context.Context, table string) (map[string]string, error) {
	cacheKey := c.qualifiedTable(table)

	c.schemaMu.RLock()
	if schema, ok := c.schemaCache[cacheKey]; ok {
		c.schemaMu.RUnlock()
		return schema, nil
	}
	c.schemaMu.RUnlock()

	rows, err := c.QueryRows(ctx, fmt.Sprintf("DESCRIBE TABLE %s", cacheKey))
	if err != nil {
		return nil, err
	}
	schema := make(map[string]string, len(rows))
	for _, row := range rows {
		name := SafeString(row["name"])
		typ := SafeString(row["type"])
		if name != "" {
			schema[name] = typ
		}
	}

	c.schemaMu.Lock()
	c.schemaCache[cacheKey] = schema
	c.schemaMu.Unlock()
	return schema, nil
}

func (c *ClickHouseClient) GetInsertColumns(ctx context.Context, entity string) ([]string, error) {
	columns := append([]string{}, baseInsertColumns[entity]...)
	schema, err := c.DescribeTable(ctx, c.cfg.EntityTable(entity))
	if err != nil {
		return nil, err
	}
	if _, ok := schema["raw_json"]; ok {
		columns = append(columns, "raw_json")
	}
	return columns, nil
}

func (c *ClickHouseClient) PrepareRawJSONValue(ctx context.Context, entity string, obj map[string]any) (any, bool, error) {
	schema, err := c.DescribeTable(ctx, c.cfg.EntityTable(entity))
	if err != nil {
		return nil, false, err
	}
	rawType, ok := schema["raw_json"]
	if !ok {
		return nil, false, nil
	}
	if strings.HasPrefix(strings.ToUpper(rawType), "JSON") {
		return obj, true, nil
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, false, err
	}
	return string(b), true, nil
}

const autoDisableParallelParsingRowBytes = 1 << 20

func encodeJSONEachRowBody(rows []map[string]any) ([]byte, int, error) {
	buf := &bytes.Buffer{}
	maxRowBytes := 0
	for _, row := range rows {
		lineBuf := &bytes.Buffer{}
		enc := json.NewEncoder(lineBuf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(row); err != nil {
			return nil, 0, err
		}
		if lineBuf.Len() > maxRowBytes {
			maxRowBytes = lineBuf.Len()
		}
		if _, err := buf.Write(lineBuf.Bytes()); err != nil {
			return nil, 0, err
		}
	}
	return buf.Bytes(), maxRowBytes, nil
}

func (c *ClickHouseClient) InsertJSONEachRow(ctx context.Context, table string, columns []string, rows []map[string]any, settings map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	bodyBytes, maxRowBytes, err := encodeJSONEachRowBody(rows)
	if err != nil {
		return err
	}

	effectiveSettings := cloneAnyMap(settings)
	if maxRowBytes >= autoDisableParallelParsingRowBytes {
		if _, exists := effectiveSettings["input_format_parallel_parsing"]; !exists {
			effectiveSettings["input_format_parallel_parsing"] = 0
			fmt.Printf("[INSERT SETTINGS] table=%s rows=%d max_row_bytes=%d -> input_format_parallel_parsing=0\n", table, len(rows), maxRowBytes)
		}
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(c.qualifiedTable(table))
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(")")
	if len(effectiveSettings) > 0 {
		first := true
		sb.WriteString(" SETTINGS ")
		for key, value := range effectiveSettings {
			if !first {
				sb.WriteString(", ")
			}
			first = false
			sb.WriteString(key)
			sb.WriteString(" = ")
			switch v := value.(type) {
			case string:
				sb.WriteString("'")
				sb.WriteString(strings.ReplaceAll(v, "'", "\\'"))
				sb.WriteString("'")
			default:
				sb.WriteString(fmt.Sprint(v))
			}
		}
	}
	sb.WriteString(" FORMAT JSONEachRow")

	_, err = c.doRequest(ctx, sb.String(), bytes.NewReader(bodyBytes))
	return err
}

func (c *ClickHouseClient) GetInsertSettings() map[string]any {
	settings := make(map[string]any)
	if c.cfg.InsertMaxPartitionsPerBlock > 0 {
		settings["max_partitions_per_insert_block"] = c.cfg.InsertMaxPartitionsPerBlock
	}
	if c.cfg.InsertThrowOnMaxPartitionsPerBlock {
		settings["throw_on_max_partitions_per_insert_block"] = 1
	} else {
		settings["throw_on_max_partitions_per_insert_block"] = 0
	}
	if strings.TrimSpace(c.cfg.CHDateTimeInputFormat) != "" {
		settings["date_time_input_format"] = c.cfg.CHDateTimeInputFormat
	}
	return settings
}

func splitRows(rows []map[string]any) ([]map[string]any, []map[string]any) {
	mid := len(rows) / 2
	if mid < 1 {
		mid = 1
	}
	left := append([]map[string]any{}, rows[:mid]...)
	right := append([]map[string]any{}, rows[mid:]...)
	return left, right
}

func (c *ClickHouseClient) InsertEntityRows(ctx context.Context, entity string, rows []map[string]any) (*ClickHouseClient, error) {
	if len(rows) == 0 {
		return c, nil
	}
	table := c.cfg.EntityTable(entity)
	columns, err := c.GetInsertColumns(ctx, entity)
	if err != nil {
		return c, err
	}
	settings := c.GetInsertSettings()

	current := c
	for attempt := 1; attempt <= c.cfg.InsertMaxRetries; attempt++ {
		err = current.InsertJSONEachRow(ctx, table, columns, rows, settings)
		if err == nil {
			return current, nil
		}
		if isParallelParsingLargeRowError(err) {
			fallbackSettings := cloneAnyMap(settings)
			fallbackSettings["input_format_parallel_parsing"] = 0
			fmt.Printf("[INSERT FALLBACK] entity=%s rows=%d attempt=%d/%d -> input_format_parallel_parsing=0 err=%v\n", entity, len(rows), attempt, c.cfg.InsertMaxRetries, err)
			err = current.InsertJSONEachRow(ctx, table, columns, rows, fallbackSettings)
			if err == nil {
				return current, nil
			}
		}
		retryable := IsRetryableInsertError(err)
		shouldSplit := retryable && len(rows) >= c.cfg.InsertMinSplitBatchRows && attempt >= c.cfg.InsertSplitAfterAttempt
		if shouldSplit {
			left, right := splitRows(rows)
			fmt.Printf("[INSERT SPLIT] entity=%s rows=%d -> %d+%d attempt=%d/%d err=%v\n", entity, len(rows), len(left), len(right), attempt, c.cfg.InsertMaxRetries, err)
			next, resetErr := current.Reset()
			if resetErr != nil {
				return current, resetErr
			}
			current = next
			current, err = current.InsertEntityRows(ctx, entity, left)
			if err != nil {
				return current, err
			}
			current, err = current.InsertEntityRows(ctx, entity, right)
			return current, err
		}
		if attempt >= c.cfg.InsertMaxRetries || !retryable {
			return current, err
		}
		sleepFor := RetryBackoff(c.cfg.InsertRetryBaseSleep, attempt)
		fmt.Printf("[INSERT RETRY] entity=%s rows=%d attempt=%d/%d sleep=%s err=%v\n", entity, len(rows), attempt, c.cfg.InsertMaxRetries, sleepFor, err)
		if err := SleepContext(ctx, sleepFor); err != nil {
			return current, err
		}
		next, resetErr := current.Reset()
		if resetErr != nil {
			return current, resetErr
		}
		current = next
	}
	return current, err
}

func (c *ClickHouseClient) FlushEntityRows(ctx context.Context, entity string, buffer *[]map[string]any, force bool) (*ClickHouseClient, error) {
	batchSize := c.cfg.InsertBatchSizeForEntity(entity)
	current := c
	for len(*buffer) > 0 && (force || len(*buffer) >= batchSize) {
		batchLen := batchSize
		if len(*buffer) < batchLen {
			batchLen = len(*buffer)
		}
		batch := append([]map[string]any{}, (*buffer)[:batchLen]...)
		next, err := current.InsertEntityRows(ctx, entity, batch)
		if err != nil {
			return current, err
		}
		current = next
		*buffer = append([]map[string]any{}, (*buffer)[batchLen:]...)
	}
	return current, nil
}

func (c *ClickHouseClient) OptimizeRandomPartitions(ctx context.Context, tables []string) {
	if !c.cfg.OptimizeAfterRun {
		return
	}
	prob := c.cfg.OptimizeProb
	if prob <= 0 {
		return
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	if rnd.Float64() > prob {
		return
	}

	nPartitions := maxInt(1, c.cfg.OptimizePartitions)
	k := c.cfg.OptimizeMaxPartitions
	if k < 1 {
		k = 1
	}
	if k > nPartitions {
		k = nPartitions
	}

	perm := rnd.Perm(nPartitions)
	picked := perm[:k]
	for _, table := range tables {
		for _, part := range picked {
			sql := fmt.Sprintf("OPTIMIZE TABLE %s PARTITION %d FINAL", table, part)
			if err := c.Exec(ctx, sql); err != nil {
				fmt.Printf("[OPTIMIZE] skip: %s partition=%d err=%v\n", table, part, err)
				continue
			}
			fmt.Printf("[OPTIMIZE] ok: %s partition=%d\n", table, part)
		}
	}
}
