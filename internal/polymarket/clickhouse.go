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

type ClickHouseClient struct {
	cfg    *Config
	client *http.Client
}

type ClickHouseStateStore struct {
	cfg *Config
	ch  *ClickHouseClient
}

var clickHouseAsiaSeoulLocation = time.FixedZone("Asia/Seoul", 9*60*60)

func NewClickHouseClient(cfg *Config) *ClickHouseClient {
	timeout := cfg.ClickHouseTimeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	connectTimeout := cfg.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: timeout,
	}
	return &ClickHouseClient{
		cfg:    cfg,
		client: &http.Client{Timeout: timeout, Transport: tr},
	}
}

func (c *ClickHouseClient) Exec(ctx context.Context, query string) ([]byte, error) {
	baseURL, err := c.baseURL()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("User-Agent", "statground-polymarket-clickhouse-direct")
	if strings.TrimSpace(c.cfg.ClickHouseUser) != "" || strings.TrimSpace(c.cfg.ClickHousePassword) != "" {
		req.SetBasicAuth(c.cfg.ClickHouseUser, c.cfg.ClickHousePassword)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("clickhouse status=%d body=%s", resp.StatusCode, TrimBody(string(body), 600))
	}
	return body, nil
}

func (c *ClickHouseClient) baseURL() (string, error) {
	if raw := strings.TrimSpace(c.cfg.ClickHouseHTTPURL); raw != "" {
		u, err := url.Parse(raw)
		if err != nil {
			return "", err
		}
		if u.Scheme == "" || u.Host == "" {
			return "", fmt.Errorf("invalid ClickHouse HTTP URL")
		}
		return u.String(), nil
	}

	host := strings.TrimSpace(c.cfg.ClickHouseHost)
	if host == "" {
		return "", fmt.Errorf("missing required env: CH_HOST or CLICKHOUSE_HOST")
	}
	protocol := strings.TrimSpace(c.cfg.ClickHouseProtocol)
	if protocol == "" {
		protocol = "http"
	}
	port := strings.TrimSpace(c.cfg.ClickHousePort)
	if port == "" {
		port = "8123"
	}
	address := host
	if _, _, err := net.SplitHostPort(host); err != nil {
		address = net.JoinHostPort(strings.Trim(host, "[]"), port)
	}
	path := strings.TrimSpace(c.cfg.ClickHouseHTTPURLPath)
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u := url.URL{Scheme: protocol, Host: address, Path: path}
	return u.String(), nil
}

func (i *Ingestor) ValidateClickHouseIngest(ctx context.Context) error {
	if i.ch == nil {
		i.ch = NewClickHouseClient(i.cfg)
	}
	if strings.TrimSpace(i.cfg.ClickHouseUser) == "" {
		return fmt.Errorf("missing required env: CH_USER or CLICKHOUSE_USER")
	}
	if strings.TrimSpace(i.cfg.ClickHousePassword) == "" {
		return fmt.Errorf("missing required env: CH_PASSWORD or CLICKHOUSE_PASSWORD")
	}
	tables := []string{
		i.clickHouseSnapshotTable("events"),
		i.clickHouseSnapshotTable("markets"),
		i.clickHouseSnapshotTable("series"),
		i.clickHouseCheckpointTable(),
	}
	for _, table := range tables {
		query := fmt.Sprintf("SELECT 1 FROM %s LIMIT 0 FORMAT TSV", table)
		if _, err := i.ch.Exec(ctx, query); err != nil {
			return fmt.Errorf("clickhouse preflight failed for %s: %w", table, err)
		}
	}
	fmt.Printf("[clickhouse] preflight ok database=%s tables=%d\n", i.cfg.ClickHouseDatabase, len(tables))
	return nil
}

func (i *Ingestor) InsertEntityRowsClickHouse(ctx context.Context, entity string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	if i.ch == nil {
		i.ch = NewClickHouseClient(i.cfg)
	}
	body, err := i.clickHouseInsertBody(entity, rows)
	if err != nil {
		return err
	}
	if err := i.execClickHouseWithRetry(ctx, fmt.Sprintf("insert entity=%s rows=%d", entity, len(rows)), body); err != nil {
		return err
	}
	fmt.Printf("[clickhouse] entity=%s inserted=%d table=%s\n", entity, len(rows), i.clickHouseSnapshotTable(entity))
	return nil
}

func (i *Ingestor) clickHouseInsertBody(entity string, rows []map[string]any) (string, error) {
	table := i.clickHouseSnapshotTable(entity)
	columns := baseInsertColumns[entity]
	if table == "" || len(columns) == 0 {
		return "", fmt.Errorf("unsupported entity for ClickHouse insert: %s", entity)
	}
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "INSERT INTO %s (%s) FORMAT JSONEachRow\n", table, clickHouseColumnList(columns))
	for _, row := range rows {
		out := make(map[string]any, len(columns))
		for _, col := range columns {
			out[col] = row[col]
		}
		line, err := json.Marshal(out)
		if err != nil {
			return "", err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.String(), nil
}

func (i *Ingestor) execClickHouseWithRetry(ctx context.Context, label string, query string) error {
	maxRetries := maxInt(1, i.cfg.MaxRetries)
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, err := i.ch.Exec(ctx, query)
		if err == nil {
			if attempt > 1 {
				fmt.Printf("[clickhouse retry] succeeded label=%s attempt=%d\n", label, attempt)
			}
			return nil
		}
		lastErr = err
		if ctx.Err() != nil || !IsRetryableInsertError(err) || attempt >= maxRetries {
			return err
		}
		sleepFor := RetryBackoff(i.cfg.BaseSleep, attempt)
		fmt.Printf("[clickhouse retry] label=%s attempt=%d/%d sleep=%s err=%s\n", label, attempt, maxRetries, sleepFor, shortClickHouseError(err))
		if err := SleepContext(ctx, sleepFor); err != nil {
			return fmt.Errorf("clickhouse retry wait stopped: %w; last_error=%s", err, shortClickHouseError(lastErr))
		}
	}
	return lastErr
}

func (s *ClickHouseStateStore) LoadCheckpoint(ctx context.Context) (map[string]string, error) {
	query := fmt.Sprintf(
		"SELECT checkpoint_json FROM %s WHERE service = 'polymarket' ORDER BY updated_at DESC, checkpoint_uuid DESC LIMIT 1 FORMAT JSONEachRow",
		(&Ingestor{cfg: s.cfg}).clickHouseCheckpointTable(),
	)
	body, err := s.ch.Exec(ctx, query)
	if err != nil {
		return nil, err
	}
	line := strings.TrimSpace(string(body))
	if line == "" {
		return map[string]string{}, nil
	}
	var row struct {
		CheckpointJSON string `json:"checkpoint_json"`
	}
	if err := json.Unmarshal([]byte(firstJSONEachRowLine(line)), &row); err != nil {
		return map[string]string{}, err
	}
	if strings.TrimSpace(row.CheckpointJSON) == "" {
		return map[string]string{}, nil
	}
	var checkpoint map[string]string
	if err := json.Unmarshal([]byte(row.CheckpointJSON), &checkpoint); err != nil {
		return map[string]string{}, nil
	}
	if checkpoint == nil {
		checkpoint = map[string]string{}
	}
	return checkpoint, nil
}

func (s *ClickHouseStateStore) SaveCheckpoint(ctx context.Context, checkpoint map[string]string) error {
	if s.ch == nil {
		s.ch = NewClickHouseClient(s.cfg)
	}
	checkpointUUID, err := UUIDv7()
	if err != nil {
		return err
	}
	payload, err := marshalCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	row := map[string]any{
		"checkpoint_uuid": checkpointUUID,
		"service":         "polymarket",
		"source":          s.cfg.ProducerSource,
		"checkpoint_json": string(payload),
		"updated_at":      FormatClickHouseDateTime64Millis(UTCNow(), clickHouseAsiaSeoulLocation),
	}
	line, err := json.Marshal(row)
	if err != nil {
		return err
	}
	table := (&Ingestor{cfg: s.cfg}).clickHouseCheckpointTable()
	query := fmt.Sprintf("INSERT INTO %s (`checkpoint_uuid`, `service`, `source`, `checkpoint_json`, `updated_at`) FORMAT JSONEachRow\n%s\n", table, string(line))
	maxRetries := maxInt(1, s.cfg.MaxRetries)
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if _, err := s.ch.Exec(ctx, query); err == nil {
			return nil
		} else {
			lastErr = err
			if ctx.Err() != nil || !IsRetryableInsertError(err) || attempt >= maxRetries {
				return err
			}
			if waitErr := SleepContext(ctx, RetryBackoff(s.cfg.BaseSleep, attempt)); waitErr != nil {
				return fmt.Errorf("clickhouse checkpoint retry wait stopped: %w; last_error=%s", waitErr, shortClickHouseError(lastErr))
			}
		}
	}
	return lastErr
}

func (i *Ingestor) clickHouseSnapshotTable(entity string) string {
	switch entity {
	case "events":
		return clickHouseTableName(i.cfg.ClickHouseDatabase, "polymarket_event_snapshot")
	case "markets":
		return clickHouseTableName(i.cfg.ClickHouseDatabase, "polymarket_market_snapshot")
	case "series":
		return clickHouseTableName(i.cfg.ClickHouseDatabase, "polymarket_series_snapshot")
	default:
		return ""
	}
}

func (i *Ingestor) clickHouseCheckpointTable() string {
	return clickHouseTableName(i.cfg.ClickHouseDatabase, "polymarket_crawl_checkpoint")
}

func clickHouseTableName(database string, table string) string {
	return clickHouseIdent(database) + "." + clickHouseIdent(table)
}

func clickHouseColumnList(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, col := range columns {
		quoted = append(quoted, clickHouseIdent(col))
	}
	return strings.Join(quoted, ", ")
}

func clickHouseIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func firstJSONEachRowLine(body string) string {
	body = strings.TrimSpace(body)
	if idx := strings.IndexByte(body, '\n'); idx >= 0 {
		return strings.TrimSpace(body[:idx])
	}
	return body
}

func shortClickHouseError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.Join(strings.Fields(err.Error()), " ")
	return truncateString(msg, 280)
}
