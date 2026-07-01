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

var polymarketDirectInsertOutboxColumns = []string{
	"outbox_uuid", "created_at", "target_database", "target_table",
	"target_columns", "rows_json", "row_count", "payload_hash", "source_error",
}

var clickHouseUTCDateTime64Columns = map[string]bool{
	"collected_at_utc":  true,
	"created_at_utc":    true,
	"updated_at_utc":    true,
	"start_date_utc":    true,
	"end_date_utc":      true,
	"closed_time_utc":   true,
	"creation_date_utc": true,
}

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
	if i.cfg.ClickHouseDirectOutboxFallback {
		drained, err := drainClickHouseDirectOutbox(ctx, i.cfg, i.ch)
		if err != nil {
			fmt.Printf("[warn] clickhouse direct outbox drain skipped: %s\n", sanitizeClickHouseError(i.cfg, err))
		} else if drained > 0 {
			fmt.Printf("[clickhouse] drained direct outbox chunks=%d\n", drained)
		}
	}
	return nil
}

func (i *Ingestor) InsertEntityRowsClickHouse(ctx context.Context, entity string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	if i.ch == nil {
		i.ch = NewClickHouseClient(i.cfg)
	}
	table := i.clickHouseSnapshotTable(entity)
	targetTable := clickHouseSnapshotTableBase(entity)
	columns := baseInsertColumns[entity]
	if table == "" || targetTable == "" || len(columns) == 0 {
		return fmt.Errorf("unsupported entity for ClickHouse insert: %s", entity)
	}
	payload, err := clickHouseJSONEachRowPayload(columns, rows)
	if err != nil {
		return err
	}
	body := clickHouseInsertQuery(table, columns, payload)
	if err := i.execClickHouseWithRetry(ctx, fmt.Sprintf("insert entity=%s rows=%d", entity, len(rows)), body); err != nil {
		if i.cfg.ClickHouseDirectOutboxFallback && IsRetryableInsertError(err) {
			if outboxErr := enqueueClickHouseDirectOutbox(ctx, i.cfg, i.ch, i.cfg.ClickHouseDatabase, targetTable, columns, payload, len(rows), err); outboxErr == nil {
				return nil
			} else {
				return fmt.Errorf("%w; clickhouse direct outbox enqueue failed: %s", err, sanitizeClickHouseError(i.cfg, outboxErr))
			}
		}
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
	payload, err := clickHouseJSONEachRowPayload(columns, rows)
	if err != nil {
		return "", err
	}
	return clickHouseInsertQuery(table, columns, payload), nil
}

func clickHouseInsertQuery(table string, columns []string, payload []byte) string {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "INSERT INTO %s (%s) FORMAT JSONEachRow\n", table, clickHouseColumnList(columns))
	buf.Write(payload)
	return buf.String()
}

func clickHouseJSONEachRowPayload(columns []string, rows []map[string]any) ([]byte, error) {
	buf := &bytes.Buffer{}
	for _, row := range rows {
		out := make(map[string]any, len(columns))
		for _, col := range columns {
			out[col] = clickHouseInsertValue(col, row[col])
		}
		line, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func clickHouseInsertValue(column string, value any) any {
	if value == nil {
		return nil
	}
	if !clickHouseUTCDateTime64Columns[column] {
		return value
	}
	switch v := value.(type) {
	case time.Time:
		return FormatClickHouseDateTime64Millis(v, time.UTC)
	case string:
		if t := ParseISOUTC(v); t != nil {
			return FormatClickHouseDateTime64Millis(*t, time.UTC)
		}
	}
	if t := ParseClickHouseTime(value); t != nil {
		return FormatClickHouseDateTime64Millis(*t, time.UTC)
	}
	return value
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
	payloadRows := append(line, '\n')
	table := (&Ingestor{cfg: s.cfg}).clickHouseCheckpointTable()
	targetTable := "polymarket_crawl_checkpoint"
	columns := []string{"checkpoint_uuid", "service", "source", "checkpoint_json", "updated_at"}
	query := clickHouseInsertQuery(table, columns, payloadRows)
	maxRetries := maxInt(1, s.cfg.MaxRetries)
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if _, err := s.ch.Exec(ctx, query); err == nil {
			if attempt > 1 {
				fmt.Printf("[clickhouse checkpoint retry] succeeded attempt=%d\n", attempt)
			}
			return nil
		} else {
			lastErr = err
			if ctx.Err() != nil {
				return err
			}
			if !IsRetryableInsertError(err) || attempt >= maxRetries {
				if s.cfg.ClickHouseDirectOutboxFallback && IsRetryableInsertError(err) {
					if outboxErr := enqueueClickHouseDirectOutbox(ctx, s.cfg, s.ch, s.cfg.ClickHouseDatabase, targetTable, columns, payloadRows, 1, err); outboxErr == nil {
						return nil
					} else {
						return fmt.Errorf("%w; clickhouse direct outbox enqueue failed: %s", err, sanitizeClickHouseError(s.cfg, outboxErr))
					}
				}
				return err
			}
			sleepFor := RetryBackoff(s.cfg.BaseSleep, attempt)
			fmt.Printf("[clickhouse checkpoint retry] attempt=%d/%d sleep=%s err=%s\n", attempt, maxRetries, sleepFor, shortClickHouseError(err))
			if waitErr := SleepContext(ctx, sleepFor); waitErr != nil {
				return fmt.Errorf("clickhouse checkpoint retry wait stopped: %w; last_error=%s", waitErr, shortClickHouseError(lastErr))
			}
		}
	}
	return lastErr
}

func enqueueClickHouseDirectOutbox(ctx context.Context, cfg *Config, ch *ClickHouseClient, database, table string, columns []string, payload []byte, rowCount int, sourceErr error) error {
	outboxDB := clickHouseOutboxDatabase(cfg)
	outboxTable := clickHouseOutboxTable(cfg)
	chunks := clickHouseDirectOutboxPayloadChunks(payload, rowCount, clickHouseOutboxChunkRows(cfg), clickHouseOutboxChunkBytes(cfg))
	for idx, chunk := range chunks {
		payloadHash := H64(database + "\x1f" + table + "\x1f" + strings.Join(columns, "\x1f") + "\x1f" + string(chunk.payload))
		if exists, err := pendingClickHouseDirectOutboxExistsWithTimeout(ctx, cfg, ch, outboxDB, outboxTable, database, table, payloadHash); err == nil && exists {
			fmt.Printf("[clickhouse] direct outbox already has pending chunk target=%s.%s rows=%d chunk=%d/%d hash=%d\n",
				database, table, chunk.rowCount, idx+1, len(chunks), payloadHash)
			continue
		}
		outboxUUID, err := UUIDv7()
		if err != nil {
			return err
		}
		row := map[string]any{
			"outbox_uuid":     outboxUUID,
			"created_at":      FormatClickHouseDateTime64Millis(UTCNow(), clickHouseAsiaSeoulLocation),
			"target_database": database,
			"target_table":    table,
			"target_columns":  columns,
			"rows_json":       string(chunk.payload),
			"row_count":       chunk.rowCount,
			"payload_hash":    payloadHash,
			"source_error":    sanitizeClickHouseError(cfg, sourceErr),
		}
		body, err := clickHouseJSONEachRowPayload(polymarketDirectInsertOutboxColumns, []map[string]any{row})
		if err != nil {
			return err
		}
		query := clickHouseInsertQuery(clickHouseTableName(outboxDB, outboxTable), polymarketDirectInsertOutboxColumns, body)
		if err := execClickHouseOutboxInsertWithRetry(ctx, cfg, ch, query, outboxDB, outboxTable, database, table, payloadHash); err != nil {
			return err
		}
		fmt.Printf("[clickhouse] queued direct outbox target=%s.%s rows=%d chunk=%d/%d hash=%d reason=%s\n",
			database, table, chunk.rowCount, idx+1, len(chunks), payloadHash, sanitizeClickHouseError(cfg, sourceErr))
	}
	return nil
}

type clickHouseDirectOutboxPayloadChunk struct {
	payload  []byte
	rowCount int
}

func clickHouseDirectOutboxPayloadChunks(payload []byte, rowCount, maxRows, maxBytes int) []clickHouseDirectOutboxPayloadChunk {
	if len(payload) == 0 {
		return nil
	}
	if maxRows <= 0 {
		maxRows = rowCount
	}
	if maxRows <= 0 {
		maxRows = 1
	}
	if maxBytes <= 0 {
		maxBytes = len(payload) + 1
	}
	trimmed := bytes.TrimRight(payload, "\n")
	if len(trimmed) == 0 {
		return []clickHouseDirectOutboxPayloadChunk{{payload: payload, rowCount: rowCount}}
	}
	lines := bytes.Split(trimmed, []byte{'\n'})
	if len(lines) <= maxRows && len(payload) <= maxBytes {
		return []clickHouseDirectOutboxPayloadChunk{{payload: payload, rowCount: len(lines)}}
	}
	chunks := make([]clickHouseDirectOutboxPayloadChunk, 0, (len(lines)+maxRows-1)/maxRows)
	current := bytes.Buffer{}
	currentRows := 0
	for _, line := range lines {
		lineLen := len(line) + 1
		if currentRows > 0 && (currentRows >= maxRows || current.Len()+lineLen > maxBytes) {
			chunks = append(chunks, clickHouseDirectOutboxPayloadChunk{payload: append([]byte(nil), current.Bytes()...), rowCount: currentRows})
			current.Reset()
			currentRows = 0
		}
		current.Write(line)
		current.WriteByte('\n')
		currentRows++
	}
	if currentRows > 0 {
		chunks = append(chunks, clickHouseDirectOutboxPayloadChunk{payload: append([]byte(nil), current.Bytes()...), rowCount: currentRows})
	}
	return chunks
}

func execClickHouseOutboxInsertWithRetry(ctx context.Context, cfg *Config, ch *ClickHouseClient, query, outboxDB, outboxTable, database, table string, payloadHash uint64) error {
	maxRetries := 1
	if cfg != nil {
		maxRetries = maxInt(1, cfg.MaxRetries)
	}
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		insertCtx, cancel := context.WithTimeout(ctx, clickHouseOutboxInsertTimeout(cfg))
		_, err := ch.Exec(insertCtx, query)
		cancel()
		if err == nil {
			if attempt > 1 {
				fmt.Printf("[clickhouse outbox retry] succeeded target=%s.%s hash=%d attempt=%d\n", database, table, payloadHash, attempt)
			}
			return nil
		}
		lastErr = err
		if exists, existsErr := pendingClickHouseDirectOutboxExistsWithTimeout(ctx, cfg, ch, outboxDB, outboxTable, database, table, payloadHash); existsErr == nil && exists {
			fmt.Printf("[clickhouse] direct outbox enqueue confirmed after retryable response target=%s.%s hash=%d\n", database, table, payloadHash)
			return nil
		}
		if ctx.Err() != nil || !IsRetryableInsertError(err) || attempt >= maxRetries {
			return err
		}
		baseSleep := 200 * time.Millisecond
		if cfg != nil && cfg.BaseSleep > 0 {
			baseSleep = cfg.BaseSleep
		}
		sleepFor := RetryBackoff(baseSleep, attempt)
		fmt.Printf("[clickhouse outbox retry] target=%s.%s hash=%d attempt=%d/%d sleep=%s err=%s\n",
			database, table, payloadHash, attempt, maxRetries, sleepFor, shortClickHouseError(err))
		if err := SleepContext(ctx, sleepFor); err != nil {
			return fmt.Errorf("clickhouse outbox retry wait stopped: %w; last_error=%s", err, shortClickHouseError(lastErr))
		}
	}
	return lastErr
}

func pendingClickHouseDirectOutboxExistsWithTimeout(ctx context.Context, cfg *Config, ch *ClickHouseClient, outboxDB, outboxTable, database, table string, payloadHash uint64) (bool, error) {
	probeTimeout := minDuration(15*time.Second, clickHouseOutboxInsertTimeout(cfg))
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	return pendingClickHouseDirectOutboxExists(probeCtx, ch, outboxDB, outboxTable, database, table, payloadHash)
}

func pendingClickHouseDirectOutboxExists(ctx context.Context, ch *ClickHouseClient, outboxDB, outboxTable, database, table string, payloadHash uint64) (bool, error) {
	query := fmt.Sprintf(`
SELECT count() AS c
FROM %s
WHERE target_database = %s
  AND target_table = %s
  AND payload_hash = %d
  AND replayed_at IS NULL
LIMIT 1
FORMAT JSONEachRow
`, clickHouseTableName(outboxDB, outboxTable), QuoteSQLString(database), QuoteSQLString(table), payloadHash)
	rows, err := clickHouseQueryRows(ctx, ch, query)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	if count := SafeUint64(rows[0]["c"]); count != nil && *count > 0 {
		return true, nil
	}
	return false, nil
}

func drainClickHouseDirectOutbox(ctx context.Context, cfg *Config, ch *ClickHouseClient) (int, error) {
	limit := cfg.ClickHouseOutboxReplayLimit
	if limit <= 0 {
		return 0, nil
	}
	outboxDB := clickHouseOutboxDatabase(cfg)
	outboxTable := clickHouseOutboxTable(cfg)
	query := fmt.Sprintf(`
SELECT outbox_uuid, target_database, target_table, target_columns, rows_json, row_count
FROM %s
WHERE replayed_at IS NULL
ORDER BY created_at ASC, outbox_uuid ASC
LIMIT %d
FORMAT JSONEachRow
`, clickHouseTableName(outboxDB, outboxTable), limit)
	rows, err := clickHouseQueryRows(ctx, ch, query)
	if err != nil {
		return 0, err
	}
	drained := 0
	for _, row := range rows {
		outboxUUID := strings.TrimSpace(SafeString(row["outbox_uuid"]))
		database := strings.TrimSpace(SafeString(row["target_database"]))
		table := strings.TrimSpace(SafeString(row["target_table"]))
		columns := clickHouseStringSlice(row["target_columns"])
		payload := []byte(SafeString(row["rows_json"]))
		rowCount := clickHouseInt(row["row_count"])
		if outboxUUID == "" || database == "" || table == "" || len(columns) == 0 || len(payload) == 0 {
			continue
		}
		replayQuery := clickHouseInsertQuery(clickHouseTableName(database, table), columns, payload)
		if _, err := ch.Exec(ctx, replayQuery); err != nil {
			fmt.Printf("[warn] clickhouse direct outbox replay failed uuid=%s target=%s.%s rows=%d error=%s\n",
				outboxUUID, database, table, rowCount, sanitizeClickHouseError(cfg, err))
			_ = markClickHouseDirectOutboxReplay(ctx, cfg, ch, outboxDB, outboxTable, outboxUUID, false, err)
			if IsRetryableInsertError(err) {
				break
			}
			continue
		}
		if err := markClickHouseDirectOutboxReplay(ctx, cfg, ch, outboxDB, outboxTable, outboxUUID, true, nil); err != nil {
			fmt.Printf("[warn] clickhouse direct outbox replay mark failed uuid=%s error=%s\n", outboxUUID, sanitizeClickHouseError(cfg, err))
		}
		drained++
		fmt.Printf("[clickhouse] direct outbox replayed uuid=%s target=%s.%s rows=%d\n", outboxUUID, database, table, rowCount)
	}
	return drained, nil
}

func markClickHouseDirectOutboxReplay(ctx context.Context, cfg *Config, ch *ClickHouseClient, outboxDB, outboxTable, outboxUUID string, success bool, replayErr error) error {
	var query string
	if success {
		query = fmt.Sprintf(`
ALTER TABLE %s
UPDATE replay_attempt = replay_attempt + 1,
       last_replay_at = now64(3, 'Asia/Seoul'),
       replayed_at = now64(3, 'Asia/Seoul'),
       replay_error = ''
WHERE outbox_uuid = toUUID(%s)
SETTINGS mutations_sync = 1
`, clickHouseTableName(outboxDB, outboxTable), QuoteSQLString(outboxUUID))
	} else {
		query = fmt.Sprintf(`
ALTER TABLE %s
UPDATE replay_attempt = replay_attempt + 1,
       last_replay_at = now64(3, 'Asia/Seoul'),
       replay_error = %s
WHERE outbox_uuid = toUUID(%s)
SETTINGS mutations_sync = 1
`, clickHouseTableName(outboxDB, outboxTable), QuoteSQLString(sanitizeClickHouseError(cfg, replayErr)), QuoteSQLString(outboxUUID))
	}
	markCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := ch.Exec(markCtx, strings.TrimSpace(query))
	return err
}

func (i *Ingestor) clickHouseSnapshotTable(entity string) string {
	table := clickHouseSnapshotTableBase(entity)
	if table == "" {
		return ""
	}
	return clickHouseTableName(i.cfg.ClickHouseDatabase, table)
}

func clickHouseSnapshotTableBase(entity string) string {
	switch entity {
	case "events":
		return "polymarket_event_snapshot"
	case "markets":
		return "polymarket_market_snapshot"
	case "series":
		return "polymarket_series_snapshot"
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

func clickHouseQueryRows(ctx context.Context, ch *ClickHouseClient, query string) ([]map[string]any, error) {
	body, err := ch.Exec(ctx, query)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	rows := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(line))
		dec.UseNumber()
		var row map[string]any
		if err := dec.Decode(&row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func clickHouseStringSlice(value any) []string {
	switch v := value.(type) {
	case nil:
		return nil
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s := strings.TrimSpace(SafeString(item))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		parts := splitCSV(v)
		if len(parts) == 0 {
			return nil
		}
		return parts
	default:
		return nil
	}
}

func clickHouseInt(value any) int {
	if u := SafeUint64(value); u != nil {
		return int(*u)
	}
	return 0
}

func clickHouseOutboxDatabase(cfg *Config) string {
	if cfg != nil && strings.TrimSpace(cfg.ClickHouseOutboxDatabase) != "" {
		return strings.TrimSpace(cfg.ClickHouseOutboxDatabase)
	}
	return "Data_Prediction_Polymarket_Log"
}

func clickHouseOutboxTable(cfg *Config) string {
	if cfg != nil && strings.TrimSpace(cfg.ClickHouseOutboxTable) != "" {
		return strings.TrimSpace(cfg.ClickHouseOutboxTable)
	}
	return "polymarket_direct_insert_outbox"
}

func clickHouseOutboxInsertTimeout(cfg *Config) time.Duration {
	if cfg != nil && cfg.ClickHouseOutboxInsertTimeout > 0 {
		return cfg.ClickHouseOutboxInsertTimeout
	}
	return 90 * time.Second
}

func clickHouseOutboxChunkRows(cfg *Config) int {
	if cfg != nil && cfg.ClickHouseOutboxChunkRows > 0 {
		return cfg.ClickHouseOutboxChunkRows
	}
	return 100
}

func clickHouseOutboxChunkBytes(cfg *Config) int {
	if cfg != nil && cfg.ClickHouseOutboxChunkBytes > 0 {
		return cfg.ClickHouseOutboxChunkBytes
	}
	return 512 * 1024
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func shortClickHouseError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.Join(strings.Fields(err.Error()), " ")
	return truncateString(msg, 280)
}

func sanitizeClickHouseError(cfg *Config, err error) string {
	if err == nil {
		return ""
	}
	text := strings.TrimSpace(err.Error())
	if cfg != nil {
		for _, secret := range []string{cfg.ClickHousePassword, cfg.KafkaPassword, cfg.GHToken} {
			secret = strings.TrimSpace(secret)
			if secret != "" {
				text = strings.ReplaceAll(text, secret, "***")
			}
		}
	}
	text = strings.Join(strings.Fields(text), " ")
	return truncateString(text, 400)
}
