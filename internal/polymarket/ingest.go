package polymarket

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type Ingestor struct {
	cfg   *Config
	api   *HTTPJSONClient
	state StateStore
	ch    *ClickHouseClient
}

type FetchResult struct {
	TotalWritten  int
	NewCheckpoint string
	Completed     bool
	StopReason    string
}

func NewIngestor(cfg *Config) (*Ingestor, error) {
	var ch *ClickHouseClient
	if cfg.UsesClickHouseIngest() || cfg.UsesClickHouseState() {
		ch = NewClickHouseClient(cfg)
	}
	return &Ingestor{
		cfg:   cfg,
		api:   NewHTTPJSONClient(cfg.RequestTimeout, cfg.ConnectTimeout, "statground-polymarket-crawler"),
		state: NewStateStore(cfg),
		ch:    ch,
	}, nil
}

func (i *Ingestor) PickOrder(ctx context.Context, entity string) (string, error) {
	url := i.cfg.Endpoint(entity)
	orders := []string{i.cfg.OrderPrimary, i.cfg.OrderFallback}
	for _, order := range orders {
		data, _, err := i.api.SafeGetJSON(ctx, url, map[string]string{
			"limit":     fmt.Sprint(i.cfg.PageLimitForEntity(entity)),
			"offset":    "0",
			"order":     order,
			"ascending": "false",
		}, i.cfg.MaxRetries, i.cfg.BaseSleep)
		if err != nil {
			continue
		}
		items := ExtractItems(data, entity)
		if len(items) > 0 || data != nil {
			return order, nil
		}
	}
	return "", fmt.Errorf("failed to fetch %s: API returned non-list for both orders", entity)
}

func (i *Ingestor) FetchAndPublish(ctx context.Context, entity string, checkpoint map[string]string, stopAt *time.Time) (FetchResult, error) {
	if checkpoint == nil {
		checkpoint = map[string]string{}
	}
	lastCP := checkpoint[entity]
	bestSeen := lastCP

	orderUsed, err := i.PickOrder(ctx, entity)
	if err != nil {
		return FetchResult{}, err
	}
	useKeyset := i.cfg.UseKeysetPagination && supportsKeysetPagination(entity)
	cursor := ""
	if useKeyset && canResumeIngestCursor(checkpoint, entity, lastCP, orderUsed) {
		cursor = checkpoint[ingestCursorKey(entity)]
		bestSeen = MaxTimeISO(bestSeen, checkpoint[ingestBestSeenKey(entity)])
		fmt.Printf("[RESUME] %s keyset_cursor=%s checkpoint=%s best_seen=%s\n",
			entity, shortCursor(cursor), defaultDisplay(lastCP, "(none)"), defaultDisplay(bestSeen, "(none)"))
	}

	fmt.Printf("[FETCH] %s order=%s checkpoint=%s pagination=%s\n",
		entity, orderUsed, defaultDisplay(lastCP, "(none)"), paginationModeLabel(useKeyset))

	totalWritten := 0
	rows := make([]map[string]any, 0, i.cfg.InsertBatchSizeForEntity(entity))
	effectiveMaxPages := i.cfg.MaxPages
	if strings.TrimSpace(lastCP) == "" && i.cfg.MaxPagesNoCheckpoint > 0 {
		effectiveMaxPages = minInt(effectiveMaxPages, i.cfg.MaxPagesNoCheckpoint)
		fmt.Printf("[BOOTSTRAP-CAP] %s checkpoint is empty; max_pages capped to %d by MAX_PAGES_NO_CHECKPOINT to keep GitHub Actions bounded. Override only for manual backfill.\n", entity, effectiveMaxPages)
	}

	result := FetchResult{Completed: false, StopReason: "max_pages"}
	lastNextCursor := ""
	for page := 0; page < effectiveMaxPages; page++ {
		if ingestSoftDeadlineReached(stopAt) {
			result.StopReason = "soft_deadline"
			break
		}
		if i.cfg.IngestMaxObjectsPerEntity > 0 && totalWritten >= i.cfg.IngestMaxObjectsPerEntity {
			result.StopReason = "max_objects_per_entity"
			break
		}

		pageData, err := i.FetchEntityPage(ctx, entity, orderUsed, page, cursor)
		if err != nil {
			if useKeyset {
				fmt.Printf("[STOP] %s fetch failed at cursor=%s err=%v\n", entity, shortCursor(cursor), err)
			} else {
				fmt.Printf("[STOP] %s fetch failed at offset=%d err=%v\n", entity, page*i.cfg.PageLimitForEntity(entity), err)
			}
			result.StopReason = "fetch_error"
			break
		}
		items := pageData.Items
		lastNextCursor = pageData.NextCursor
		if len(items) == 0 {
			result.Completed = true
			result.StopReason = "empty_page"
			break
		}

		stopByCheckpoint := false
		collectedAt := wrapTime(UTCNow())
		for _, obj := range items {
			updated := SafeString(firstNonNil(obj["updatedAt"], obj["updated_at"]))
			if lastCP != "" && stringsEqualFold(orderUsed, "updatedAt") && ISOLEQ(updated, lastCP) {
				stopByCheckpoint = true
				result.Completed = true
				result.StopReason = "checkpoint"
				break
			}
			rawKey, err := UUIDv7()
			if err != nil {
				return result, err
			}
			row, err := BuildEntityRow(entity, NormalizeRawJSON(obj), collectedAt, rawKey)
			if err != nil {
				return result, err
			}
			if row != nil {
				rows = append(rows, row)
				totalWritten++
			}
			bestSeen = MaxTimeISO(bestSeen, updated)
		}

		if err := i.FlushEntityRows(ctx, entity, &rows, false); err != nil {
			return result, err
		}

		if useKeyset {
			fmt.Printf("[%s] page=%d cursor=%s next_cursor=%s written=%d http_status=%d\n",
				entity, page+1, shortCursor(pageData.CursorIn), shortCursor(pageData.NextCursor), totalWritten, pageData.Meta.HTTPStatus)
		} else {
			fmt.Printf("[%s] page=%d offset=%d written=%d http_status=%d\n", entity, page+1, pageData.Offset, totalWritten, pageData.Meta.HTTPStatus)
		}

		if stopByCheckpoint {
			fmt.Printf("[%s] reached checkpoint -> stop\n", entity)
			break
		}
		if len(items) < i.cfg.PageLimitForEntity(entity) {
			result.Completed = true
			result.StopReason = "last_page"
			break
		}
		if useKeyset && strings.TrimSpace(pageData.NextCursor) == "" {
			result.Completed = true
			result.StopReason = "last_page_no_cursor"
			break
		}

		if useKeyset {
			cursor = pageData.NextCursor
			setIngestResumeState(checkpoint, entity, lastCP, orderUsed, cursor, bestSeen)
			if err := i.state.SaveCheckpoint(ctx, checkpoint); err != nil {
				return result, err
			}
		}

		if ingestSoftDeadlineReached(stopAt) {
			result.StopReason = "soft_deadline"
			break
		}
		if err := SleepContext(ctx, i.cfg.BaseSleep); err != nil {
			return result, err
		}
	}

	if err := i.FlushEntityRows(ctx, entity, &rows, true); err != nil {
		return result, err
	}

	if result.Completed {
		clearIngestResumeState(checkpoint, entity)
		result.NewCheckpoint = bestSeen
	} else if useKeyset && strings.TrimSpace(lastNextCursor) != "" {
		setIngestResumeState(checkpoint, entity, lastCP, orderUsed, lastNextCursor, bestSeen)
	} else {
		clearIngestResumeState(checkpoint, entity)
	}
	if !result.Completed {
		fmt.Printf("[PARTIAL] %s stopped before checkpoint completion stop_reason=%s checkpoint_not_advanced=%s resume_cursor=%s\n",
			entity, result.StopReason, defaultDisplay(lastCP, "(none)"), shortCursor(checkpoint[ingestCursorKey(entity)]))
	}
	return FetchResult{TotalWritten: totalWritten, NewCheckpoint: result.NewCheckpoint, Completed: result.Completed, StopReason: result.StopReason}, nil
}

func RunIngest() error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	if !cfg.UsesClickHouseState() {
		if err := EnsureStatePath(cfg); err != nil {
			return err
		}
	}
	ingestor, err := NewIngestor(cfg)
	if err != nil {
		return err
	}

	fmt.Printf("[CONFIG] ingest_mode=%s clickhouse_database=%s state_backend=%s clickhouse_direct_outbox=%t clickhouse_outbox_chunk_rows=%d clickhouse_outbox_chunk_bytes=%d clickhouse_outbox_insert_timeout_seconds=%.0f entities=%s max_pages=%d max_pages_no_checkpoint=%d ingest_max_objects_per_entity=%d run_soft_deadline_seconds=%.0f use_keyset_pagination=%t batch_size_default=%d batch_size_events=%d batch_size_markets=%d batch_size_series=%d\n",
		cfg.IngestMode,
		cfg.ClickHouseDatabase,
		cfg.StateBackend,
		cfg.ClickHouseDirectOutboxFallback,
		clickHouseOutboxChunkRows(cfg),
		clickHouseOutboxChunkBytes(cfg),
		clickHouseOutboxInsertTimeout(cfg).Seconds(),
		joinCSV(cfg.Entities),
		cfg.MaxPages,
		cfg.MaxPagesNoCheckpoint,
		cfg.IngestMaxObjectsPerEntity,
		cfg.RunSoftDeadline.Seconds(),
		cfg.UseKeysetPagination,
		cfg.InsertBatchSize,
		cfg.InsertBatchSizeForEntity("events"),
		cfg.InsertBatchSizeForEntity("markets"),
		cfg.InsertBatchSizeForEntity("series"),
	)

	ctx := context.Background()
	if cfg.UsesClickHouseIngest() {
		if err := ingestor.ValidateClickHouseIngest(ctx); err != nil {
			return err
		}
	} else {
		if err := ingestor.ValidateKafkaIngest(ctx); err != nil {
			return err
		}
	}
	checkpoint, err := ingestor.state.LoadCheckpoint(ctx)
	if err != nil {
		return err
	}
	if checkpoint == nil {
		checkpoint = map[string]string{}
	}

	var stopAt *time.Time
	if cfg.RunSoftDeadline > 0 {
		deadline := UTCNow().Add(cfg.RunSoftDeadline)
		stopAt = &deadline
		fmt.Printf("[DEADLINE] ingest soft deadline at %s; crawler will stop cleanly before the GitHub Actions hard timeout.\n", FormatISO8601UTC(deadline))
	}

	wroteTotal := 0
	for _, entity := range cfg.Entities {
		if ingestSoftDeadlineReached(stopAt) {
			fmt.Printf("[STOP] soft deadline reached before entity=%s; remaining entities skipped cleanly.\n", entity)
			break
		}
		res, err := ingestor.FetchAndPublish(ctx, entity, checkpoint, stopAt)
		if err != nil {
			return err
		}
		wroteTotal += res.TotalWritten
		if res.Completed && res.NewCheckpoint != "" {
			checkpoint[entity] = res.NewCheckpoint
			clearIngestResumeState(checkpoint, entity)
			if err := ingestor.state.SaveCheckpoint(ctx, checkpoint); err != nil {
				return err
			}
			if cfg.UsesKafkaIngest() {
				if err := ingestor.PublishCheckpoint(ctx, publicEntityCheckpoint(checkpoint)); err != nil {
					return err
				}
			}
			fmt.Printf("[CHECKPOINT] %s advanced to %s stop_reason=%s\n", entity, res.NewCheckpoint, res.StopReason)
		} else {
			if err := ingestor.state.SaveCheckpoint(ctx, checkpoint); err != nil {
				return err
			}
		}
	}

	if err := ingestor.state.SaveCheckpoint(ctx, checkpoint); err != nil {
		return err
	}
	fmt.Printf("\nDONE. written_objects=%d\n", wroteTotal)
	return nil
}

func defaultDisplay(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func firstNonNil(values ...any) any {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

func stringsEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return strings.ToLower(a) == strings.ToLower(b)
}

func toISO(t time.Time) string {
	return FormatISO8601UTC(t)
}

func ingestSoftDeadlineReached(stopAt *time.Time) bool {
	return stopAt != nil && !UTCNow().Before(*stopAt)
}

func paginationModeLabel(useKeyset bool) string {
	if useKeyset {
		return "keyset"
	}
	return "offset"
}

func publicEntityCheckpoint(checkpoint map[string]string) map[string]string {
	out := map[string]string{}
	for _, entity := range []string{"events", "markets", "series"} {
		if v := strings.TrimSpace(checkpoint[entity]); v != "" {
			out[entity] = v
		}
	}
	return out
}
