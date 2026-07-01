package polymarket

import (
	"context"
	"fmt"
	"time"
)

type RefreshEntityReport struct {
	Mode            string `json:"mode"`
	WrittenObjects  int    `json:"written_objects"`
	RefreshUntilUTC string `json:"refresh_until_utc,omitempty"`
	OrderUsed       string `json:"order_used,omitempty"`
	PagesRead       int    `json:"pages_read,omitempty"`
	StopReason      string `json:"stop_reason,omitempty"`
	NewestSeenUTC   string `json:"newest_seen_utc,omitempty"`
	OldestSeenUTC   string `json:"oldest_seen_utc,omitempty"`
}

type RefreshReport struct {
	RunAtUTC            string                         `json:"run_at_utc"`
	LookbackHours       int                            `json:"lookback_hours"`
	Entities            map[string]RefreshEntityReport `json:"entities"`
	WrittenObjectsTotal int                            `json:"written_objects_total"`
}

type refreshWindowResult struct {
	WrittenObjects int
	OrderUsed      string
	PagesRead      int
	StopReason     string
	NewestSeenUTC  string
	OldestSeenUTC  string
}

func fetchRefreshWindow(ctx context.Context, ingestor *Ingestor, entity string, refreshUntilISO string, stopAt *time.Time) (refreshWindowResult, error) {
	orderUsed, err := ingestor.PickOrder(ctx, entity)
	if err != nil {
		return refreshWindowResult{}, err
	}

	result := refreshWindowResult{OrderUsed: orderUsed, StopReason: "max_pages"}
	rows := make([]map[string]any, 0, ingestor.cfg.InsertBatchSizeForEntity(entity))
	canStopByLookback := stringsEqualFold(orderUsed, "updatedAt") || stringsEqualFold(orderUsed, "updated_at")
	if !canStopByLookback {
		fmt.Printf("[WARN] %s order=%s is not updatedAt; lookback cutoff will not be used for early stop. max_pages=%d bounds this run.\n", entity, orderUsed, ingestor.cfg.MaxPages)
	}
	useKeyset := ingestor.cfg.UseKeysetPagination && supportsKeysetPagination(entity)
	cursor := ""

	for page := 0; page < ingestor.cfg.MaxPages; page++ {
		if refreshSoftDeadlineReached(stopAt) {
			result.StopReason = "soft_deadline"
			break
		}
		if ingestor.cfg.RefreshMaxObjectsPerEntity > 0 && result.WrittenObjects >= ingestor.cfg.RefreshMaxObjectsPerEntity {
			result.StopReason = "max_objects_per_entity"
			break
		}

		pageData, err := ingestor.FetchEntityPage(ctx, entity, orderUsed, page, cursor)
		if err != nil {
			if useKeyset {
				fmt.Printf("[STOP] %s fetch failed at cursor=%s err=%v\n", entity, shortCursor(cursor), err)
			} else {
				fmt.Printf("[STOP] %s fetch failed at offset=%d err=%v\n", entity, page*ingestor.cfg.PageLimitForEntity(entity), err)
			}
			result.StopReason = "fetch_error"
			break
		}
		items := pageData.Items
		if len(items) == 0 {
			result.StopReason = "empty_page"
			break
		}

		result.PagesRead++
		collectedAt := wrapTime(UTCNow())
		stopByLookback := false
		stopByMaxObjects := false
		pageNewest := ""
		pageOldest := ""

		for _, obj := range items {
			updated := refreshObjectUpdatedISO(obj)
			pageNewest = MaxTimeISO(pageNewest, updated)
			pageOldest = MinTimeISO(pageOldest, updated)
			result.NewestSeenUTC = MaxTimeISO(result.NewestSeenUTC, updated)
			result.OldestSeenUTC = MinTimeISO(result.OldestSeenUTC, updated)

			if canStopByLookback && updated != "" && ISOLEQ(updated, refreshUntilISO) {
				stopByLookback = true
				result.StopReason = "lookback_cutoff"
				break
			}
			if ingestor.cfg.RefreshMaxObjectsPerEntity > 0 && result.WrittenObjects >= ingestor.cfg.RefreshMaxObjectsPerEntity {
				stopByMaxObjects = true
				result.StopReason = "max_objects_per_entity"
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
				result.WrittenObjects++
			}
		}
		if err := ingestor.FlushEntityRows(ctx, entity, &rows, false); err != nil {
			return result, err
		}
		if useKeyset {
			fmt.Printf("[%s] page=%d cursor=%s next_cursor=%s written=%d http_status=%d page_newest=%s page_oldest=%s\n",
				entity, page+1, shortCursor(pageData.CursorIn), shortCursor(pageData.NextCursor), result.WrittenObjects, pageData.Meta.HTTPStatus, defaultDisplay(pageNewest, "(unknown)"), defaultDisplay(pageOldest, "(unknown)"))
		} else {
			fmt.Printf("[%s] page=%d offset=%d written=%d http_status=%d page_newest=%s page_oldest=%s\n",
				entity, page+1, pageData.Offset, result.WrittenObjects, pageData.Meta.HTTPStatus, defaultDisplay(pageNewest, "(unknown)"), defaultDisplay(pageOldest, "(unknown)"))
		}

		if stopByLookback || stopByMaxObjects {
			break
		}
		if len(items) < ingestor.cfg.PageLimitForEntity(entity) {
			result.StopReason = "last_page"
			break
		}
		if useKeyset {
			if pageData.NextCursor == "" {
				result.StopReason = "last_page_no_cursor"
				break
			}
			cursor = pageData.NextCursor
		}
		if refreshSoftDeadlineReached(stopAt) {
			result.StopReason = "soft_deadline"
			break
		}
		if err := SleepContext(ctx, ingestor.cfg.BaseSleep); err != nil {
			return result, err
		}
	}

	if err := ingestor.FlushEntityRows(ctx, entity, &rows, true); err != nil {
		return result, err
	}
	return result, nil
}

func RunRefresh() error {
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

	fmt.Printf("[CONFIG] ingest_mode=%s clickhouse_database=%s state_backend=%s clickhouse_direct_outbox=%t clickhouse_outbox_chunk_rows=%d clickhouse_outbox_chunk_bytes=%d clickhouse_outbox_insert_timeout_seconds=%.0f entities=%s max_pages=%d lookback_hours=%d refresh_max_objects_per_entity=%d run_soft_deadline_seconds=%.0f use_keyset_pagination=%t batch_size_default=%d batch_size_events=%d batch_size_markets=%d batch_size_series=%d\n",
		cfg.IngestMode,
		cfg.ClickHouseDatabase,
		cfg.StateBackend,
		cfg.ClickHouseDirectOutboxFallback,
		clickHouseOutboxChunkRows(cfg),
		clickHouseOutboxChunkBytes(cfg),
		clickHouseOutboxInsertTimeout(cfg).Seconds(),
		joinCSV(cfg.Entities),
		cfg.MaxPages,
		cfg.LookbackHours,
		cfg.RefreshMaxObjectsPerEntity,
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
	report := RefreshReport{
		RunAtUTC:      FormatISO8601UTC(UTCNow()),
		LookbackHours: cfg.LookbackHours,
		Entities:      make(map[string]RefreshEntityReport),
	}

	var stopAt *time.Time
	if cfg.RunSoftDeadline > 0 {
		deadline := UTCNow().Add(cfg.RunSoftDeadline)
		stopAt = &deadline
		fmt.Printf("[DEADLINE] refresh soft deadline at %s; crawler will stop cleanly before the GitHub Actions hard timeout.\n", FormatISO8601UTC(deadline))
	}

	refreshUntil := UTCNow().Add(-time.Duration(cfg.LookbackHours) * time.Hour)
	refreshUntilISO := FormatISO8601UTC(refreshUntil)
	wroteTotal := 0
	for _, entity := range cfg.Entities {
		if refreshSoftDeadlineReached(stopAt) {
			fmt.Printf("[STOP] soft deadline reached before entity=%s; remaining entities skipped cleanly.\n", entity)
			report.Entities[entity] = RefreshEntityReport{
				Mode:            "lookback_refresh_clickhouse_direct",
				RefreshUntilUTC: refreshUntilISO,
				StopReason:      "soft_deadline_before_entity",
			}
			continue
		}

		fmt.Printf("[REFRESH] %s: refresh_until=%s\n", entity, refreshUntilISO)
		res, err := fetchRefreshWindow(ctx, ingestor, entity, refreshUntilISO, stopAt)
		if err != nil {
			return err
		}
		wroteTotal += res.WrittenObjects
		report.Entities[entity] = RefreshEntityReport{
			Mode:            "lookback_refresh_clickhouse_direct",
			WrittenObjects:  res.WrittenObjects,
			RefreshUntilUTC: refreshUntilISO,
			OrderUsed:       res.OrderUsed,
			PagesRead:       res.PagesRead,
			StopReason:      res.StopReason,
			NewestSeenUTC:   res.NewestSeenUTC,
			OldestSeenUTC:   res.OldestSeenUTC,
		}
		fmt.Printf("[REFRESH-DONE] %s written=%d pages=%d stop_reason=%s newest_seen=%s oldest_seen=%s\n",
			entity, res.WrittenObjects, res.PagesRead, res.StopReason, defaultDisplay(res.NewestSeenUTC, "(unknown)"), defaultDisplay(res.OldestSeenUTC, "(unknown)"))
	}
	report.WrittenObjectsTotal = wroteTotal

	fmt.Printf("\nDONE. written_objects_total=%d\n", wroteTotal)
	return nil
}

func refreshObjectUpdatedISO(obj map[string]any) string {
	for _, key := range []string{"updatedAt", "updated_at", "updated_at_utc"} {
		raw := SafeString(obj[key])
		if t := ParseISOUTC(raw); t != nil {
			return FormatISO8601UTC(*t)
		}
	}
	return ""
}

func refreshSoftDeadlineReached(stopAt *time.Time) bool {
	return stopAt != nil && !UTCNow().Before(*stopAt)
}
