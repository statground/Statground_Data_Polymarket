package polymarket

import (
	"context"
	"fmt"
	"time"
)

type RefreshEntityReport struct {
	Mode             string `json:"mode"`
	PublishedObjects int    `json:"published_objects"`
	RefreshUntilUTC  string `json:"refresh_until_utc,omitempty"`
	OrderUsed        string `json:"order_used,omitempty"`
	PagesRead        int    `json:"pages_read,omitempty"`
	StopReason       string `json:"stop_reason,omitempty"`
	NewestSeenUTC    string `json:"newest_seen_utc,omitempty"`
	OldestSeenUTC    string `json:"oldest_seen_utc,omitempty"`
}

type RefreshReport struct {
	RunAtUTC              string                         `json:"run_at_utc"`
	LookbackHours         int                            `json:"lookback_hours"`
	Entities              map[string]RefreshEntityReport `json:"entities"`
	PublishedObjectsTotal int                            `json:"published_objects_total"`
}

type refreshWindowResult struct {
	PublishedObjects int
	OrderUsed        string
	PagesRead        int
	StopReason       string
	NewestSeenUTC    string
	OldestSeenUTC    string
}

func fetchRefreshWindow(ctx context.Context, ingestor *Ingestor, entity string, refreshUntilISO string, stopAt *time.Time) (refreshWindowResult, error) {
	orderUsed, err := ingestor.PickOrder(ctx, entity)
	if err != nil {
		return refreshWindowResult{}, err
	}

	result := refreshWindowResult{OrderUsed: orderUsed, StopReason: "max_pages"}
	rows := make([]map[string]any, 0, ingestor.cfg.InsertBatchSizeForEntity(entity))
	url := ingestor.cfg.Endpoint(entity)
	canStopByLookback := stringsEqualFold(orderUsed, "updatedAt") || stringsEqualFold(orderUsed, "updated_at")
	if !canStopByLookback {
		fmt.Printf("[WARN] %s order=%s is not updatedAt; lookback cutoff will not be used for early stop. max_pages=%d bounds this run.\n", entity, orderUsed, ingestor.cfg.MaxPages)
	}

	for page := 0; page < ingestor.cfg.MaxPages; page++ {
		if refreshSoftDeadlineReached(stopAt) {
			result.StopReason = "soft_deadline"
			break
		}
		if ingestor.cfg.RefreshMaxObjectsPerEntity > 0 && result.PublishedObjects >= ingestor.cfg.RefreshMaxObjectsPerEntity {
			result.StopReason = "max_objects_per_entity"
			break
		}

		offset := page * ingestor.cfg.PageLimit
		data, meta, err := ingestor.api.SafeGetJSON(ctx, url, map[string]string{
			"limit":     fmt.Sprint(ingestor.cfg.PageLimit),
			"offset":    fmt.Sprint(offset),
			"order":     orderUsed,
			"ascending": "false",
		}, ingestor.cfg.MaxRetries, ingestor.cfg.BaseSleep)
		if err != nil {
			fmt.Printf("[STOP] %s fetch failed at offset=%d err=%v\n", entity, offset, err)
			result.StopReason = "fetch_error"
			break
		}
		items := ExtractItems(data, entity)
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
			if ingestor.cfg.RefreshMaxObjectsPerEntity > 0 && result.PublishedObjects >= ingestor.cfg.RefreshMaxObjectsPerEntity {
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
				result.PublishedObjects++
			}
		}
		if err := ingestor.FlushEntityRows(ctx, entity, &rows, false); err != nil {
			return result, err
		}
		fmt.Printf("[%s] page=%d offset=%d published=%d http_status=%d page_newest=%s page_oldest=%s\n",
			entity, page+1, offset, result.PublishedObjects, meta.HTTPStatus, defaultDisplay(pageNewest, "(unknown)"), defaultDisplay(pageOldest, "(unknown)"))

		if stopByLookback || stopByMaxObjects {
			break
		}
		if len(items) < ingestor.cfg.PageLimit {
			result.StopReason = "last_page"
			break
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
	if err := EnsureStatePath(cfg); err != nil {
		return err
	}
	ingestor, err := NewIngestor(cfg)
	if err != nil {
		return err
	}

	fmt.Printf("[CONFIG] ingest_mode=%s kafka_topic=%s entities=%s max_pages=%d lookback_hours=%d refresh_max_objects_per_entity=%d run_soft_deadline_seconds=%.0f batch_size_default=%d batch_size_events=%d batch_size_markets=%d batch_size_series=%d kafka_batch_size=%d kafka_write_chunk_size=%d kafka_batch_bytes=%d kafka_max_message_bytes=%d kafka_max_array_items=%d\n",
		cfg.IngestMode,
		cfg.KafkaTopic,
		joinCSV(cfg.Entities),
		cfg.MaxPages,
		cfg.LookbackHours,
		cfg.RefreshMaxObjectsPerEntity,
		cfg.RunSoftDeadline.Seconds(),
		cfg.InsertBatchSize,
		cfg.InsertBatchSizeForEntity("events"),
		cfg.InsertBatchSizeForEntity("markets"),
		cfg.InsertBatchSizeForEntity("series"),
		cfg.KafkaBatchSize,
		cfg.KafkaWriteChunkSize,
		cfg.KafkaBatchBytes,
		cfg.KafkaMaxMessageBytes,
		cfg.KafkaMaxArrayItems,
	)

	ctx := context.Background()
	if err := ingestor.ValidateKafkaIngest(ctx); err != nil {
		return err
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
				Mode:            "lookback_refresh_without_clickhouse_read",
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
		wroteTotal += res.PublishedObjects
		report.Entities[entity] = RefreshEntityReport{
			Mode:             "lookback_refresh_without_clickhouse_read",
			PublishedObjects: res.PublishedObjects,
			RefreshUntilUTC:  refreshUntilISO,
			OrderUsed:        res.OrderUsed,
			PagesRead:        res.PagesRead,
			StopReason:       res.StopReason,
			NewestSeenUTC:    res.NewestSeenUTC,
			OldestSeenUTC:    res.OldestSeenUTC,
		}
		fmt.Printf("[REFRESH-DONE] %s published=%d pages=%d stop_reason=%s newest_seen=%s oldest_seen=%s\n",
			entity, res.PublishedObjects, res.PagesRead, res.StopReason, defaultDisplay(res.NewestSeenUTC, "(unknown)"), defaultDisplay(res.OldestSeenUTC, "(unknown)"))
	}
	report.PublishedObjectsTotal = wroteTotal

	fmt.Printf("\nDONE. published_objects_total=%d\n", wroteTotal)
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
