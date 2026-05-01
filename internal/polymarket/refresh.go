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
}

type RefreshReport struct {
	RunAtUTC              string                         `json:"run_at_utc"`
	LookbackHours         int                            `json:"lookback_hours"`
	Entities              map[string]RefreshEntityReport `json:"entities"`
	PublishedObjectsTotal int                            `json:"published_objects_total"`
}

func fetchRefreshWindow(ctx context.Context, ingestor *Ingestor, entity string, refreshUntilISO string) (int, error) {
	orderUsed, err := ingestor.PickOrder(ctx, entity)
	if err != nil {
		return 0, err
	}
	totalWritten := 0
	rows := make([]map[string]any, 0, ingestor.cfg.InsertBatchSizeForEntity(entity))
	url := ingestor.cfg.Endpoint(entity)

	for page := 0; page < ingestor.cfg.MaxPages; page++ {
		offset := page * ingestor.cfg.PageLimit
		data, meta, err := ingestor.api.SafeGetJSON(ctx, url, map[string]string{
			"limit":     fmt.Sprint(ingestor.cfg.PageLimit),
			"offset":    fmt.Sprint(offset),
			"order":     orderUsed,
			"ascending": "false",
		}, ingestor.cfg.MaxRetries, ingestor.cfg.BaseSleep)
		if err != nil {
			fmt.Printf("[STOP] %s fetch failed at offset=%d err=%v\n", entity, offset, err)
			break
		}
		items := ExtractItems(data, entity)
		if len(items) == 0 {
			break
		}

		collectedAt := wrapTime(UTCNow())
		stop := false
		for _, obj := range items {
			updated := SafeString(firstNonNil(obj["updatedAt"], obj["updated_at"]))
			if updated != "" && ISOLEQ(updated, refreshUntilISO) {
				stop = true
				break
			}
			rawKey, err := UUIDv7()
			if err != nil {
				return totalWritten, err
			}
			row, err := BuildEntityRow(entity, NormalizeRawJSON(obj), collectedAt, rawKey)
			if err != nil {
				return totalWritten, err
			}
			if row != nil {
				rows = append(rows, row)
				totalWritten++
			}
		}
		if err := ingestor.FlushEntityRows(ctx, entity, &rows, false); err != nil {
			return totalWritten, err
		}
		fmt.Printf("[%s] page=%d offset=%d published=%d http_status=%d\n", entity, page+1, offset, totalWritten, meta.HTTPStatus)
		if stop {
			break
		}
		if len(items) < ingestor.cfg.PageLimit {
			break
		}
		if err := SleepContext(ctx, ingestor.cfg.BaseSleep); err != nil {
			return totalWritten, err
		}
	}

	if err := ingestor.FlushEntityRows(ctx, entity, &rows, true); err != nil {
		return totalWritten, err
	}
	return totalWritten, nil
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

	fmt.Printf("[CONFIG] ingest_mode=%s kafka_topic=%s lookback_hours=%d batch_size_default=%d batch_size_events=%d batch_size_markets=%d batch_size_series=%d kafka_batch_size=%d\n",
		cfg.IngestMode,
		cfg.KafkaTopic,
		cfg.LookbackHours,
		cfg.InsertBatchSize,
		cfg.InsertBatchSizeForEntity("events"),
		cfg.InsertBatchSizeForEntity("markets"),
		cfg.InsertBatchSizeForEntity("series"),
		cfg.KafkaBatchSize,
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

	refreshUntil := UTCNow().Add(-time.Duration(cfg.LookbackHours) * time.Hour)
	refreshUntilISO := FormatISO8601UTC(refreshUntil)
	wroteTotal := 0
	for _, entity := range []string{"events", "markets", "series"} {
		fmt.Printf("[REFRESH] %s: refresh_until=%s\n", entity, refreshUntilISO)
		wrote, err := fetchRefreshWindow(ctx, ingestor, entity, refreshUntilISO)
		if err != nil {
			return err
		}
		wroteTotal += wrote
		report.Entities[entity] = RefreshEntityReport{
			Mode:             "lookback_refresh_without_clickhouse_read",
			PublishedObjects: wrote,
			RefreshUntilUTC:  refreshUntilISO,
		}
	}
	report.PublishedObjectsTotal = wroteTotal

	fmt.Printf("\nDONE. published_objects_total=%d\n", wroteTotal)
	return nil
}
