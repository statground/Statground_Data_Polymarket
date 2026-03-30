package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"
)

type RefreshEntityReport struct {
	Mode            string `json:"mode"`
	InsertedObjects int    `json:"inserted_objects"`

	MaxUpdatedAtUTC string `json:"max_updated_at_utc,omitempty"`
	RefreshUntilUTC string `json:"refresh_until_utc,omitempty"`
}

type RefreshReport struct {
	RunAtUTC             string                         `json:"run_at_utc"`
	LookbackHours        int                            `json:"lookback_hours"`
	Entities             map[string]RefreshEntityReport `json:"entities"`
	InsertedObjectsTotal int                            `json:"inserted_objects_total"`
}

func maxUpdatedAt(ctx context.Context, cfg *Config, ch *ClickHouseClient, entity string) (*time.Time, error) {
	sql := fmt.Sprintf("SELECT max(ifNull(updated_at_utc, collected_at_utc)) AS mx FROM %s", cfg.EntityQualifiedTable(entity))
	row, err := ch.QueryOneRow(ctx, sql)
	if err != nil {
		return nil, err
	}
	return ParseClickHouseTime(row["mx"]), nil
}

func fetchRefreshWindow(ctx context.Context, ingestor *Ingestor, entity string, ch *ClickHouseClient, refreshUntilISO string) (int, *ClickHouseClient, error) {
	orderUsed, err := ingestor.PickOrder(ctx, entity)
	if err != nil {
		return 0, ch, err
	}
	totalWritten := 0
	rows := make([]map[string]any, 0, ingestor.cfg.InsertBatchSizeForEntity(entity))
	current := ch
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
				return totalWritten, current, err
			}
			row, err := BuildEntityRow(ctx, current, entity, NormalizeRawJSON(obj), collectedAt, rawKey)
			if err != nil {
				return totalWritten, current, err
			}
			if row != nil {
				rows = append(rows, row)
				totalWritten++
			}
		}
		current, err = current.FlushEntityRows(ctx, entity, &rows, false)
		if err != nil {
			return totalWritten, current, err
		}
		fmt.Printf("[%s] page=%d offset=%d inserted=%d http_status=%d\n", entity, page+1, offset, totalWritten, meta.HTTPStatus)
		if stop {
			break
		}
		if len(items) < ingestor.cfg.PageLimit {
			break
		}
		if err := SleepContext(ctx, ingestor.cfg.BaseSleep); err != nil {
			return totalWritten, current, err
		}
	}

	current, err = current.FlushEntityRows(ctx, entity, &rows, true)
	if err != nil {
		return totalWritten, current, err
	}
	return totalWritten, current, nil
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

	fmt.Printf("[CONFIG] insert_batch_size_default=%d insert_batch_size_events=%d insert_batch_size_markets=%d insert_batch_size_series=%d insert_split_after_attempt=%d insert_min_split_batch_rows=%d\n",
		cfg.InsertBatchSize,
		cfg.InsertBatchSizeForEntity("events"),
		cfg.InsertBatchSizeForEntity("markets"),
		cfg.InsertBatchSizeForEntity("series"),
		cfg.InsertSplitAfterAttempt,
		cfg.InsertMinSplitBatchRows,
	)

	ctx := context.Background()
	report := RefreshReport{
		RunAtUTC:      FormatISO8601UTC(UTCNow()),
		LookbackHours: cfg.LookbackHours,
		Entities:      make(map[string]RefreshEntityReport),
	}

	current := ingestor.click
	wroteTotal := 0
	for _, entity := range []string{"events", "markets", "series"} {
		mx, err := maxUpdatedAt(ctx, cfg, current, entity)
		if err != nil {
			return err
		}
		if mx == nil {
			fmt.Printf("[INFO] %s: no max(updated_at_utc). Falling back to incremental ingestion.\n", entity)
			checkpoint, err := ingestor.state.LoadCheckpoint(ctx)
			if err != nil {
				return err
			}
			res, err := ingestor.FetchAndInsert(ctx, entity, checkpoint, current)
			if err != nil {
				return err
			}
			current = res.Client
			wroteTotal += res.TotalWritten
			if res.NewCheckpoint != "" {
				checkpoint[entity] = res.NewCheckpoint
				if err := ingestor.state.SaveCheckpoint(ctx, checkpoint); err != nil {
					return err
				}
			}
			report.Entities[entity] = RefreshEntityReport{
				Mode:            "fallback_incremental",
				InsertedObjects: res.TotalWritten,
			}
			continue
		}

		refreshUntil := mx.Add(-time.Duration(cfg.LookbackHours) * time.Hour)
		refreshUntilISO := FormatISO8601UTC(refreshUntil)
		fmt.Printf("[REFRESH] %s: max_updated_at_utc=%s refresh_until=%s\n", entity, FormatISO8601UTC(*mx), refreshUntilISO)
		wrote, next, err := fetchRefreshWindow(ctx, ingestor, entity, current, refreshUntilISO)
		if err != nil {
			return err
		}
		current = next
		wroteTotal += wrote
		report.Entities[entity] = RefreshEntityReport{
			Mode:            "lookback_refresh",
			InsertedObjects: wrote,
			MaxUpdatedAtUTC: FormatISO8601UTC(*mx),
			RefreshUntilUTC: refreshUntilISO,
		}
	}

	ingestor.OptimizeAfterBatch(ctx, current)
	report.InsertedObjectsTotal = wroteTotal

	if cfg.RefreshReportPath != "" {
		path := cfg.RefreshReportPath
		if !filepath.IsAbs(path) {
			path = filepath.Join(cfg.RepoRoot, path)
		}
		payload, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		if err := WriteFileAtomic(path, payload); err != nil {
			return err
		}
		fmt.Printf("[REPORT] wrote refresh report -> %s\n", path)
	}

	fmt.Printf("\nDONE. inserted_objects_total=%d\n", wroteTotal)
	return nil
}
