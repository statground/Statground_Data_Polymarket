package polymarket

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestClickHouseInsertBodyUsesSnapshotTableAndJSONEachRow(t *testing.T) {
	cfg := &Config{ClickHouseDatabase: "Data_Prediction_Polymarket_Raw"}
	ingestor := &Ingestor{cfg: cfg}
	body, err := ingestor.clickHouseInsertBody("events", []map[string]any{{
		"event_id":         uint64(42),
		"raw_key":          "019a0000-0000-7000-8000-000000000042",
		"collected_at_utc": "2026-07-01T00:00:00.000000Z",
		"title":            "Test event",
		"series_ids":       []uint64{1, 2},
		"market_ids":       []uint64{3},
		"raw_json":         `{"id":42}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(body, "INSERT INTO `Data_Prediction_Polymarket_Raw`.`polymarket_event_snapshot`") {
		t.Fatalf("unexpected insert target: %s", body)
	}
	if !strings.Contains(body, "FORMAT JSONEachRow\n") {
		t.Fatalf("insert body must use JSONEachRow: %s", body)
	}
	parts := strings.Split(strings.TrimSpace(body), "\n")
	if len(parts) != 2 {
		t.Fatalf("insert body lines = %d, want 2: %q", len(parts), body)
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(parts[1]), &row); err != nil {
		t.Fatal(err)
	}
	if got := row["title"]; got != "Test event" {
		t.Fatalf("title = %#v, want Test event", got)
	}
	if _, ok := row["ingested_at"]; ok {
		t.Fatal("ingested_at should be left to ClickHouse default")
	}
}

func TestNewStateStoreUsesClickHouseBackend(t *testing.T) {
	store := NewStateStore(&Config{StateBackend: "clickhouse"})
	if _, ok := store.(*ClickHouseStateStore); !ok {
		t.Fatalf("state store = %T, want *ClickHouseStateStore", store)
	}
}

func TestFormatClickHouseDateTime64MillisUsesClickHouseLiteral(t *testing.T) {
	ts := time.Date(2026, 7, 1, 13, 43, 44, 304787000, time.UTC)
	got := FormatClickHouseDateTime64Millis(ts, clickHouseAsiaSeoulLocation)
	if got != "2026-07-01 22:43:44.304" {
		t.Fatalf("formatted timestamp = %q", got)
	}
	if strings.ContainsAny(got, "TZ") {
		t.Fatalf("ClickHouse DateTime64 literal should not use RFC3339 marker: %q", got)
	}
}
