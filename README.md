# Statground Data Polymarket (Go)

이 버전은 기존 Python 기반 Polymarket 수집/리프레시/통계 리포트/안전 푸시 파이프라인을 Go로 옮긴 리포지토리 구조입니다.

## 명령 대응

- `python -m scripts.polymarket_crawl_to_clickhouse` → `go run ./cmd/polymarket-ingest`
- `python -m scripts.polymarket_refresh_and_gapfill` → `go run ./cmd/polymarket-refresh`
- `python -m scripts.polymarket_stats_report` → `go run ./cmd/polymarket-stats`
- `python -m scripts.polymarket_sync_and_push_stats` → `go run ./cmd/polymarket-sync-push`

## 유지한 동작

- Polymarket API incremental 수집
- GitHub Contents API 기반 checkpoint 저장
- ClickHouse normalized table insert
- retry / split insert / best-effort optimize
- lookback refresh + gap-fill
- stats README + chart 산출
- 최신 remote branch 기준 stats 재생성 후 safe push

## 변경한 점

- Python 의존성 제거
- GitHub Actions를 Go 기반으로 교체
- 통계 차트 산출물은 `PNG` 대신 `SVG`로 생성
- ClickHouse 연결은 순수 Go 표준 라이브러리 기반 `HTTP 인터페이스` 전제로 구현

## ClickHouse 연결 주의

이 변환본은 `CLICKHOUSE_INTERFACE=http` 기준입니다.
워크플로우에서도 강제로 `http`를 사용하도록 변경했습니다.
필요 시 `CLICKHOUSE_SCHEME=https` 와 8443 포트를 함께 사용하면 HTTPS도 적용할 수 있습니다.
