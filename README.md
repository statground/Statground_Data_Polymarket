# Statground_Data_Polymarket (ClickHouse)

이 레포는 Polymarket 데이터를 **GitHub에 저장하지 않고**, 수집 결과를 **ClickHouse(statground_polymarket)** 로 바로 적재합니다.

## 동작 방식
- GitHub Actions가 **6시간마다(UTC)** 실행됩니다.
- 실행 스크립트: `scripts/polymarket_crawl_to_clickhouse.py`
- 체크포인트는 `.state/polymarket_checkpoint.json` 으로 관리되며, 이 파일은 GitHub에 커밋되어 다음 실행에서 이어서 수집합니다.

## 필요한 GitHub Secrets
레포 Settings → Secrets and variables → Actions → Repository secrets

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_INTERFACE`  (예: `http` 또는 `native`)

옵션:
- `POLYMARKET_PAT` : org-wide 권한이 필요한 경우만. 없으면 기본 `GITHUB_TOKEN`으로 동작합니다.

## ClickHouse 적재 테이블
- `statground_polymarket.polymarket_raw`
- `statground_polymarket.polymarket_event`
- `statground_polymarket.polymarket_market`
- `statground_polymarket.polymarket_series`

## 불필요 파일 제거
- 기존 GitHub 저장(fanout) 및 repo-stats 갱신 관련 스크립트는 제거되었습니다.
