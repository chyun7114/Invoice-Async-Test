# K6 Load Test Guide (sync vs async)

## Profiles
- `user` (Closed Model)
  - `VUS=2`
  - `THINK_TIME_SEC=1`
  - `FILES_PER_REQUEST=3`
  - `DURATION=1m`
  - 목적: 실제 사용자 행동 기반 부하

- `compare` (Arrival Rate Model)
  - `RATE=3 req/s` (기본)
  - `FILES_PER_REQUEST=5`
  - `DURATION=1m`
  - 목적: sync vs async 동일 입력 부하 비교

- `peak` (Stress Test)
  - `VUS=15`
  - `THINK_TIME_SEC=0.3`
  - `FILES_PER_REQUEST=5`
  - `DURATION=1m`
  - 목적: 처리 한계 및 lag 누적/회복 확인

## Prerequisites
1. App + monitoring stack 실행
2. `k6` 설치 (`experimental-prometheus-rw` 지원 버전)

```bash
docker compose up --build -d
```

- Grafana: `http://localhost:3000` (`admin/admin`)
- Prometheus: `http://localhost:9090`

## Run Commands
공통:
- PowerShell에서는 `K6_PROMETHEUS_RW_TREND_STATS`를 반드시 따옴표로 감싸기
- `MODE=sync|async`만 바꿔 같은 프로필로 비교

### 1) user (Closed)
```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=async -e PROFILE=user -e "K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99)" -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```

### 2) compare (Arrival) - 단계형
`3 -> 5 -> 8 req/s` 순서로 실행

```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=async -e PROFILE=compare -e RATE=3 -e PRE_ALLOCATED_VUS=10 -e MAX_VUS=50 -e "K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99)" -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```

```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=async -e PROFILE=compare -e RATE=5 -e PRE_ALLOCATED_VUS=15 -e MAX_VUS=80 -e "K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99)" -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```

```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=async -e PROFILE=compare -e RATE=8 -e PRE_ALLOCATED_VUS=20 -e MAX_VUS=120 -e "K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99)" -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```

### 3) peak (Stress)
```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=async -e PROFILE=peak -e "K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99)" -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```

## Compare Metrics
- API Avg / P95 latency
- API error rate
- API TPS
- Kafka consumer group lag
- Kafka partition lag
- topic message rate
- consumer TPS (estimated)

기본 대시보드:
- Grafana -> `K6 + Kafka Async Overview`

## 파일 개수 확인 팁
- 서버 로그에서 `[Async] ... files=...` 값을 확인
- `PROFILE=user` 실행 시 `files=3`이 찍혀야 정상
- `files=5`가 계속 찍히면 요청이 빈 파일로 들어간 상태일 수 있으니 최신 `k6/invoice-v2-sync-async.js`로 재실행

## Dedup 관련 참고
- Publisher는 Redis 멱등키(`empPk + fileUrl`)로 중복을 건너뜀
- 현재 스크립트는 요청마다 파일명/내용에 `VU`, `ITER`, `RUN_ID`를 넣어 중복 인식을 피함
- 필요하면 실행 시 `-e RUN_ID=baseline-1`처럼 run 식별자를 명시 가능
