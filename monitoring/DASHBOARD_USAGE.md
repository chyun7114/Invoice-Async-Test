# Monitoring Dashboard Usage Guide

## 1) 대시보드 접근 방법
### 1. 실행
```bash
docker compose up --build -d
```

### 2. 접속 주소
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Kafka Exporter metrics: `http://localhost:9308/metrics`

### 3. Grafana 로그인
- ID: `admin`
- PW: `admin`

### 4. 대시보드 열기
- Dashboard 이름: `K6 + Kafka Async Overview`
- 경로: Grafana 좌측 메뉴 `Dashboards` -> 검색창에 `k6` 입력

## 2) 핵심 지표 해석
### API 레벨 (k6)
- `API Avg Latency`: 평균 응답시간
- `API P95 Latency`: p95 응답시간
- `API Error Rate`: 에러율
- `API TPS`: 초당 처리량

### 비동기 처리 레벨 (Kafka)
- `Consumer Group Lag (Total)`: 그룹 전체 적체량
- `Partition Lag`: 파티션별 적체량 편차
- `Topic Message Rate`: 토픽 유입 속도(생산량)
- `Consumer TPS (Estimated)`: 컨슈머 커밋 오프셋 기준 처리 속도(추정)

## 3) 같은 시간축으로 비교하는 방법
1. 우측 상단 시간 범위를 `Last 15 minutes` 또는 테스트 시간에 맞게 설정
2. `Refresh`를 `5s`로 설정
3. `Consumer Group`, `Topic` 필터를 실제 테스트 대상 값으로 선택
4. k6 부하를 시작하고, 테스트 구간에서 API TPS와 Lag 변화를 동시에 확인

핵심 판단:
- `API TPS` 상승 + `Lag` 상승: 컨슈머 처리 여력 부족
- `API TPS` 상승 + `Lag` 유지/하락: 비동기 파이프라인 수용 가능
- `API Error Rate` 상승 + `Lag` 급증: 장애 또는 병목 가능성 높음

## 4) k6 실행 예시 (Prometheus 연동)
### async
```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=async -e VUS=30 -e DURATION=3m -e K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99) -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```

### sync
```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=sync -e VUS=30 -e DURATION=3m -e K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99) -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```

## 5) 활용 시나리오
### 시나리오 A: async 수용 한계 찾기
1. `VUS=10`부터 시작
2. `10 -> 30 -> 50 -> 100` 순으로 증가
3. 각 단계 3~5분 실행
4. 단계별 `API TPS`, `P95`, `Lag`, `Consumer TPS`를 비교

### 시나리오 B: 병목 위치 확인
1. `Topic Message Rate`와 `Consumer TPS` 차이 확인
2. 차이가 계속 벌어지면 컨슈머 병목 가능성
3. `Partition Lag`에서 특정 파티션만 급증하면 파티션 불균형 가능성

### 시나리오 C: sync vs async 비교
1. 같은 `VUS`, `DURATION`으로 각각 실행
2. sync는 API latency 중심, async는 lag/consumer 처리량 중심으로 해석
3. 최종적으로 사용자 응답 지표와 백엔드 적체 지표를 같이 판단

## 6) 추천 기록 템플릿
테스트마다 아래를 남기면 회고/재현이 쉬워집니다.

```text
[Test ID]
- DateTime:
- Mode: sync | async
- VUS / Duration:
- Avg / P95 / Error / TPS:
- Max Consumer Group Lag:
- Avg Consumer TPS:
- Observed bottleneck:
- Action items:
```

## 7) 자주 발생하는 이슈
- Grafana에 데이터가 안 보임:
  - `docker compose ps`로 `prometheus`, `grafana`, `kafka-exporter` 상태 확인
  - Prometheus `Status -> Targets`에서 `kafka-exporter`가 `UP`인지 확인
- k6 지표가 안 들어옴:
  - k6 실행 명령에 `-o experimental-prometheus-rw=...` 포함 여부 확인
  - `http://localhost:9090/api/v1/write` 접근 가능한지 확인
- Lag이 0으로만 보임:
  - 실제 컨슈머 그룹이 토픽을 소비 중인지 확인
  - Grafana 변수(`Consumer Group`, `Topic`)를 정확히 선택
