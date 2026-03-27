# 성능 병목 해결 계획서

## 1) 목표
- 토픽 분리를 바로 하지 않고 단계적으로 병목을 줄인다.
- 모든 의사결정은 측정값 기반으로 진행한다.

진행 순서:
1. Stage 1: 병렬도 튜닝
2. Stage 2: 외부 호출 Bulkhead
3. Stage 3: 필요 시 토픽 분리

## 2) 부하 프로필 표준
- `user` (Closed): `VUS=2`, `THINK_TIME_SEC=1`, `FILES_PER_REQUEST=3`, `DURATION=1m`
- `compare` (Arrival): `RATE=3 -> 5 -> 8 req/s`, `FILES_PER_REQUEST=5`, `DURATION=1m`
- `peak` (Stress): `VUS=15`, `THINK_TIME_SEC=0.3`, `FILES_PER_REQUEST=5`, `DURATION=1m`

## 3) 측정 히스토리 (비교용 유지)
### Baseline v1 (lag 정체로 중단)
기준 시점:
- 2026-03-27 (KST), 약 `15:10 ~ 15:20`

측정값:
- API Avg Latency: `25.1 ms`
- API P95 Latency: `38.6 ms`
- API Error Rate: `0%`
- API TPS: `0.605 ops/s`
- Consumer Group Lag (Total): 약 `460` 부근 정체
- Topic Message Rate: 피크 약 `9.8 ops/s`
- Consumer TPS (Estimated): 피크 약 `1.25 ops/s`
- Partition Lag: 대략 `160 / 140 / 160`

중단 사유:
- Consumer lag가 약 2분 이상 정체되어 baseline 수집 중단.

### Baseline v2 (s1-01)
기준:
- Dashboard 필터: `mode=async`, `profile=user`, `consumerGroup=ocr-group`, `topic=ocr_result`
- 관측 창: 약 `15:47 ~ 16:01` (KST)

측정값:
- API Avg Latency: `31.1 ms`
- API P95 Latency: `34.2 ms`
- API Error Rate: `0%`
- API TPS: `0.267 ops/s`
- Consumer Group Lag (Total): 피크 약 `330` 후 최종 `0` 복귀
- Topic Message Rate: 피크 약 `5.2 ops/s`, 이후 `0` 근처
- Consumer TPS (Estimated): 피크 약 `3.3 ops/s`
- Partition Lag: 피크 약 `105 ~ 120`, 최종 모든 파티션 `0`

해석:
- v1은 lag 정체가 있었고, v2는 일시 누적 후 회복 완료.
- 현재 `user` 프로필은 시스템이 소화 가능한 구간으로 판단.

## 4) Stage 1 현재 판정
- 상태: `부분 성공`
- 근거:
  - API 지표 안정(낮은 지연, 에러 0%)
  - lag 영구 누적 없이 회복 확인
- 남은 과제:
  - `compare(rate=3/5/8)`에서 정체 재발 여부 확인
  - `peak`에서 누적량/회복시간 측정

## 5) 다음 실행 계획
1. `compare(rate=3)` 실행 후 lag 회복 여부 확인
2. `compare(rate=5)`에서 회복 시간(peak -> 0) 비교
3. `compare(rate=8)`에서 정체 발생 여부 확인
4. 필요 시 `concurrency 3 -> 6` 또는 `partition 3 -> 6` 조정 후 재측정

## 6) Stage 1 Run 매트릭스
| Run ID | Profile | Partition | Concurrency | Instance | 목적 |
|---|---|---:|---:|---:|---|
| S1-00 | user | 3 | 3 | 1 | 기준선 고정 |
| S1-01 | user | 3 | 6 | 1 | concurrency 단독 효과 |
| S1-02 | compare(rate=3) | 3 | 6 | 1 | Arrival 저강도 검증 |
| S1-03 | compare(rate=5) | 6 | 6 | 1 | Arrival 중강도 검증 |
| S1-04 | compare(rate=8) | 6 | 6 | 1 | Arrival 고강도 검증 |
| S1-05 | peak | 6 | 6 | 1 | 피크 내성 확인 |
| S1-06 | peak | 6 | 6 | 2 | scale-out 효과 |

참고:
- `concurrency > partition`이면 추가 이득이 제한될 수 있음
- 가능하면 한 번에 한 변수만 변경

## 7) Run 기록 템플릿
```text
[Run]
- Run ID:
- Profile: user | compare | peak
- Arrival 단계(해당 시): RATE=3 | 5 | 8
- Config: partition / concurrency / instance
- Duration:
- API: avg / p95 / error / TPS
- Kafka: max lag / lag area / topic rate / consumer TPS
- Recovery: lag peak -> 0 소요시간
- Verdict: improved | neutral | degraded
- Decision:
```
