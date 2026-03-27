# 성능 개선 리포트 (포트폴리오 초안)

## 1) 목적
- API 지표와 Kafka 비동기 지표를 같은 시간축에서 관측해 병목을 개선한다.
- 개선 순서는 `병렬도 튜닝 -> 외부 호출 Bulkhead -> 필요 시 토픽 분리`를 따른다.

## 2) 문제 정의
- API 응답은 빠른 편이나, 비동기 구간에서 lag/queue 지연이 누적된다.
- 따라서 1차 병목은 API가 아니라 비동기 소비 처리량/해소 속도다.

## 3) 관측 체계
- 부하: `k6`
- 모니터링: `Kafka Exporter + Prometheus + Grafana`
- 로그: `[Consumer]`, `[Consumer-Step]`

핵심 지표:
- API: avg, p95, error rate, TPS
- Kafka: consumer lag, partition lag, topic message rate, consumer TPS(estimated)
- 로그: queueDelay, processing, endToEnd, 단계별 처리시간

## 4) Baseline (이미지 분석 반영)
측정 시점:
- 2026-03-27 (KST), Grafana 화면 기준 약 `15:10 ~ 15:20`

측정값:
- API Avg Latency: `25.1 ms`
- API P95 Latency: `38.6 ms`
- API Error Rate: `0%`
- API TPS: `0.605 ops/s`
- Consumer Group Lag (Total): 약 `460`에서 정체
- Topic Message Rate: 피크 약 `9.8 ops/s`, 이후 `0` 근처
- Consumer TPS (Estimated): 피크 약 `1.25 ops/s`, 이후 `0` 근처
- Partition Lag: 대략 `160 / 140 / 160` 수준

Baseline 작업 중단 사유:
- Consumer lag가 약 2분 이상 정체되어 실험 중단.

해석:
- API는 안정적이지만, consumer 측 해소 속도가 충분하지 않다.
- 이후 실험은 lag 정체 해소 여부를 최우선 판단 기준으로 둔다.

## 5) 적용한 부하 모델
- `user` (Closed): `VUS=2`, `THINK_TIME_SEC=1`, `FILES_PER_REQUEST=3`, `DURATION=1m`
- `compare` (Arrival): `RATE=3 -> 5 -> 8 req/s`, `FILES_PER_REQUEST=5`, `DURATION=1m`
- `peak` (Stress): `VUS=15`, `THINK_TIME_SEC=0.3`, `FILES_PER_REQUEST=5`, `DURATION=1m`

## 6) Stage 1 실행 기준
- 변경 대상: partition / listener concurrency / instance
- 목표:
  - Consumer TPS 증가
  - Partition 평균 lag 감소
  - 동일 시간창 기준 lag 해소 속도 개선
- 중단 기준:
  - error rate 급증
  - processing p95 급악화
  - lag 정체 지속

## 7) 결과 기록 템플릿
```text
[Run]
- Run ID:
- Profile: user | compare | peak
- Arrival 단계(해당 시): RATE=3 | 5 | 8
- Config: partition / concurrency / instance
- Duration:
- API: avg / p95 / error / TPS
- Kafka: max lag / lag area / topic rate / consumer TPS
- Log: queueDelay p95 / processing p95
- Verdict: improved | neutral | degraded
- Decision:
```
