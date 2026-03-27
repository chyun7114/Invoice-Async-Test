# Invoice Async Test

Kafka 기반 비동기 인보이스 처리 파이프라인 검증 프로젝트입니다.

## 개요
현재 구현은 다음 항목을 중심으로 구성되어 있습니다.
- 동기/비동기 처리 경로 분리
- 파일 1건당 메시지 1건 발행
- Redis 기반 Producer 멱등성(중복 발행 방지)
- Retry + DLQ 기반 실패 처리
- 파일/배치 단위 처리 상태 저장

## 기술 스택
- Java 21
- Spring Boot 3.3.x
- Spring Kafka
- Redis
- H2 (local/test)
- Gradle
- Embedded Kafka / Embedded Redis (테스트)

## API
기준 컨트롤러: `InvoiceOcrController`

- `POST /v2/invoice/async`
  - 파일 목록을 이벤트로 변환 후 Kafka 비동기 발행
  - 즉시 `Boolean` 응답 반환

- `POST /v2/invoice/sync`
  - 요청 스레드에서 동기 처리
  - 처리 완료 후 `Boolean` 응답 반환

## 처리 흐름
### Async 경로
1. `InvoiceRequestAssembler`
   - 입력 파일을 `OcrValidationRequest`로 변환
   - 파일 내용 해시(fingerprint) 기반 식별 URL 생성
2. `KafkaOcrEventPublisher`
   - 파일 1건당 메시지 1건 발행
   - Redis `SET NX`로 동일 영수증 중복 발행 차단
3. `TaxReceiptValidationService` (main consumer)
   - OCR/검증 mock 처리
   - 처리 상태 저장
   - transient 실패 시 retry topic 발행
   - 재시도 한도 초과 시 DLQ 발행

### Retry / DLQ
- Retry consumer topic: `${kafka.retry-topic}`
- DLQ producer topic: `${kafka.dlq-topic}`
- 실패 유형 분류: `ReceiptProcessingFailureClassifier`

## 주요 컴포넌트
### Invoice
- `InvoiceRequestAssembler`
- `AsyncInvoiceProcessService`
- `SyncInvoiceProcessService`
- `KafkaOcrEventPublisher`

### Receipt
- `TaxReceiptValidationService`
- `ReceiptRetryDlqPublisher`
- `ReceiptHistoryService`
- `BatchSummaryService`

## 설정
주요 설정 키:
- `kafka.topic`
- `kafka.retry-topic`
- `kafka.dlq-topic`
- `kafka.group-id`
- `invoice.mock.*`
  - `ocr-delay-min-ms`, `ocr-delay-max-ms`
  - `validation-delay-min-ms`, `validation-delay-max-ms`
  - `default-auto-file-count` (파일 미전달 시 생성 개수)
  - `random-fail-rate` (랜덤 실패 주입 비율)
  - `transient-fail-ratio`, `review-required-fail-ratio`, `business-fail-ratio`
  - `allow-mock-emp-fallback`

로컬 기본 설정 파일:
- `src/main/resources/application-local.yml`

## 실행
### 1) Infra + App (Docker Compose)
```bash
docker compose up --build
```

### 1-1) Monitoring endpoints
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Prometheus: `http://localhost:9090`
- Kafka Exporter metrics: `http://localhost:9308/metrics`

기본 Grafana 대시보드:
- `K6 + Kafka Async Overview`
- 상세 사용 가이드: `monitoring/DASHBOARD_USAGE.md`
- k6 프로필 상세: `k6/README.md`

`k6`를 Prometheus에 적재하려면:
```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=async -e PROFILE=user -e "K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99)" -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```

프로필 기준:
- `user` (Closed): `VUS=2`, `THINK_TIME_SEC=1`, `FILES_PER_REQUEST=3`, `DURATION=1m`
- `compare` (Arrival): `RATE=3->5->8 req/s`, `FILES_PER_REQUEST=5`, `DURATION=1m`
- `peak` (Stress): `VUS=15`, `THINK_TIME_SEC=0.3`, `FILES_PER_REQUEST=5`, `DURATION=1m`

`compare`(Arrival) 단계 실행:
```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=async -e PROFILE=compare -e RATE=3 -e PRE_ALLOCATED_VUS=10 -e MAX_VUS=50 -e "K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99)" -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```
```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=async -e PROFILE=compare -e RATE=5 -e PRE_ALLOCATED_VUS=15 -e MAX_VUS=80 -e "K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99)" -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```
```bash
k6 run -e BASE_URL=http://localhost:8080 -e MODE=async -e PROFILE=compare -e RATE=8 -e PRE_ALLOCATED_VUS=20 -e MAX_VUS=120 -e "K6_PROMETHEUS_RW_TREND_STATS=avg,p(95),p(99)" -o experimental-prometheus-rw=http://localhost:9090/api/v1/write k6/invoice-v2-sync-async.js
```

### 2) App only (local)
```bash
./gradlew bootRun --args='--spring.profiles.active=local'
```

## 테스트
대표 통합 테스트:
- `KafkaPipelineIntegrationTest`
  - Producer -> Consumer 파이프라인
  - Redis 멱등성(중복 발행 차단)
  - Retry 라우팅
- `TaxReceiptPipelineStatusIntegrationTest`
  - SUCCESS / FAILED / REVIEW_REQUIRED / DLQ 상태 저장
  - 배치 요약 집계 검증

실행:
```bash
./gradlew test
```

## 참고
- 이 문서는 현재 구현 기준 초안입니다.
- 운영 설정/배포 절차/모니터링 내용은 이후 추가 예정입니다.
