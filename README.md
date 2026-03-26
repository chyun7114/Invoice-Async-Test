# Invoice Async Test - 로직 정리

이 문서는 현재 브랜치 기준으로 `/v1/invoice` 성능 비교(동기/비동기, Kafka 유무)에 맞춰 반영된 **핵심 처리 로직**만 정리합니다.

## 1. 목표 아키텍처

- API: 최소 검증 후 즉시 응답
- 백그라운드: OCR -> 외부 검증 -> DB 저장
- 성능 측정: API 응답시간과 백그라운드 완료시간 분리

## 2. 엔드포인트

- `POST /v1/invoice`
  - 비동기 경로
  - 파일 이벤트 생성 후 Kafka 발행
  - 즉시 응답(`Boolean`)

- `POST /v1/invoice/sync`
  - 동기 경로
  - 요청 스레드에서 OCR -> 검증 -> DB 저장까지 완료 후 응답

## 3. 비동기 처리 흐름

### API 계층

1. `InvoiceRequestAssembler`에서 파일 최소 검증/이벤트 생성
2. `KafkaOcrEventPublisher`에서 Kafka 발행
3. 응답 반환

### Kafka 발행 방식

- 기존: `List<OcrValidationRequest>`를 1개 메시지로 발행
- 현재: **파일 1개당 메시지 1개**로 분할 발행
- key: `batchId:index`
- header:
  - `x-produced-at`: 생산 시각(ms)
  - `x-batch-id`: 배치 ID
  - `x-batch-size`: 배치 총 건수

### Consumer 계층 (`TaxReceiptValidationService`)

1. 멱등 체크 (Redis, requestId 기반)
2. OCR Mock (`MockOcrExtractionService`)
3. 외부 검증 Mock (`MockExternalValidationService`)
4. DB 저장 (`ValidReceiptRepository`)
5. 파일 단위 처리 로그 출력
6. Redis 카운터로 배치 완료 집계 후 배치 총 소요시간 로그 출력

## 4. 동기 처리 흐름

`SyncInvoiceProcessService`에서 다음을 요청 스레드 내 순차 수행:

1. 이벤트 생성
2. OCR Mock
3. 외부 검증 Mock
4. DB 저장
5. 단계별 소요시간 로그 출력

## 5. 파일 미첨부 모드 (Mock 테스트용)

- 파일을 보내지 않으면 자동으로 가짜 파일 이벤트를 생성
- 기본 범위: `1~50건 랜덤`
- 설정:
  - `invoice.mock.min-auto-file-count`
  - `invoice.mock.max-auto-file-count`

## 6. 로그 해석 포인트

### API 비동기 로그

- `[Async] ... total=...ms`
  - API가 이벤트 생성 + Kafka 디스패치까지 걸린 시간

### Consumer 파일 단위 로그

- `[Consumer] ... queueDelay=..., ocr=..., validation=..., save=..., endToEnd=..., saved=...`
  - 단일 메시지(파일 단위) 처리 시간

### Consumer 배치 단위 로그

- `[Consumer-Batch] ... totalRecords=..., processed=..., saved=..., totalElapsed=...ms`
  - 전체 파일(batch) 완료까지의 총 시간

## 7. 현재 주의사항

- `saved=0` 또는 `skip DB save: emp not found`가 나오면 `empPk`에 해당하는 사용자 데이터가 DB에 없는 상태입니다.
- MinIO 로컬 충돌 이슈가 있었고, 로컬 프로필 endpoint는 `http://[::1]:9000` 기준으로 사용 중입니다.

## 8. 주요 클래스

- Controller
  - `com.seoulmilk.invoice.presentation.InvoiceOcrController`
- Async
  - `com.seoulmilk.invoice.application.AsyncInvoiceProcessService`
  - `com.seoulmilk.invoice.infrastructure.event.KafkaOcrEventPublisher`
- Sync
  - `com.seoulmilk.invoice.application.SyncInvoiceProcessService`
- Consumer
  - `com.seoulmilk.receipt.application.TaxReceiptValidationService`
- Mock 단계 분리
  - `com.seoulmilk.invoice.application.MockOcrExtractionService`
  - `com.seoulmilk.invoice.application.MockExternalValidationService`

