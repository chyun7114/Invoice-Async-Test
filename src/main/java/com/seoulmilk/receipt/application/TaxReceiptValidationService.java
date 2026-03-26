package com.seoulmilk.receipt.application;

import com.seoulmilk.core.configuration.kafka.KafkaProperties;
import com.seoulmilk.emp.domain.entity.Emp;
import com.seoulmilk.emp.domain.repository.EmpRepository;
import com.seoulmilk.invoice.application.MockExternalValidationService;
import com.seoulmilk.invoice.application.MockOcrExtractionService;
import com.seoulmilk.invoice.infrastructure.properties.InvoiceMockProperties;
import com.seoulmilk.receipt.application.policy.BatchCompletionPolicy;
import com.seoulmilk.receipt.application.policy.ReceiptProcessingFailureClassifier;
import com.seoulmilk.receipt.domain.InvoiceBatchProcessSummaryRepository;
import com.seoulmilk.receipt.domain.InvoiceFileProcessHistoryRepository;
import com.seoulmilk.receipt.domain.ValidReceiptRepository;
import com.seoulmilk.receipt.domain.entity.InvoiceBatchProcessSummary;
import com.seoulmilk.receipt.domain.entity.InvoiceFileProcessHistory;
import com.seoulmilk.receipt.domain.value.BatchProcessStatus;
import com.seoulmilk.receipt.domain.value.FileProcessStatus;
import com.seoulmilk.receipt.domain.value.ProcessingFailureType;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import com.seoulmilk.receipt.exception.ReviewRequiredException;
import com.seoulmilk.receipt.infrastructure.factory.ReceiptFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
@RequiredArgsConstructor
@Log4j2
public class TaxReceiptValidationService {
    private static final String PRODUCED_AT_HEADER = "x-produced-at";
    private static final String BATCH_ID_HEADER = "x-batch-id";
    private static final String BATCH_SIZE_HEADER = "x-batch-size";
    private static final String RETRY_COUNT_HEADER = "x-retry-count";

    private static final String BATCH_SUCCESS_KEY_PREFIX = "batch:ocr:success:";
    private static final String BATCH_FAILED_KEY_PREFIX = "batch:ocr:failed:";
    private static final String BATCH_SKIPPED_KEY_PREFIX = "batch:ocr:skipped:";
    private static final String BATCH_REVIEW_KEY_PREFIX = "batch:ocr:review:";
    private static final String BATCH_DB_LOCK_KEY_PREFIX = "batch:ocr:db-lock:";
    private static final int MAX_TRANSIENT_RETRY = 2;

    private final RedisTemplate<String, Object> redisTemplate;
    private final KafkaTemplate<String, List<OcrValidationRequest>> kafkaTemplate;
    private final KafkaProperties kafkaProperties;
    private final InvoiceMockProperties invoiceMockProperties;
    private final MockOcrExtractionService mockOcrExtractionService;
    private final MockExternalValidationService mockExternalValidationService;
    private final EmpRepository empRepository;
    private final ValidReceiptRepository validReceiptRepository;
    private final ReceiptProcessingFailureClassifier failureClassifier;
    private final BatchCompletionPolicy batchCompletionPolicy;
    private final InvoiceFileProcessHistoryRepository fileProcessHistoryRepository;
    private final InvoiceBatchProcessSummaryRepository batchProcessSummaryRepository;

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group-id}", concurrency = "3")
    public void listen(
            List<OcrValidationRequest> ocrValidationRequestList,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String requestId,
            @Header(value = PRODUCED_AT_HEADER, required = false) byte[] producedAtHeader,
            @Header(value = BATCH_ID_HEADER, required = false) byte[] batchIdHeader,
            @Header(value = BATCH_SIZE_HEADER, required = false) byte[] batchSizeHeader
    ) {
        try {
            handleMessage(ocrValidationRequestList, requestId, producedAtHeader, batchIdHeader, batchSizeHeader, 0, kafkaProperties.getTopic());
        } catch (Exception ex) {
            log.error("[Consumer-Unhandled] sourceTopic={}, requestId={}, reason={}",
                    kafkaProperties.getTopic(), requestId, normalizeReason(ex), ex);
        }
    }

    @KafkaListener(topics = "${kafka.retry-topic}", groupId = "${kafka.group-id}", concurrency = "3")
    public void listenRetry(
            List<OcrValidationRequest> ocrValidationRequestList,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String requestId,
            @Header(value = PRODUCED_AT_HEADER, required = false) byte[] producedAtHeader,
            @Header(value = BATCH_ID_HEADER, required = false) byte[] batchIdHeader,
            @Header(value = BATCH_SIZE_HEADER, required = false) byte[] batchSizeHeader,
            @Header(value = RETRY_COUNT_HEADER, required = false) byte[] retryCountHeader
    ) {
        int retryCount = parseHeaderAsInteger(retryCountHeader) == null ? 0 : parseHeaderAsInteger(retryCountHeader);
        try {
            handleMessage(ocrValidationRequestList, requestId, producedAtHeader, batchIdHeader, batchSizeHeader, retryCount, kafkaProperties.getRetryTopic());
        } catch (Exception ex) {
            log.error("[Consumer-Unhandled] sourceTopic={}, requestId={}, retryCount={}, reason={}",
                    kafkaProperties.getRetryTopic(), requestId, retryCount, normalizeReason(ex), ex);
        }
    }

    private void handleMessage(
            List<OcrValidationRequest> ocrValidationRequestList,
            String requestId,
            byte[] producedAtHeader,
            byte[] batchIdHeader,
            byte[] batchSizeHeader,
            int retryCount,
            String sourceTopic
    ) {
        if (ocrValidationRequestList == null || ocrValidationRequestList.isEmpty()) {
            return;
        }

        OcrValidationRequest request = ocrValidationRequestList.getFirst();
        Long normalizedEmpPk = request.empPk() != null ? request.empPk() : invoiceMockProperties.getDefaultEmpPk();
        OcrValidationRequest normalizedRequest = new OcrValidationRequest(normalizedEmpPk, request.fileUrl(), request.taxValidationInfo());
        String fallbackRequestId = "legacy-" + UUID.nameUUIDFromBytes(request.fileUrl().getBytes(StandardCharsets.UTF_8));
        String effectiveRequestId = Optional.ofNullable(requestId).orElse(fallbackRequestId);
        String batchId = parseHeaderAsString(batchIdHeader);
        Integer batchSize = parseHeaderAsInteger(batchSizeHeader);
        Long producedAt = parseProducedAt(producedAtHeader);

        Optional<InvoiceFileProcessHistory> existingHistory = fileProcessHistoryRepository.findByRequestId(effectiveRequestId);
        if (existingHistory.isPresent() && isTerminal(existingHistory.get().getStatus())) {
            log.warn("[Consumer-Idempotency] terminal status already exists. requestId={}, batchId={}, status={}",
                    effectiveRequestId, existingHistory.get().getBatchId(), existingHistory.get().getStatus());
            return;
        }

        long consumedAt = System.currentTimeMillis();

        try {
            maybeInjectRandomFailure();
            OcrValidationRequest extractedRequest = mockOcrExtractionService.extract(normalizedRequest);
            OcrValidationRequest validatedRequest = mockExternalValidationService.validate(extractedRequest);
            saveValidatedReceipt(validatedRequest);

            upsertFileHistory(batchId, effectiveRequestId, normalizedRequest.empPk(), normalizedRequest.fileUrl(),
                    FileProcessStatus.SUCCESS, null, null, retryCount);
            updateBatchSummary(batchId, batchSize, producedAt, FileProcessStatus.SUCCESS);
            logHandled(sourceTopic, effectiveRequestId, batchId, FileProcessStatus.SUCCESS, null, retryCount, consumedAt, producedAt, null);
        } catch (Exception ex) {
            ProcessingFailureType failureType = failureClassifier.classify(ex);
            if (failureType == ProcessingFailureType.TRANSIENT) {
                if (retryCount < MAX_TRANSIENT_RETRY) {
                    publishRetry(normalizedRequest, effectiveRequestId, batchId, batchSize, producedAt, retryCount + 1);
                    upsertFileHistory(batchId, effectiveRequestId, normalizedRequest.empPk(), normalizedRequest.fileUrl(),
                            FileProcessStatus.RETRYING, failureType, normalizeReason(ex), retryCount + 1);
                    logHandled(sourceTopic, effectiveRequestId, batchId, FileProcessStatus.RETRYING, failureType, retryCount + 1, consumedAt, producedAt, ex);
                    return;
                }

                publishDlq(normalizedRequest, effectiveRequestId, batchId, batchSize, producedAt, retryCount);
                upsertFileHistory(batchId, effectiveRequestId, normalizedRequest.empPk(), normalizedRequest.fileUrl(),
                        FileProcessStatus.DLQ, failureType, normalizeReason(ex), retryCount);
                updateBatchSummary(batchId, batchSize, producedAt, FileProcessStatus.DLQ);
                logHandled(sourceTopic, effectiveRequestId, batchId, FileProcessStatus.DLQ, failureType, retryCount, consumedAt, producedAt, ex);
                return;
            }

            FileProcessStatus status = failureType == ProcessingFailureType.REVIEW_REQUIRED
                    ? FileProcessStatus.REVIEW_REQUIRED
                    : FileProcessStatus.FAILED;
            upsertFileHistory(batchId, effectiveRequestId, normalizedRequest.empPk(), normalizedRequest.fileUrl(),
                    status, failureType, normalizeReason(ex), retryCount);
            updateBatchSummary(batchId, batchSize, producedAt, status);
            logHandled(sourceTopic, effectiveRequestId, batchId, status, failureType, retryCount, consumedAt, producedAt, ex);
        }
    }

    private void publishRetry(
            OcrValidationRequest request,
            String requestId,
            String batchId,
            Integer batchSize,
            Long producedAt,
            int retryCount
    ) {
        ProducerRecord<String, List<OcrValidationRequest>> record =
                new ProducerRecord<>(kafkaProperties.getRetryTopic(), requestId, List.of(request));
        applyCommonHeaders(record, batchId, batchSize, producedAt, retryCount);
        kafkaTemplate.send(record);
    }

    private void publishDlq(
            OcrValidationRequest request,
            String requestId,
            String batchId,
            Integer batchSize,
            Long producedAt,
            int retryCount
    ) {
        ProducerRecord<String, List<OcrValidationRequest>> record =
                new ProducerRecord<>(kafkaProperties.getDlqTopic(), requestId, List.of(request));
        applyCommonHeaders(record, batchId, batchSize, producedAt, retryCount);
        kafkaTemplate.send(record);
    }

    private void applyCommonHeaders(
            ProducerRecord<String, List<OcrValidationRequest>> record,
            String batchId,
            Integer batchSize,
            Long producedAt,
            int retryCount
    ) {
        long producedAtValue = producedAt == null ? System.currentTimeMillis() : producedAt;
        if (batchId != null) {
            record.headers().add(new RecordHeader(BATCH_ID_HEADER, batchId.getBytes(StandardCharsets.UTF_8)));
        }
        if (batchSize != null) {
            record.headers().add(new RecordHeader(BATCH_SIZE_HEADER, Integer.toString(batchSize).getBytes(StandardCharsets.UTF_8)));
        }
        record.headers().add(new RecordHeader(PRODUCED_AT_HEADER, Long.toString(producedAtValue).getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(RETRY_COUNT_HEADER, Integer.toString(retryCount).getBytes(StandardCharsets.UTF_8)));
    }

    private void logHandled(
            String sourceTopic,
            String requestId,
            String batchId,
            FileProcessStatus status,
            ProcessingFailureType failureType,
            int retryCount,
            long consumedAt,
            Long producedAt,
            Exception ex
    ) {
        long endTime = System.currentTimeMillis();
        Long queueDelay = producedAt == null ? null : consumedAt - producedAt;
        Long endToEnd = producedAt == null ? null : endTime - producedAt;

        if (ex == null) {
            log.info("[Consumer] sourceTopic={}, requestId={}, batchId={}, status={}, retryCount={}, queueDelay={}ms, processing={}ms, endToEnd={}ms",
                    sourceTopic, requestId, batchId, status, retryCount,
                    queueDelay == null ? "N/A" : queueDelay,
                    endTime - consumedAt,
                    endToEnd == null ? "N/A" : endToEnd);
            return;
        }

        log.warn("[Consumer] sourceTopic={}, requestId={}, batchId={}, status={}, failureType={}, retryCount={}, queueDelay={}ms, processing={}ms, endToEnd={}ms, reason={}",
                sourceTopic, requestId, batchId, status, failureType, retryCount,
                queueDelay == null ? "N/A" : queueDelay,
                endTime - consumedAt,
                endToEnd == null ? "N/A" : endToEnd,
                normalizeReason(ex));
    }

    private boolean isTerminal(FileProcessStatus status) {
        return status == FileProcessStatus.SUCCESS
                || status == FileProcessStatus.FAILED
                || status == FileProcessStatus.DLQ
                || status == FileProcessStatus.REVIEW_REQUIRED
                || status == FileProcessStatus.SKIPPED_DUPLICATE;
    }

    private Long parseProducedAt(byte[] producedAtHeader) {
        if (producedAtHeader == null || producedAtHeader.length == 0) {
            return null;
        }
        try {
            String value = new String(producedAtHeader, StandardCharsets.UTF_8);
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            log.warn("[Consumer] invalid produced-at header");
            return null;
        }
    }

    private String parseHeaderAsString(byte[] header) {
        if (header == null || header.length == 0) {
            return null;
        }
        return new String(header, StandardCharsets.UTF_8);
    }

    private Integer parseHeaderAsInteger(byte[] header) {
        String value = parseHeaderAsString(header);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void maybeInjectRandomFailure() {
        double failRate = invoiceMockProperties.getRandomFailRate();
        if (failRate <= 0) {
            return;
        }

        double sample = ThreadLocalRandom.current().nextDouble();
        if (sample < failRate / 2) {
            throw new RuntimeException(new java.net.SocketTimeoutException("simulated transient timeout"));
        }
        if (sample < failRate) {
            throw new ReviewRequiredException("simulated unstable OCR result requires manual review");
        }
    }

    private String normalizeReason(Throwable ex) {
        if (ex == null) {
            return null;
        }
        String message = ex.getMessage();
        if (message != null && !message.isBlank()) {
            return message.length() > 500 ? message.substring(0, 500) : message;
        }
        return ex.getClass().getSimpleName();
    }

    private void upsertFileHistory(
            String batchId,
            String requestId,
            Long empPk,
            String fileUrl,
            FileProcessStatus status,
            ProcessingFailureType failureType,
            String reason,
            int retryCount
    ) {
        if (requestId == null) {
            return;
        }

        Optional<InvoiceFileProcessHistory> existing = fileProcessHistoryRepository.findByRequestId(requestId);
        Long existingId = existing.map(InvoiceFileProcessHistory::getId).orElse(null);
        String resolvedBatchId = batchId != null
                ? batchId
                : existing.map(InvoiceFileProcessHistory::getBatchId).orElse("legacy-batch");

        InvoiceFileProcessHistory history = InvoiceFileProcessHistory.builder()
                .id(existingId)
                .batchId(resolvedBatchId)
                .requestId(requestId)
                .empPk(empPk == null ? -1L : empPk)
                .fileUrl(fileUrl == null ? "unknown" : fileUrl)
                .status(status)
                .failureType(failureType)
                .failReason(reason)
                .retryCount(retryCount)
                .processedAt(LocalDateTime.now())
                .build();
        fileProcessHistoryRepository.save(history);
    }

    private void updateBatchSummary(String batchId, Integer batchSize, Long producedAt, FileProcessStatus status) {
        if (batchId == null || batchSize == null || batchSize <= 0) {
            return;
        }

        String lockKey = BATCH_DB_LOCK_KEY_PREFIX + batchId;
        if (!acquireBatchLock(lockKey)) {
            log.warn("[Consumer-Batch] lock busy. skip this turn. batchId={}", batchId);
            return;
        }
        try {
            updateBatchSummaryWithLock(batchId, batchSize, producedAt, status);
        } finally {
            releaseBatchLock(lockKey);
        }
    }

    private void updateBatchSummaryWithLock(String batchId, Integer batchSize, Long producedAt, FileProcessStatus status) {
        if (batchId == null || batchSize == null || batchSize <= 0) {
            return;
        }

        String successKey = BATCH_SUCCESS_KEY_PREFIX + batchId;
        String failedKey = BATCH_FAILED_KEY_PREFIX + batchId;
        String skippedKey = BATCH_SKIPPED_KEY_PREFIX + batchId;
        String reviewKey = BATCH_REVIEW_KEY_PREFIX + batchId;

        incrementStatusCounter(successKey, failedKey, skippedKey, reviewKey, status);
        expireBatchCounters(successKey, failedKey, skippedKey, reviewKey);

        long successCount = defaultZero(readLongValue(successKey));
        long failedCount = defaultZero(readLongValue(failedKey));
        long skippedCount = defaultZero(readLongValue(skippedKey));
        long reviewCount = defaultZero(readLongValue(reviewKey));

        boolean completed = batchCompletionPolicy.isCompleted(batchSize, successCount, failedCount, skippedCount, reviewCount);
        BatchProcessStatus batchStatus = completed
                ? batchCompletionPolicy.resolveStatus(failedCount, skippedCount, reviewCount)
                : BatchProcessStatus.PROCESSING;

        LocalDateTime startedAt = producedAt == null
                ? LocalDateTime.now()
                : LocalDateTime.ofInstant(Instant.ofEpochMilli(producedAt), ZoneId.systemDefault());
        LocalDateTime completedAt = completed ? LocalDateTime.now() : null;

        InvoiceBatchProcessSummary summary = loadOrCreateBatchSummary(batchId, batchSize, startedAt);
        persistBatchSummary(summary, batchSize, successCount, failedCount, skippedCount, reviewCount, batchStatus, startedAt, completedAt);

        if (!completed) {
            return;
        }

        long endedAtMillis = System.currentTimeMillis();
        Long batchElapsed = producedAt == null ? null : endedAtMillis - producedAt;
        log.info("[Consumer-Batch] batchId={}, totalRecords={}, finalized={}, success={}, failed={}, skipped={}, reviewRequired={}, totalElapsed={}ms, status={}",
                batchId,
                batchSize,
                successCount + failedCount + skippedCount + reviewCount,
                successCount,
                failedCount,
                skippedCount,
                reviewCount,
                batchElapsed == null ? "N/A" : batchElapsed,
                batchStatus);

        clearBatchCounters(successKey, failedKey, skippedKey, reviewKey);
    }

    private void persistBatchSummary(
            InvoiceBatchProcessSummary summary,
            int batchSize,
            long successCount,
            long failedCount,
            long skippedCount,
            long reviewCount,
            BatchProcessStatus batchStatus,
            LocalDateTime startedAt,
            LocalDateTime completedAt
    ) {
        InvoiceBatchProcessSummary updated = InvoiceBatchProcessSummary.builder()
                .id(summary.getId())
                .batchId(summary.getBatchId())
                .totalCount(batchSize)
                .successCount((int) successCount)
                .failedCount((int) failedCount)
                .skippedDuplicateCount((int) skippedCount)
                .reviewRequiredCount((int) reviewCount)
                .status(batchStatus)
                .startedAt(summary.getStartedAt() == null ? startedAt : summary.getStartedAt())
                .completedAt(completedAt)
                .build();

        try {
            batchProcessSummaryRepository.save(updated);
        } catch (DataIntegrityViolationException e) {
            InvoiceBatchProcessSummary existing = batchProcessSummaryRepository.findByBatchId(updated.getBatchId())
                    .orElseThrow(() -> e);
            InvoiceBatchProcessSummary retryUpdated = InvoiceBatchProcessSummary.builder()
                    .id(existing.getId())
                    .batchId(existing.getBatchId())
                    .totalCount(batchSize)
                    .successCount((int) successCount)
                    .failedCount((int) failedCount)
                    .skippedDuplicateCount((int) skippedCount)
                    .reviewRequiredCount((int) reviewCount)
                    .status(batchStatus)
                    .startedAt(existing.getStartedAt() == null ? startedAt : existing.getStartedAt())
                    .completedAt(completedAt)
                    .build();
            batchProcessSummaryRepository.save(retryUpdated);
        }
    }

    private boolean acquireBatchLock(String lockKey) {
        for (int attempt = 0; attempt < 5; attempt++) {
            Boolean acquired = redisTemplate.opsForValue().setIfAbsent(lockKey, "LOCKED", Duration.ofSeconds(3));
            if (Boolean.TRUE.equals(acquired)) {
                return true;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private void releaseBatchLock(String lockKey) {
        try {
            redisTemplate.delete(lockKey);
        } catch (Exception e) {
            log.warn("[Consumer-Batch] failed to release lock. key={}", lockKey);
        }
    }

    private long defaultZero(Long value) {
        return value == null ? 0L : value;
    }

    private void incrementStatusCounter(
            String successKey,
            String failedKey,
            String skippedKey,
            String reviewKey,
            FileProcessStatus status
    ) {
        if (status == FileProcessStatus.SUCCESS) {
            redisTemplate.opsForValue().increment(successKey);
            return;
        }
        if (status == FileProcessStatus.SKIPPED_DUPLICATE) {
            redisTemplate.opsForValue().increment(skippedKey);
            return;
        }
        if (status == FileProcessStatus.REVIEW_REQUIRED) {
            redisTemplate.opsForValue().increment(reviewKey);
            return;
        }
        redisTemplate.opsForValue().increment(failedKey);
    }

    private void expireBatchCounters(String successKey, String failedKey, String skippedKey, String reviewKey) {
        Duration ttl = Duration.ofMinutes(30);
        redisTemplate.expire(successKey, ttl);
        redisTemplate.expire(failedKey, ttl);
        redisTemplate.expire(skippedKey, ttl);
        redisTemplate.expire(reviewKey, ttl);
    }

    private void clearBatchCounters(String successKey, String failedKey, String skippedKey, String reviewKey) {
        redisTemplate.delete(successKey);
        redisTemplate.delete(failedKey);
        redisTemplate.delete(skippedKey);
        redisTemplate.delete(reviewKey);
    }

    private InvoiceBatchProcessSummary loadOrCreateBatchSummary(String batchId, int batchSize, LocalDateTime startedAt) {
        return batchProcessSummaryRepository.findByBatchId(batchId)
                .orElseGet(() -> InvoiceBatchProcessSummary.create(
                        batchId,
                        batchSize,
                        0,
                        0,
                        0,
                        0,
                        BatchProcessStatus.PROCESSING,
                        startedAt,
                        null
                ));
    }

    private Long readLongValue(String key) {
        Object value = redisTemplate.opsForValue().get(key);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void saveValidatedReceipt(OcrValidationRequest request) {
        Emp emp = empRepository.findById(request.empPk()).orElse(null);
        if (emp == null && invoiceMockProperties.isAllowMockEmpFallback()) {
            emp = Emp.builder()
                    .id(request.empPk())
                    .employeeId("MOCK-EMP-" + request.empPk())
                    .name("mock-user")
                    .build();
        }
        if (emp == null) {
            throw new IllegalArgumentException("emp not found: " + request.empPk());
        }
        validReceiptRepository.save(ReceiptFactory.validReceiptCreate(emp, request));
    }
}
