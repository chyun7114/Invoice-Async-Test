package com.seoulmilk.receipt.application;

import com.seoulmilk.emp.domain.entity.Emp;
import com.seoulmilk.emp.domain.repository.EmpRepository;
import com.seoulmilk.invoice.application.MockExternalValidationService;
import com.seoulmilk.invoice.application.MockOcrExtractionService;
import com.seoulmilk.receipt.domain.ValidReceiptRepository;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import com.seoulmilk.receipt.infrastructure.factory.ReceiptFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Log4j2
public class TaxReceiptValidationService {
    private static final String BATCH_COUNTER_KEY_PREFIX = "batch:ocr:count:";
    private static final String BATCH_SAVED_KEY_PREFIX = "batch:ocr:saved:";

    private final RedisTemplate<String, Object> redisTemplate;
    private final MockOcrExtractionService mockOcrExtractionService;
    private final MockExternalValidationService mockExternalValidationService;
    private final EmpRepository empRepository;
    private final ValidReceiptRepository validReceiptRepository;

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group-id}", concurrency = "3")
    public void listen(
            List<OcrValidationRequest> ocrValidationRequestList,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String requestId,
            @Header(value = "x-produced-at", required = false) byte[] producedAtHeader,
            @Header(value = "x-batch-id", required = false) byte[] batchIdHeader,
            @Header(value = "x-batch-size", required = false) byte[] batchSizeHeader
    ) {
        if (ocrValidationRequestList == null || ocrValidationRequestList.isEmpty()) {
            return;
        }

        String fallbackRequestId = "legacy-" + UUID.nameUUIDFromBytes(
                ocrValidationRequestList.getFirst().fileUrl().getBytes(StandardCharsets.UTF_8)
        );
        String effectiveRequestId = Optional.ofNullable(requestId).orElse(fallbackRequestId);
        String idempotencyKey = "idempotency:ocr_event:" + effectiveRequestId;
        Boolean isFirstReceived =
                redisTemplate.opsForValue().setIfAbsent(idempotencyKey, "DONE", Duration.ofMinutes(10));

        if (Boolean.FALSE.equals(isFirstReceived)) {
            log.warn("[Consumer-Idempotency] duplicate event ignored: requestId={}, key={}",
                    effectiveRequestId, idempotencyKey);
            return;
        }

        long consumedAt = System.currentTimeMillis();
        long ocrStart = System.currentTimeMillis();
        List<OcrValidationRequest> extractedRequests = ocrValidationRequestList.stream()
                .map(mockOcrExtractionService::extract)
                .toList();
        long ocrEnd = System.currentTimeMillis();

        List<OcrValidationRequest> validatedRequests = extractedRequests.stream()
                .map(mockExternalValidationService::validate)
                .toList();
        long validationEnd = System.currentTimeMillis();

        int savedCount = saveValidatedReceipts(validatedRequests);
        long endTime = System.currentTimeMillis();

        Long producedAt = parseProducedAt(producedAtHeader);
        String batchId = parseHeaderAsString(batchIdHeader);
        Integer batchSize = parseHeaderAsInteger(batchSizeHeader);
        Long queueDelay = producedAt == null ? null : consumedAt - producedAt;
        Long endToEnd = producedAt == null ? null : endTime - producedAt;

        log.info("[Consumer] requestId={}, records={}, queueDelay={}ms, ocr={}ms, validation={}ms, save={}ms, endToEnd={}ms, saved={}",
                effectiveRequestId,
                validatedRequests.size(),
                queueDelay == null ? "N/A" : queueDelay,
                ocrEnd - ocrStart,
                validationEnd - ocrEnd,
                endTime - validationEnd,
                endToEnd == null ? "N/A" : endToEnd,
                savedCount);

        logBatchSummaryIfCompleted(batchId, batchSize, producedAt, savedCount);
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
            log.warn("[Consumer] invalid batch-size header");
            return null;
        }
    }

    private void logBatchSummaryIfCompleted(String batchId, Integer batchSize, Long producedAt, int savedCount) {
        if (batchId == null || batchSize == null || batchSize <= 0) {
            return;
        }

        String processedKey = BATCH_COUNTER_KEY_PREFIX + batchId;
        String savedKey = BATCH_SAVED_KEY_PREFIX + batchId;

        Long processedCount = redisTemplate.opsForValue().increment(processedKey);
        redisTemplate.expire(processedKey, Duration.ofMinutes(30));

        redisTemplate.opsForValue().increment(savedKey, savedCount);
        redisTemplate.expire(savedKey, Duration.ofMinutes(30));

        if (processedCount == null || processedCount < batchSize) {
            return;
        }

        Long totalSaved = readLongValue(savedKey);
        long completedAt = System.currentTimeMillis();
        Long batchElapsed = producedAt == null ? null : completedAt - producedAt;

        log.info("[Consumer-Batch] batchId={}, totalRecords={}, processed={}, saved={}, totalElapsed={}ms",
                batchId,
                batchSize,
                processedCount,
                totalSaved == null ? "N/A" : totalSaved,
                batchElapsed == null ? "N/A" : batchElapsed);

        redisTemplate.delete(processedKey);
        redisTemplate.delete(savedKey);
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

    private int saveValidatedReceipts(List<OcrValidationRequest> validatedRequests) {
        int savedCount = 0;

        for (OcrValidationRequest request : validatedRequests) {
            Emp emp = empRepository.findById(request.empPk()).orElse(null);
            if (emp == null) {
                log.warn("[Consumer] skip DB save: emp not found, empPk={}, fileUrl={}",
                        request.empPk(), request.fileUrl());
                continue;
            }

            validReceiptRepository.save(ReceiptFactory.validReceiptCreate(emp, request));
            savedCount++;
        }

        return savedCount;
    }
}
