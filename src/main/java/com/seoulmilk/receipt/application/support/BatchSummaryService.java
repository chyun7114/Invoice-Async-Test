package com.seoulmilk.receipt.application.support;

import com.seoulmilk.receipt.application.policy.BatchCompletionPolicy;
import com.seoulmilk.receipt.domain.InvoiceBatchProcessSummaryRepository;
import com.seoulmilk.receipt.domain.entity.InvoiceBatchProcessSummary;
import com.seoulmilk.receipt.domain.value.BatchProcessStatus;
import com.seoulmilk.receipt.domain.value.FileProcessStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Component
@RequiredArgsConstructor
@Log4j2
public class BatchSummaryService {
    private static final String BATCH_SUCCESS_KEY_PREFIX = "batch:ocr:success:";
    private static final String BATCH_FAILED_KEY_PREFIX = "batch:ocr:failed:";
    private static final String BATCH_SKIPPED_KEY_PREFIX = "batch:ocr:skipped:";
    private static final String BATCH_REVIEW_KEY_PREFIX = "batch:ocr:review:";
    private static final String BATCH_DB_LOCK_KEY_PREFIX = "batch:ocr:db-lock:";

    private final RedisTemplate<String, Object> redisTemplate;
    private final BatchCompletionPolicy batchCompletionPolicy;
    private final InvoiceBatchProcessSummaryRepository batchProcessSummaryRepository;

    public void update(String batchId, Integer batchSize, Long producedAt, FileProcessStatus status) {
        if (batchId == null || batchSize == null || batchSize <= 0) {
            return;
        }

        String lockKey = BATCH_DB_LOCK_KEY_PREFIX + batchId;
        if (!acquireBatchLock(lockKey)) {
            log.warn("[Consumer-Batch] lock busy. skip this turn. batchId={}", batchId);
            return;
        }
        try {
            updateWithLock(batchId, batchSize, producedAt, status);
        } finally {
            releaseBatchLock(lockKey);
        }
    }

    private void updateWithLock(String batchId, Integer batchSize, Long producedAt, FileProcessStatus status) {
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

    private long defaultZero(Long value) {
        return value == null ? 0L : value;
    }
}
