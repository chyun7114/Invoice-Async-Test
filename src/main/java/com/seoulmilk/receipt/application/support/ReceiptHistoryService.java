package com.seoulmilk.receipt.application.support;

import com.seoulmilk.receipt.domain.InvoiceFileProcessHistoryRepository;
import com.seoulmilk.receipt.domain.entity.InvoiceFileProcessHistory;
import com.seoulmilk.receipt.domain.value.FileProcessStatus;
import com.seoulmilk.receipt.domain.value.ProcessingFailureType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class ReceiptHistoryService {
    private static final String LEGACY_BATCH_ID = "legacy-batch";
    private static final Long UNKNOWN_EMP_PK = -1L;
    private static final String UNKNOWN_FILE_URL = "unknown";

    private final InvoiceFileProcessHistoryRepository fileProcessHistoryRepository;

    public Optional<InvoiceFileProcessHistory> findByRequestId(String requestId) {
        return fileProcessHistoryRepository.findByRequestId(requestId);
    }

    public boolean isTerminal(FileProcessStatus status) {
        return status == FileProcessStatus.SUCCESS
                || status == FileProcessStatus.FAILED
                || status == FileProcessStatus.DLQ
                || status == FileProcessStatus.REVIEW_REQUIRED
                || status == FileProcessStatus.SKIPPED_DUPLICATE;
    }

    public void upsert(
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
                : existing.map(InvoiceFileProcessHistory::getBatchId).orElse(LEGACY_BATCH_ID);

        InvoiceFileProcessHistory history = InvoiceFileProcessHistory.builder()
                .id(existingId)
                .batchId(resolvedBatchId)
                .requestId(requestId)
                .empPk(empPk == null ? UNKNOWN_EMP_PK : empPk)
                .fileUrl(fileUrl == null ? UNKNOWN_FILE_URL : fileUrl)
                .status(status)
                .failureType(failureType)
                .failReason(reason)
                .retryCount(retryCount)
                .processedAt(LocalDateTime.now())
                .build();
        fileProcessHistoryRepository.save(history);
    }
}
