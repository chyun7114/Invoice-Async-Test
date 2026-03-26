package com.seoulmilk.receipt.domain.entity;

import com.seoulmilk.receipt.domain.value.BatchProcessStatus;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.entity.InvoiceBatchProcessSummaryJpaEntity;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Builder
public class InvoiceBatchProcessSummary {
    private Long id;
    private String batchId;
    private Integer totalCount;
    private Integer successCount;
    private Integer failedCount;
    private Integer skippedDuplicateCount;
    private Integer reviewRequiredCount;
    private BatchProcessStatus status;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private Boolean deleted;

    public static InvoiceBatchProcessSummary create(
            String batchId,
            Integer totalCount,
            Integer successCount,
            Integer failedCount,
            Integer skippedDuplicateCount,
            Integer reviewRequiredCount,
            BatchProcessStatus status,
            LocalDateTime startedAt,
            LocalDateTime completedAt
    ) {
        return InvoiceBatchProcessSummary.builder()
                .batchId(batchId)
                .totalCount(totalCount)
                .successCount(successCount)
                .failedCount(failedCount)
                .skippedDuplicateCount(skippedDuplicateCount)
                .reviewRequiredCount(reviewRequiredCount)
                .status(status)
                .startedAt(startedAt)
                .completedAt(completedAt)
                .build();
    }

    public static InvoiceBatchProcessSummary toDomainEntity(InvoiceBatchProcessSummaryJpaEntity entity) {
        return InvoiceBatchProcessSummary.builder()
                .id(entity.getId())
                .batchId(entity.getBatchId())
                .totalCount(entity.getTotalCount())
                .successCount(entity.getSuccessCount())
                .failedCount(entity.getFailedCount())
                .skippedDuplicateCount(entity.getSkippedDuplicateCount())
                .reviewRequiredCount(entity.getReviewRequiredCount())
                .status(entity.getStatus())
                .startedAt(entity.getStartedAt())
                .completedAt(entity.getCompletedAt())
                .createdAt(entity.getCreatedAt())
                .updatedAt(entity.getUpdatedAt())
                .deleted(entity.getDeleted())
                .build();
    }
}
