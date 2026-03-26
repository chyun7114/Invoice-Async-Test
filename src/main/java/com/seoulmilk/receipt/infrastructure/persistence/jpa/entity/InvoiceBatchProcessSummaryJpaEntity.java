package com.seoulmilk.receipt.infrastructure.persistence.jpa.entity;

import com.seoulmilk.core.infrastructure.jpa.entity.BaseLongIdEntity;
import com.seoulmilk.receipt.domain.entity.InvoiceBatchProcessSummary;
import com.seoulmilk.receipt.domain.value.BatchProcessStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;
import java.util.Optional;

@Entity
@Getter
@SuperBuilder
@Table(
        name = "NTS_INVOICE_BATCH_SUMMARY",
        indexes = {
                @Index(name = "idx_invoice_batch_batch_id", columnList = "batch_id", unique = true)
        }
)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class InvoiceBatchProcessSummaryJpaEntity extends BaseLongIdEntity {

    @Column(name = "batch_id", nullable = false, length = 64)
    private String batchId;

    @Column(name = "total_count", nullable = false)
    private Integer totalCount;

    @Builder.Default
    @Column(name = "success_count", nullable = false)
    private Integer successCount = 0;

    @Builder.Default
    @Column(name = "failed_count", nullable = false)
    private Integer failedCount = 0;

    @Builder.Default
    @Column(name = "skipped_duplicate_count", nullable = false)
    private Integer skippedDuplicateCount = 0;

    @Builder.Default
    @Column(name = "review_required_count", nullable = false)
    private Integer reviewRequiredCount = 0;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 40)
    private BatchProcessStatus status;

    @Column(name = "started_at", nullable = false)
    private LocalDateTime startedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    public static InvoiceBatchProcessSummaryJpaEntity toJpaEntity(InvoiceBatchProcessSummary summary) {
        return InvoiceBatchProcessSummaryJpaEntity.builder()
                .id(summary.getId())
                .batchId(summary.getBatchId())
                .totalCount(summary.getTotalCount())
                .successCount(Optional.ofNullable(summary.getSuccessCount()).orElse(0))
                .failedCount(Optional.ofNullable(summary.getFailedCount()).orElse(0))
                .skippedDuplicateCount(Optional.ofNullable(summary.getSkippedDuplicateCount()).orElse(0))
                .reviewRequiredCount(Optional.ofNullable(summary.getReviewRequiredCount()).orElse(0))
                .status(summary.getStatus())
                .startedAt(summary.getStartedAt())
                .completedAt(summary.getCompletedAt())
                .createdAt(summary.getCreatedAt())
                .updatedAt(summary.getUpdatedAt())
                .deleted(Optional.ofNullable(summary.getDeleted()).orElse(false))
                .build();
    }
}
