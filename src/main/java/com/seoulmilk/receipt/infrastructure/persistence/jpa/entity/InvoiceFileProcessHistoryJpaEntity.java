package com.seoulmilk.receipt.infrastructure.persistence.jpa.entity;

import com.seoulmilk.core.infrastructure.jpa.entity.BaseLongIdEntity;
import com.seoulmilk.receipt.domain.entity.InvoiceFileProcessHistory;
import com.seoulmilk.receipt.domain.value.FileProcessStatus;
import com.seoulmilk.receipt.domain.value.ProcessingFailureType;
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
        name = "NTS_INVOICE_FILE_PROCESS",
        indexes = {
                @Index(name = "idx_invoice_file_batch_id", columnList = "batch_id"),
                @Index(name = "idx_invoice_file_request_id", columnList = "request_id", unique = true)
        }
)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class InvoiceFileProcessHistoryJpaEntity extends BaseLongIdEntity {

    @Column(name = "batch_id", nullable = false, length = 64)
    private String batchId;

    @Column(name = "request_id", nullable = false, length = 128)
    private String requestId;

    @Column(name = "emp_pk", nullable = false)
    private Long empPk;

    @Column(name = "file_url", nullable = false, length = 1024)
    private String fileUrl;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private FileProcessStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "failure_type", length = 30)
    private ProcessingFailureType failureType;

    @Column(name = "fail_reason", length = 500)
    private String failReason;

    @Builder.Default
    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;

    @Column(name = "processed_at")
    private LocalDateTime processedAt;

    public static InvoiceFileProcessHistoryJpaEntity toJpaEntity(InvoiceFileProcessHistory history) {
        return InvoiceFileProcessHistoryJpaEntity.builder()
                .id(history.getId())
                .batchId(history.getBatchId())
                .requestId(history.getRequestId())
                .empPk(history.getEmpPk())
                .fileUrl(history.getFileUrl())
                .status(history.getStatus())
                .failureType(history.getFailureType())
                .failReason(history.getFailReason())
                .retryCount(Optional.ofNullable(history.getRetryCount()).orElse(0))
                .processedAt(history.getProcessedAt())
                .createdAt(history.getCreatedAt())
                .updatedAt(history.getUpdatedAt())
                .deleted(Optional.ofNullable(history.getDeleted()).orElse(false))
                .build();
    }
}
