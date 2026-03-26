package com.seoulmilk.receipt.domain.entity;

import com.seoulmilk.receipt.domain.value.FileProcessStatus;
import com.seoulmilk.receipt.domain.value.ProcessingFailureType;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.entity.InvoiceFileProcessHistoryJpaEntity;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Builder
public class InvoiceFileProcessHistory {
    private Long id;
    private String batchId;
    private String requestId;
    private Long empPk;
    private String fileUrl;
    private FileProcessStatus status;
    private ProcessingFailureType failureType;
    private String failReason;
    private Integer retryCount;
    private LocalDateTime processedAt;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private Boolean deleted;

    public static InvoiceFileProcessHistory create(
            String batchId,
            String requestId,
            Long empPk,
            String fileUrl,
            FileProcessStatus status,
            ProcessingFailureType failureType,
            String failReason,
            Integer retryCount,
            LocalDateTime processedAt
    ) {
        return InvoiceFileProcessHistory.builder()
                .batchId(batchId)
                .requestId(requestId)
                .empPk(empPk)
                .fileUrl(fileUrl)
                .status(status)
                .failureType(failureType)
                .failReason(failReason)
                .retryCount(retryCount)
                .processedAt(processedAt)
                .build();
    }

    public static InvoiceFileProcessHistory toDomainEntity(InvoiceFileProcessHistoryJpaEntity entity) {
        return InvoiceFileProcessHistory.builder()
                .id(entity.getId())
                .batchId(entity.getBatchId())
                .requestId(entity.getRequestId())
                .empPk(entity.getEmpPk())
                .fileUrl(entity.getFileUrl())
                .status(entity.getStatus())
                .failureType(entity.getFailureType())
                .failReason(entity.getFailReason())
                .retryCount(entity.getRetryCount())
                .processedAt(entity.getProcessedAt())
                .createdAt(entity.getCreatedAt())
                .updatedAt(entity.getUpdatedAt())
                .deleted(entity.getDeleted())
                .build();
    }
}
