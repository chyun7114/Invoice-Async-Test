package com.seoulmilk.receipt.infrastructure.persistence.jpa.repository;

import com.seoulmilk.receipt.infrastructure.persistence.jpa.entity.InvoiceBatchProcessSummaryJpaEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface InvoiceBatchProcessSummaryJpaRepository extends JpaRepository<InvoiceBatchProcessSummaryJpaEntity, Long> {
    Optional<InvoiceBatchProcessSummaryJpaEntity> findByBatchId(String batchId);
}
