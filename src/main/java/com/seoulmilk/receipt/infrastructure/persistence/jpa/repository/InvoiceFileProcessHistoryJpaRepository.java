package com.seoulmilk.receipt.infrastructure.persistence.jpa.repository;

import com.seoulmilk.receipt.infrastructure.persistence.jpa.entity.InvoiceFileProcessHistoryJpaEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface InvoiceFileProcessHistoryJpaRepository extends JpaRepository<InvoiceFileProcessHistoryJpaEntity, Long> {
    Optional<InvoiceFileProcessHistoryJpaEntity> findByRequestId(String requestId);
    List<InvoiceFileProcessHistoryJpaEntity> findAllByBatchId(String batchId);
}
