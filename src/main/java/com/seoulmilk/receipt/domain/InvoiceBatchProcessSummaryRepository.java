package com.seoulmilk.receipt.domain;

import com.seoulmilk.receipt.domain.entity.InvoiceBatchProcessSummary;

import java.util.Optional;

public interface InvoiceBatchProcessSummaryRepository {
    InvoiceBatchProcessSummary save(InvoiceBatchProcessSummary summary);
    Optional<InvoiceBatchProcessSummary> findByBatchId(String batchId);
}
