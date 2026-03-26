package com.seoulmilk.receipt.domain;

import com.seoulmilk.receipt.domain.entity.InvoiceFileProcessHistory;

import java.util.List;
import java.util.Optional;

public interface InvoiceFileProcessHistoryRepository {
    InvoiceFileProcessHistory save(InvoiceFileProcessHistory history);
    Optional<InvoiceFileProcessHistory> findByRequestId(String requestId);
    List<InvoiceFileProcessHistory> findAllByBatchId(String batchId);
}
