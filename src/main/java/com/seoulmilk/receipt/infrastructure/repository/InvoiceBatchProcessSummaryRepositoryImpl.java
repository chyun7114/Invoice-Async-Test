package com.seoulmilk.receipt.infrastructure.repository;

import com.seoulmilk.receipt.domain.InvoiceBatchProcessSummaryRepository;
import com.seoulmilk.receipt.domain.entity.InvoiceBatchProcessSummary;
import com.seoulmilk.receipt.exception.ReceiptErrorCode;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.entity.InvoiceBatchProcessSummaryJpaEntity;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.repository.InvoiceBatchProcessSummaryJpaRepository;
import com.seoulmilk.receipt.infrastructure.persistence.mapper.InvoiceBatchProcessSummaryMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class InvoiceBatchProcessSummaryRepositoryImpl implements InvoiceBatchProcessSummaryRepository {
    private final InvoiceBatchProcessSummaryMapper mapper;
    private final InvoiceBatchProcessSummaryJpaRepository jpaRepository;

    @Override
    public InvoiceBatchProcessSummary save(InvoiceBatchProcessSummary summary) {
        InvoiceBatchProcessSummaryJpaEntity entity = mapper.toJpaEntity(summary);
        if (entity == null) {
            throw ReceiptErrorCode.FAILED_TO_SAVE_RECEIPT.toException();
        }

        InvoiceBatchProcessSummaryJpaEntity saved = jpaRepository.save(entity);
        return mapper.toDomainEntity(saved);
    }

    @Override
    public Optional<InvoiceBatchProcessSummary> findByBatchId(String batchId) {
        return jpaRepository.findByBatchId(batchId).map(mapper::toDomainEntity);
    }
}
