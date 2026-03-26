package com.seoulmilk.receipt.infrastructure.repository;

import com.seoulmilk.receipt.domain.InvoiceFileProcessHistoryRepository;
import com.seoulmilk.receipt.domain.entity.InvoiceFileProcessHistory;
import com.seoulmilk.receipt.exception.ReceiptErrorCode;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.entity.InvoiceFileProcessHistoryJpaEntity;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.repository.InvoiceFileProcessHistoryJpaRepository;
import com.seoulmilk.receipt.infrastructure.persistence.mapper.InvoiceFileProcessHistoryMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class InvoiceFileProcessHistoryRepositoryImpl implements InvoiceFileProcessHistoryRepository {
    private final InvoiceFileProcessHistoryMapper mapper;
    private final InvoiceFileProcessHistoryJpaRepository jpaRepository;

    @Override
    public InvoiceFileProcessHistory save(InvoiceFileProcessHistory history) {
        InvoiceFileProcessHistoryJpaEntity entity = mapper.toJpaEntity(history);
        if (entity == null) {
            throw ReceiptErrorCode.FAILED_TO_SAVE_RECEIPT.toException();
        }

        InvoiceFileProcessHistoryJpaEntity saved = jpaRepository.save(entity);
        return mapper.toDomainEntity(saved);
    }

    @Override
    public Optional<InvoiceFileProcessHistory> findByRequestId(String requestId) {
        return jpaRepository.findByRequestId(requestId).map(mapper::toDomainEntity);
    }

    @Override
    public List<InvoiceFileProcessHistory> findAllByBatchId(String batchId) {
        return jpaRepository.findAllByBatchId(batchId).stream()
                .map(mapper::toDomainEntity)
                .toList();
    }
}
