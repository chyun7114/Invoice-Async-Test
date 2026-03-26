package com.seoulmilk.receipt.infrastructure.persistence.mapper;

import com.seoulmilk.receipt.domain.entity.InvoiceBatchProcessSummary;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.entity.InvoiceBatchProcessSummaryJpaEntity;
import org.springframework.stereotype.Component;

@Component
public class InvoiceBatchProcessSummaryMapper {
    public InvoiceBatchProcessSummary toDomainEntity(InvoiceBatchProcessSummaryJpaEntity entity) {
        return InvoiceBatchProcessSummary.toDomainEntity(entity);
    }

    public InvoiceBatchProcessSummaryJpaEntity toJpaEntity(InvoiceBatchProcessSummary entity) {
        return InvoiceBatchProcessSummaryJpaEntity.toJpaEntity(entity);
    }
}
