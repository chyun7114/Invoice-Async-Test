package com.seoulmilk.receipt.infrastructure.persistence.mapper;

import com.seoulmilk.receipt.domain.entity.InvoiceFileProcessHistory;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.entity.InvoiceFileProcessHistoryJpaEntity;
import org.springframework.stereotype.Component;

@Component
public class InvoiceFileProcessHistoryMapper {
    public InvoiceFileProcessHistory toDomainEntity(InvoiceFileProcessHistoryJpaEntity entity) {
        return InvoiceFileProcessHistory.toDomainEntity(entity);
    }

    public InvoiceFileProcessHistoryJpaEntity toJpaEntity(InvoiceFileProcessHistory entity) {
        return InvoiceFileProcessHistoryJpaEntity.toJpaEntity(entity);
    }
}
