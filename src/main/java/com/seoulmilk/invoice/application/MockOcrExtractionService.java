package com.seoulmilk.invoice.application;

import com.seoulmilk.invoice.infrastructure.properties.InvoiceMockProperties;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Service
@RequiredArgsConstructor
public class MockOcrExtractionService {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE;

    private final InvoiceMockProperties invoiceMockProperties;

    public OcrValidationRequest extract(OcrValidationRequest request) {
        pause(invoiceMockProperties.nextOcrDelayMs());

        long seed = Integer.toUnsignedLong(request.fileUrl().hashCode());
        int supplyValue = 10_000 + (int) (seed % 900_000);
        int taxTotal = Math.max(1, supplyValue / 10);
        int grandTotal = supplyValue + taxTotal;

        OcrValidationRequest.TaxValidationInfo extractedInfo = OcrValidationRequest.TaxValidationInfo.from(
                formatTenDigits(seed),
                formatTenDigits(seed + 9_973),
                formatApprovalNo(seed),
                LocalDate.now().format(DATE_FORMATTER),
                Integer.toString(supplyValue),
                "MOCK_SUPPLIER_" + (seed % 1_000),
                "MOCK_CONTRACTOR_" + (seed % 1_000),
                Integer.toString(taxTotal),
                Integer.toString(grandTotal)
        );

        return new OcrValidationRequest(request.empPk(), request.fileUrl(), extractedInfo);
    }

    private String formatTenDigits(long seed) {
        return String.format("%010d", seed % 10_000_000_000L);
    }

    private String formatApprovalNo(long seed) {
        return String.format("%024d", seed % 1_000_000_000_000_000_000L);
    }

    private void pause(int delayMs) {
        if (delayMs <= 0) {
            return;
        }

        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
