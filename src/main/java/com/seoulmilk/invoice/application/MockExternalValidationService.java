package com.seoulmilk.invoice.application;

import com.seoulmilk.invoice.infrastructure.properties.InvoiceMockProperties;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MockExternalValidationService {
    private final InvoiceMockProperties invoiceMockProperties;

    public OcrValidationRequest validate(OcrValidationRequest request) {
        pause(invoiceMockProperties.nextValidationDelayMs());

        OcrValidationRequest.TaxValidationInfo info = request.taxValidationInfo();
        OcrValidationRequest.TaxValidationInfo validatedInfo = OcrValidationRequest.TaxValidationInfo.from(
                info == null ? null : info.supplierRegNumber(),
                info == null ? null : info.contractorRegNumber(),
                info == null ? null : info.approvalNo(),
                info == null ? null : info.reportingDate(),
                info == null ? null : info.supplyValue(),
                info == null ? null : info.supplierName(),
                info == null ? null : info.contractorName(),
                info == null ? null : info.taxTotal(),
                info == null ? null : info.grandTotal()
        );

        return new OcrValidationRequest(request.empPk(), request.fileUrl(), validatedInfo);
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
