package com.seoulmilk.invoice.application;

import com.seoulmilk.receipt.dto.request.OcrValidationRequest;

import java.util.List;

public interface OcrEventPublisher {
    String publish(List<OcrValidationRequest> event);
}
