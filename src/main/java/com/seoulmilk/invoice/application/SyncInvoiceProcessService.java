package com.seoulmilk.invoice.application;

import com.seoulmilk.core.infrastructure.security.CustomUserDetails;
import com.seoulmilk.emp.domain.entity.Emp;
import com.seoulmilk.emp.domain.repository.EmpRepository;
import com.seoulmilk.invoice.application.usecase.InvoiceProcessUseCase;
import com.seoulmilk.receipt.domain.ValidReceiptRepository;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import com.seoulmilk.receipt.infrastructure.factory.ReceiptFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Service("syncInvoiceProcessService")
@RequiredArgsConstructor
@Log4j2
public class SyncInvoiceProcessService implements InvoiceProcessUseCase {

    private final InvoiceRequestAssembler invoiceRequestAssembler;
    private final MockOcrExtractionService mockOcrExtractionService;
    private final MockExternalValidationService mockExternalValidationService;
    private final EmpRepository empRepository;
    private final ValidReceiptRepository validReceiptRepository;

    @Override
    public Boolean processInvoices(CustomUserDetails customUserDetails, List<MultipartFile> files) {
        long startTime = System.currentTimeMillis();
        Long empPk = invoiceRequestAssembler.resolveEmpPk(customUserDetails);

        List<OcrValidationRequest> queueEvents = invoiceRequestAssembler.toQueueEvents(empPk, files);
        long ocrStart = System.currentTimeMillis();
        List<OcrValidationRequest> extractedRequests = queueEvents.stream()
                .map(mockOcrExtractionService::extract)
                .toList();
        long ocrEnd = System.currentTimeMillis();

        List<OcrValidationRequest> validatedRequests = extractedRequests.stream()
                .map(mockExternalValidationService::validate)
                .toList();
        long validationEnd = System.currentTimeMillis();

        int savedCount = saveValidatedReceipts(validatedRequests);

        long endTime = System.currentTimeMillis();
        log.info("[Sync] files={}, ocr={}ms, validation={}ms, save={}ms, total={}ms, saved={}",
                validatedRequests.size(),
                ocrEnd - ocrStart,
                validationEnd - ocrEnd,
                endTime - validationEnd,
                endTime - startTime,
                savedCount);

        return true;
    }

    private int saveValidatedReceipts(List<OcrValidationRequest> validatedRequests) {
        int savedCount = 0;

        for (OcrValidationRequest request : validatedRequests) {
            Emp emp = empRepository.findById(request.empPk()).orElse(null);
            if (emp == null) {
                log.warn("[Sync] skip DB save: emp not found, empPk={}, fileUrl={}",
                        request.empPk(), request.fileUrl());
                continue;
            }

            validReceiptRepository.save(ReceiptFactory.validReceiptCreate(emp, request));
            savedCount++;
        }

        return savedCount;
    }
}
