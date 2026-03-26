package com.seoulmilk.invoice.application;

import com.seoulmilk.core.infrastructure.security.CustomUserDetails;
import com.seoulmilk.invoice.application.usecase.InvoiceProcessUseCase;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Service("asyncInvoiceProcessService")
@RequiredArgsConstructor
@Log4j2
public class AsyncInvoiceProcessService implements InvoiceProcessUseCase {

    private final InvoiceRequestAssembler invoiceRequestAssembler;
    private final OcrEventPublisher ocrEventPublisher;

    @Override
    public Boolean processInvoices(CustomUserDetails customUserDetails, List<MultipartFile> files) {
        long startTime = System.currentTimeMillis();
        Long empPk = invoiceRequestAssembler.resolveEmpPk(customUserDetails);
        List<OcrValidationRequest> queueEvents = invoiceRequestAssembler.toQueueEvents(empPk, files);
        long assembledAt = System.currentTimeMillis();

        String requestId = ocrEventPublisher.publish(queueEvents);
        long endTime = System.currentTimeMillis();
        log.info("[Async] requestId={}, files={}, validate+assemble={}ms, publish-dispatch={}ms, total={}ms",
                requestId,
                queueEvents.size(),
                assembledAt - startTime,
                endTime - assembledAt,
                endTime - startTime);

        return true;
    }
}
