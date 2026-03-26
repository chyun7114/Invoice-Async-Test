package com.seoulmilk.invoice.presentation;

import com.seoulmilk.core.infrastructure.security.CustomUserDetails;
import com.seoulmilk.core.presentation.RestResponse;
import com.seoulmilk.invoice.application.usecase.InvoiceProcessUseCase;
import com.seoulmilk.invoice.presentation.swagger.InvoiceOcrSwagger;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.List;

@RestController
@Log4j2
@RequestMapping("/v2/invoice")
public class InvoiceOcrController implements InvoiceOcrSwagger {

    private final InvoiceProcessUseCase asyncInvoiceProcessService;
    private final InvoiceProcessUseCase syncInvoiceProcessService;

    public InvoiceOcrController(
            @Qualifier("asyncInvoiceProcessService") InvoiceProcessUseCase asyncInvoiceProcessService,
            @Qualifier("syncInvoiceProcessService") InvoiceProcessUseCase syncInvoiceProcessService) {
        this.asyncInvoiceProcessService = asyncInvoiceProcessService;
        this.syncInvoiceProcessService = syncInvoiceProcessService;
    }

    @PostMapping("/async")
    public ResponseEntity<RestResponse<Boolean>> uploadMultipleFiles(
            @AuthenticationPrincipal CustomUserDetails customUserDetails,
            @RequestPart(value = "files", required = false) List<MultipartFile> files) {

        if (files == null) {
            files = new ArrayList<>();
        }
        Boolean result = asyncInvoiceProcessService.processInvoices(customUserDetails, files);
        return ResponseEntity.ok(new RestResponse<>(result));
    }

    @PostMapping("/sync")
    public ResponseEntity<RestResponse<Boolean>> uploadMultipleFilesSync(
            @AuthenticationPrincipal CustomUserDetails customUserDetails,
            @RequestPart(value = "files", required = false) List<MultipartFile> files) {

        if (files == null) {
            files = new ArrayList<>();
        }
        Boolean result = syncInvoiceProcessService.processInvoices(customUserDetails, files);
        return ResponseEntity.ok(new RestResponse<>(result));
    }
}
