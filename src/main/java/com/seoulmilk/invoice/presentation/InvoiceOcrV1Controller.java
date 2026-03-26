package com.seoulmilk.invoice.presentation;

import com.seoulmilk.core.infrastructure.security.CustomUserDetails;
import com.seoulmilk.core.presentation.RestResponse;
import com.seoulmilk.invoice.application.OcrEventPublisher;
import com.seoulmilk.invoice.application.OpenFeignService;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@RestController
@Log4j2
@RequiredArgsConstructor
@RequestMapping("/v1/invoice")
public class InvoiceOcrV1Controller {
    private final OpenFeignService openFeignService;
    private final OcrEventPublisher ocrEventPublisher;

    @PostMapping
    public ResponseEntity<RestResponse<Boolean>> uploadMultipleFiles(
            @AuthenticationPrincipal CustomUserDetails customUserDetails,
            @RequestPart("files") List<MultipartFile> files
    ) {
        Long empPk = customUserDetails != null ? customUserDetails.getId() : 1L;

        List<OcrValidationRequest> results = files.stream()
                .map(file -> {
                    try {
                        return openFeignService.processImg(empPk, file);
                    } catch (FeignException e) {
                        log.error("[V1] OCR processing failed: {}", e.contentUTF8());
                        return null;
                    } catch (Exception e) {
                        log.error("[V1] OCR processing failed", e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();

        CompletableFuture.runAsync(() -> ocrEventPublisher.publish(results));
        return ResponseEntity.ok(new RestResponse<>(true));
    }
}
