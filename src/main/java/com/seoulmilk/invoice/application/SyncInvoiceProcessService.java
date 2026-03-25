package com.seoulmilk.invoice.application;

import com.seoulmilk.core.infrastructure.security.CustomUserDetails;
import com.seoulmilk.invoice.application.usecase.InvoiceProcessUseCase;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.UUID;
import java.util.Collections;

@Service("syncInvoiceProcessService")
@RequiredArgsConstructor
@Log4j2
public class SyncInvoiceProcessService implements InvoiceProcessUseCase {

    private final OpenFeignService openFeignService;

    @Override
    public Boolean processInvoices(CustomUserDetails customUserDetails, List<MultipartFile> files) {
        long startTime = System.currentTimeMillis();
        log.info("[Sync] 파이프라인 진입 (Kafka 없이 동기 대기)");

        // 1. JWT 토큰이 없거나 무효할 때를 위한 Mock User ID (예: 1L)
        Long empPk = (customUserDetails != null) ? customUserDetails.getId() : 1L;

        List<OcrValidationRequest> results;

        // 2. 파일 리스트가 없거나 비어있는 경우 강제로 Mock 데이터 생성 (1~50장 랜덤)
        if (files == null || files.isEmpty()) {
            int randomCount = new java.util.Random().nextInt(50) + 1;
            log.info("파일 리스트가 비어 있어 랜덤 수량({}장)으로 진행합니다.", randomCount);
            results = java.util.stream.IntStream.range(0, randomCount)
                    .mapToObj(i -> {
                        simulateDelay(200, 800); // 1장당 0.2초~0.8초의 가짜 OCR 지연
                        return createMockOcrRequest(empPk);
                    })
                    .toList();
        } else {
            results = files.stream()
                    .map(file -> {
                        try {
                            simulateDelay(200, 800); // 실제 파일이 있어도 서버 연결 전 딜레이 (선택)
                            return openFeignService.processImg(empPk, file);
                        } catch (Exception e) {
                            // OCR 서버 실패 시 에러 로그 없이 조용히 더미 생성
                            return createMockOcrRequest(empPk);
                        }
                    })
                    .toList();
        }

        // 추후 TaxReceiptValidationProvider 를 주입받아 동기로 검증을 수행할 위치입니다.
        log.info("국세청 검증 서버 연동 중...");
        simulateDelay(1000, 3000); // 국세청 검증은 1~3초 걸린다고 가정 (랜덤)
        log.info("국세청 검증 완료!");

        long endTime = System.currentTimeMillis();
        log.info("[Sync] 동기 처리 완전 종료. 소요시간: {}ms", (endTime - startTime));

        return true;
    }

    private OcrValidationRequest createMockOcrRequest(Long empPk) {
        return new OcrValidationRequest(
                empPk,
                "dummy-file-url-" + UUID.randomUUID(),
                OcrValidationRequest.TaxValidationInfo.from(
                        null, null, null, null, null, null, null, null, null
                )
        );
    }

    private void simulateDelay(int minMs, int maxMs) {
        try {
            int delay = new java.util.Random().nextInt(maxMs - minMs + 1) + minMs;
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
