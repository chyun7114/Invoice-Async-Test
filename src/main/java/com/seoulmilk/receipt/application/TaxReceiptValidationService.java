package com.seoulmilk.receipt.application;

import com.seoulmilk.core.configuration.kafka.KafkaProperties;
import com.seoulmilk.emp.domain.entity.Emp;
import com.seoulmilk.emp.domain.repository.EmpRepository;
import com.seoulmilk.invoice.application.MockExternalValidationService;
import com.seoulmilk.invoice.application.MockOcrExtractionService;
import com.seoulmilk.invoice.infrastructure.properties.InvoiceMockProperties;
import com.seoulmilk.receipt.application.policy.ReceiptProcessingFailureClassifier;
import com.seoulmilk.receipt.application.support.BatchSummaryService;
import com.seoulmilk.receipt.application.support.ReceiptHistoryService;
import com.seoulmilk.receipt.application.support.ReceiptRetryDlqPublisher;
import com.seoulmilk.receipt.domain.ValidReceiptRepository;
import com.seoulmilk.receipt.domain.entity.InvoiceFileProcessHistory;
import com.seoulmilk.receipt.domain.value.FileProcessStatus;
import com.seoulmilk.receipt.domain.value.ProcessingFailureType;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import com.seoulmilk.receipt.exception.ReviewRequiredException;
import com.seoulmilk.receipt.infrastructure.factory.ReceiptFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.kafka.support.KafkaHeaders;

@Service
@RequiredArgsConstructor
@Log4j2
public class TaxReceiptValidationService {
    private static final String PRODUCED_AT_HEADER = "x-produced-at";
    private static final String BATCH_ID_HEADER = "x-batch-id";
    private static final String BATCH_SIZE_HEADER = "x-batch-size";
    private static final String RETRY_COUNT_HEADER = "x-retry-count";

    private static final int MAX_TRANSIENT_RETRY = 2;

    private final KafkaProperties kafkaProperties;
    private final InvoiceMockProperties invoiceMockProperties;
    private final MockOcrExtractionService mockOcrExtractionService;
    private final MockExternalValidationService mockExternalValidationService;
    private final EmpRepository empRepository;
    private final ValidReceiptRepository validReceiptRepository;
    private final ReceiptProcessingFailureClassifier failureClassifier;
    private final ReceiptRetryDlqPublisher retryDlqPublisher;
    private final ReceiptHistoryService historyService;
    private final BatchSummaryService batchSummaryService;

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group-id}", concurrency = "${kafka.listener-concurrency:3}")
    public void listen(
            List<OcrValidationRequest> ocrValidationRequestList,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String requestId,
            @Header(value = PRODUCED_AT_HEADER, required = false) byte[] producedAtHeader,
            @Header(value = BATCH_ID_HEADER, required = false) byte[] batchIdHeader,
            @Header(value = BATCH_SIZE_HEADER, required = false) byte[] batchSizeHeader
    ) {
        processIncoming(ocrValidationRequestList, requestId, producedAtHeader, batchIdHeader, batchSizeHeader, 0, kafkaProperties.getTopic());
    }

    @KafkaListener(topics = "${kafka.retry-topic}", groupId = "${kafka.group-id}", concurrency = "${kafka.listener-concurrency:3}")
    public void listenRetry(
            List<OcrValidationRequest> ocrValidationRequestList,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String requestId,
            @Header(value = PRODUCED_AT_HEADER, required = false) byte[] producedAtHeader,
            @Header(value = BATCH_ID_HEADER, required = false) byte[] batchIdHeader,
            @Header(value = BATCH_SIZE_HEADER, required = false) byte[] batchSizeHeader,
            @Header(value = RETRY_COUNT_HEADER, required = false) byte[] retryCountHeader
    ) {
        int retryCount = Optional.ofNullable(parseHeaderAsInteger(retryCountHeader)).orElse(0);
        processIncoming(ocrValidationRequestList, requestId, producedAtHeader, batchIdHeader, batchSizeHeader, retryCount, kafkaProperties.getRetryTopic());
    }

    private void processIncoming(
            List<OcrValidationRequest> ocrValidationRequestList,
            String requestId,
            byte[] producedAtHeader,
            byte[] batchIdHeader,
            byte[] batchSizeHeader,
            int retryCount,
            String sourceTopic
    ) {
        try {
            ProcessingContext context = buildContext(
                    ocrValidationRequestList,
                    requestId,
                    producedAtHeader,
                    batchIdHeader,
                    batchSizeHeader,
                    retryCount,
                    sourceTopic
            );
            if (context == null) {
                return;
            }

            Optional<InvoiceFileProcessHistory> existingHistory = historyService.findByRequestId(context.requestId());
            if (existingHistory.isPresent() && historyService.isTerminal(existingHistory.get().getStatus())) {
                log.warn("[Consumer-Idempotency] terminal status already exists. requestId={}, batchId={}, status={}",
                        context.requestId(), existingHistory.get().getBatchId(), existingHistory.get().getStatus());
                return;
            }

            long consumedAt = System.currentTimeMillis();
            Long injectMs = null;
            Long extractMs = null;
            Long validateMs = null;
            Long saveMs = null;
            Long routeMs = null;
            Long historyMs = null;
            Long batchMs = null;

            try {
                long t0 = System.currentTimeMillis();
                maybeInjectRandomFailure();
                injectMs = System.currentTimeMillis() - t0;

                // OCR 처리 지점: 영수증 이미지/데이터에서 OCR 추출을 수행
                t0 = System.currentTimeMillis();
                OcrValidationRequest extractedRequest = mockOcrExtractionService.extract(context.request());
                extractMs = System.currentTimeMillis() - t0;

                // 국세청(외부 검증) 처리 지점: OCR 결과를 기반으로 세금계산서 유효성 검증 수행
                t0 = System.currentTimeMillis();
                OcrValidationRequest validatedRequest = mockExternalValidationService.validate(extractedRequest);
                validateMs = System.currentTimeMillis() - t0;

                t0 = System.currentTimeMillis();
                saveValidatedReceipt(validatedRequest);
                saveMs = System.currentTimeMillis() - t0;

                t0 = System.currentTimeMillis();
                historyService.upsert(context.batchId(), context.requestId(), context.request().empPk(), context.request().fileUrl(),
                        FileProcessStatus.SUCCESS, null, null, retryCount);
                historyMs = System.currentTimeMillis() - t0;

                t0 = System.currentTimeMillis();
                batchSummaryService.update(context.batchId(), context.batchSize(), context.producedAt(), FileProcessStatus.SUCCESS);
                batchMs = System.currentTimeMillis() - t0;

                logHandled(context.sourceTopic(), context.requestId(), context.batchId(), FileProcessStatus.SUCCESS, null, retryCount, consumedAt, context.producedAt(), null);
                logStepBreakdown(context.sourceTopic(), context.requestId(), context.batchId(), FileProcessStatus.SUCCESS, retryCount,
                        consumedAt, injectMs, extractMs, validateMs, saveMs, routeMs, historyMs, batchMs);
            } catch (Exception ex) {
                long t0 = System.currentTimeMillis();
                ProcessingFailureType failureType = failureClassifier.classify(ex);
                routeMs = System.currentTimeMillis() - t0;

                if (failureType == ProcessingFailureType.TRANSIENT) {
                    if (retryCount < MAX_TRANSIENT_RETRY) {
                        t0 = System.currentTimeMillis();
                        retryDlqPublisher.publishRetry(context.request(), context.requestId(), context.batchId(), context.batchSize(), context.producedAt(), retryCount + 1);
                        routeMs += (System.currentTimeMillis() - t0);

                        t0 = System.currentTimeMillis();
                        historyService.upsert(context.batchId(), context.requestId(), context.request().empPk(), context.request().fileUrl(),
                                FileProcessStatus.RETRYING, failureType, normalizeReason(ex), retryCount + 1);
                        historyMs = System.currentTimeMillis() - t0;

                        logHandled(context.sourceTopic(), context.requestId(), context.batchId(), FileProcessStatus.RETRYING, failureType, retryCount + 1, consumedAt, context.producedAt(), ex);
                        logStepBreakdown(context.sourceTopic(), context.requestId(), context.batchId(), FileProcessStatus.RETRYING, retryCount + 1,
                                consumedAt, injectMs, extractMs, validateMs, saveMs, routeMs, historyMs, batchMs);
                        return;
                    }

                    t0 = System.currentTimeMillis();
                    retryDlqPublisher.publishDlq(context.request(), context.requestId(), context.batchId(), context.batchSize(), context.producedAt(), retryCount);
                    routeMs += (System.currentTimeMillis() - t0);

                    t0 = System.currentTimeMillis();
                    historyService.upsert(context.batchId(), context.requestId(), context.request().empPk(), context.request().fileUrl(),
                            FileProcessStatus.DLQ, failureType, normalizeReason(ex), retryCount);
                    historyMs = System.currentTimeMillis() - t0;

                    t0 = System.currentTimeMillis();
                    batchSummaryService.update(context.batchId(), context.batchSize(), context.producedAt(), FileProcessStatus.DLQ);
                    batchMs = System.currentTimeMillis() - t0;

                    logHandled(context.sourceTopic(), context.requestId(), context.batchId(), FileProcessStatus.DLQ, failureType, retryCount, consumedAt, context.producedAt(), ex);
                    logStepBreakdown(context.sourceTopic(), context.requestId(), context.batchId(), FileProcessStatus.DLQ, retryCount,
                            consumedAt, injectMs, extractMs, validateMs, saveMs, routeMs, historyMs, batchMs);
                    return;
                }

                FileProcessStatus status = failureType == ProcessingFailureType.REVIEW_REQUIRED
                        ? FileProcessStatus.REVIEW_REQUIRED
                        : FileProcessStatus.FAILED;

                t0 = System.currentTimeMillis();
                historyService.upsert(context.batchId(), context.requestId(), context.request().empPk(), context.request().fileUrl(),
                        status, failureType, normalizeReason(ex), retryCount);
                historyMs = System.currentTimeMillis() - t0;

                t0 = System.currentTimeMillis();
                batchSummaryService.update(context.batchId(), context.batchSize(), context.producedAt(), status);
                batchMs = System.currentTimeMillis() - t0;

                logHandled(context.sourceTopic(), context.requestId(), context.batchId(), status, failureType, retryCount, consumedAt, context.producedAt(), ex);
                logStepBreakdown(context.sourceTopic(), context.requestId(), context.batchId(), status, retryCount,
                        consumedAt, injectMs, extractMs, validateMs, saveMs, routeMs, historyMs, batchMs);
            }
        } catch (Exception ex) {
            log.error("[Consumer-Unhandled] sourceTopic={}, requestId={}, retryCount={}, reason={}",
                    sourceTopic, requestId, retryCount, normalizeReason(ex), ex);
        }
    }

    private ProcessingContext buildContext(
            List<OcrValidationRequest> ocrValidationRequestList,
            String requestId,
            byte[] producedAtHeader,
            byte[] batchIdHeader,
            byte[] batchSizeHeader,
            int retryCount,
            String sourceTopic
    ) {
        if (ocrValidationRequestList == null || ocrValidationRequestList.isEmpty()) {
            return null;
        }

        OcrValidationRequest request = ocrValidationRequestList.getFirst();
        Long normalizedEmpPk = request.empPk() != null ? request.empPk() : invoiceMockProperties.getDefaultEmpPk();
        OcrValidationRequest normalizedRequest = new OcrValidationRequest(normalizedEmpPk, request.fileUrl(), request.taxValidationInfo());
        String fallbackRequestId = "legacy-" + UUID.nameUUIDFromBytes(normalizedRequest.fileUrl().getBytes(StandardCharsets.UTF_8));
        String effectiveRequestId = Optional.ofNullable(requestId).orElse(fallbackRequestId);

        return new ProcessingContext(
                normalizedRequest,
                effectiveRequestId,
                parseHeaderAsString(batchIdHeader),
                parseHeaderAsInteger(batchSizeHeader),
                parseProducedAt(producedAtHeader),
                retryCount,
                sourceTopic
        );
    }

    private void logHandled(
            String sourceTopic,
            String requestId,
            String batchId,
            FileProcessStatus status,
            ProcessingFailureType failureType,
            int retryCount,
            long consumedAt,
            Long producedAt,
            Exception ex
    ) {
        long endTime = System.currentTimeMillis();
        Long queueDelay = producedAt == null ? null : consumedAt - producedAt;
        Long endToEnd = producedAt == null ? null : endTime - producedAt;

        if (ex == null) {
            log.info("[Consumer] sourceTopic={}, requestId={}, batchId={}, status={}, retryCount={}, queueDelay={}ms, processing={}ms, endToEnd={}ms",
                    sourceTopic, requestId, batchId, status, retryCount,
                    queueDelay == null ? "N/A" : queueDelay,
                    endTime - consumedAt,
                    endToEnd == null ? "N/A" : endToEnd);
            return;
        }

        log.warn("[Consumer] sourceTopic={}, requestId={}, batchId={}, status={}, failureType={}, retryCount={}, queueDelay={}ms, processing={}ms, endToEnd={}ms, reason={}",
                sourceTopic, requestId, batchId, status, failureType, retryCount,
                queueDelay == null ? "N/A" : queueDelay,
                endTime - consumedAt,
                endToEnd == null ? "N/A" : endToEnd,
                normalizeReason(ex));
    }

    private void logStepBreakdown(
            String sourceTopic,
            String requestId,
            String batchId,
            FileProcessStatus status,
            int retryCount,
            long consumedAt,
            Long injectMs,
            Long extractMs,
            Long validateMs,
            Long saveMs,
            Long routeMs,
            Long historyMs,
            Long batchMs
    ) {
        long processingMs = System.currentTimeMillis() - consumedAt;
        log.info("[Consumer-Step] sourceTopic={}, requestId={}, batchId={}, status={}, retryCount={}, inject={}ms, extract={}ms, validate={}ms, save={}ms, route={}ms, history={}ms, batch={}ms, processing={}ms",
                sourceTopic,
                requestId,
                batchId,
                status,
                retryCount,
                injectMs == null ? "N/A" : injectMs,
                extractMs == null ? "N/A" : extractMs,
                validateMs == null ? "N/A" : validateMs,
                saveMs == null ? "N/A" : saveMs,
                routeMs == null ? "N/A" : routeMs,
                historyMs == null ? "N/A" : historyMs,
                batchMs == null ? "N/A" : batchMs,
                processingMs);
    }

    private Long parseProducedAt(byte[] producedAtHeader) {
        if (producedAtHeader == null || producedAtHeader.length == 0) {
            return null;
        }
        try {
            String value = new String(producedAtHeader, StandardCharsets.UTF_8);
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            log.warn("[Consumer] invalid produced-at header");
            return null;
        }
    }

    private String parseHeaderAsString(byte[] header) {
        if (header == null || header.length == 0) {
            return null;
        }
        return new String(header, StandardCharsets.UTF_8);
    }

    private Integer parseHeaderAsInteger(byte[] header) {
        String value = parseHeaderAsString(header);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void maybeInjectRandomFailure() {
        double failRate = invoiceMockProperties.getRandomFailRate();
        if (failRate <= 0) {
            return;
        }

        double sample = ThreadLocalRandom.current().nextDouble();
        if (sample >= failRate) {
            return;
        }

        double transientRatio = invoiceMockProperties.normalizedTransientFailRatio();
        double reviewRatio = invoiceMockProperties.normalizedReviewRequiredFailRatio();
        double failureTypeSample = ThreadLocalRandom.current().nextDouble();

        if (failureTypeSample < transientRatio) {
            throw randomTransientException();
        }

        if (failureTypeSample < transientRatio + reviewRatio) {
            throw new ReviewRequiredException("simulated OCR low-confidence result requires manual review");
        }

        throw new IllegalArgumentException("simulated invalid tax invoice payload");
    }

    private RuntimeException randomTransientException() {
        int scenario = ThreadLocalRandom.current().nextInt(3);
        if (scenario == 0) {
            return new RuntimeException(new java.net.SocketTimeoutException("simulated OCR gateway timeout"));
        }
        if (scenario == 1) {
            return new RuntimeException(new ConnectException("simulated NTS endpoint connection reset"));
        }
        return new RuntimeException(new IOException("simulated temporary upstream IO failure"));
    }

    private String normalizeReason(Throwable ex) {
        if (ex == null) {
            return null;
        }
        String message = ex.getMessage();
        if (message != null && !message.isBlank()) {
            return message.length() > 500 ? message.substring(0, 500) : message;
        }
        return ex.getClass().getSimpleName();
    }

    private void saveValidatedReceipt(OcrValidationRequest request) {
        Emp emp = empRepository.findById(request.empPk()).orElse(null);
        if (emp == null && invoiceMockProperties.isAllowMockEmpFallback()) {
            emp = Emp.builder()
                    .id(request.empPk())
                    .employeeId("MOCK-EMP-" + request.empPk())
                    .name("mock-user")
                    .build();
        }
        if (emp == null) {
            throw new IllegalArgumentException("emp not found: " + request.empPk());
        }
        validReceiptRepository.save(ReceiptFactory.validReceiptCreate(emp, request));
    }

    private record ProcessingContext(
            OcrValidationRequest request,
            String requestId,
            String batchId,
            Integer batchSize,
            Long producedAt,
            int retryCount,
            String sourceTopic
    ) {
    }
}
