package com.seoulmilk.receipt;

import com.seoulmilk.config.IntegrationTestConfig;
import com.seoulmilk.invoice.application.MockExternalValidationService;
import com.seoulmilk.invoice.application.MockOcrExtractionService;
import com.seoulmilk.receipt.domain.InvoiceBatchProcessSummaryRepository;
import com.seoulmilk.receipt.domain.InvoiceFileProcessHistoryRepository;
import com.seoulmilk.receipt.domain.entity.InvoiceBatchProcessSummary;
import com.seoulmilk.receipt.domain.entity.InvoiceFileProcessHistory;
import com.seoulmilk.receipt.domain.value.BatchProcessStatus;
import com.seoulmilk.receipt.domain.value.FileProcessStatus;
import com.seoulmilk.receipt.domain.value.ProcessingFailureType;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import com.seoulmilk.receipt.exception.ReviewRequiredException;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.repository.InvoiceBatchProcessSummaryJpaRepository;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.repository.InvoiceFileProcessHistoryJpaRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;

import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

@EmbeddedKafka(
        partitions = 1,
        topics = {"ocr_result_test", "ocr_result_retry_test", "ocr_result_dlq_test"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:0",
                "port=0"
        }
)
class TaxReceiptPipelineStatusIntegrationTest extends IntegrationTestConfig {
    private static final String PRODUCED_AT_HEADER = "x-produced-at";
    private static final String BATCH_ID_HEADER = "x-batch-id";
    private static final String BATCH_SIZE_HEADER = "x-batch-size";

    @Autowired
    private KafkaTemplate<String, List<OcrValidationRequest>> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private InvoiceFileProcessHistoryRepository fileHistoryRepository;

    @Autowired
    private InvoiceBatchProcessSummaryRepository batchSummaryRepository;

    @Autowired
    private InvoiceFileProcessHistoryJpaRepository fileHistoryJpaRepository;

    @Autowired
    private InvoiceBatchProcessSummaryJpaRepository batchSummaryJpaRepository;

    @SpyBean
    private MockOcrExtractionService mockOcrExtractionService;

    @SpyBean
    private MockExternalValidationService mockExternalValidationService;

    @Value("${kafka.topic}")
    private String mainTopic;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        reset(mockOcrExtractionService);
        fileHistoryJpaRepository.deleteAll();
        batchSummaryJpaRepository.deleteAll();
    }

    @Test
    @DisplayName("정상 처리 시 파일 상태 SUCCESS, 배치 상태 COMPLETED로 저장된다")
    void shouldPersistSuccessStatusAndBatchSummary() {
        String batchId = UUID.randomUUID().toString();
        String requestId = batchId + ":0";
        publishMainEvent(batchId, requestId, 1, createEvent(null, "mock://file/success-1.png"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Optional<InvoiceFileProcessHistory> fileHistory = fileHistoryRepository.findByRequestId(requestId);
            Optional<InvoiceBatchProcessSummary> summary = batchSummaryRepository.findByBatchId(batchId);

            assertThat(fileHistory).isPresent();
            assertThat(fileHistory.get().getStatus()).isEqualTo(FileProcessStatus.SUCCESS);

            assertThat(summary).isPresent();
            assertThat(summary.get().getStatus()).isEqualTo(BatchProcessStatus.COMPLETED);
            assertThat(summary.get().getSuccessCount()).isEqualTo(1);
            assertThat(summary.get().getFailedCount()).isEqualTo(0);
        });
    }

    @Test
    @DisplayName("Transient 실패 후 재시도 성공 시 최종 상태는 SUCCESS이고 retryCount가 반영된다")
    void shouldRetryTransientFailureAndEventuallySucceed() {
        doThrow(new RuntimeException(new SocketTimeoutException("transient-once")))
                .doCallRealMethod()
                .when(mockOcrExtractionService)
                .extract(any());

        String batchId = UUID.randomUUID().toString();
        String requestId = batchId + ":0";
        publishMainEvent(batchId, requestId, 1, createEvent(1L, "mock://file/retry-success-1.png"));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Optional<InvoiceFileProcessHistory> fileHistory = fileHistoryRepository.findByRequestId(requestId);
            Optional<InvoiceBatchProcessSummary> summary = batchSummaryRepository.findByBatchId(batchId);

            assertThat(fileHistory).isPresent();
            assertThat(fileHistory.get().getStatus()).isEqualTo(FileProcessStatus.SUCCESS);
            assertThat(fileHistory.get().getRetryCount()).isEqualTo(1);

            assertThat(summary).isPresent();
            assertThat(summary.get().getStatus()).isEqualTo(BatchProcessStatus.COMPLETED);
            assertThat(summary.get().getSuccessCount()).isEqualTo(1);
        });
    }

    @Test
    @DisplayName("Transient 실패가 최대 재시도 초과하면 DLQ 상태와 배치 실패 집계가 저장된다")
    void shouldMarkDlqAfterRetryExhausted() {
        doThrow(new RuntimeException(new SocketTimeoutException("always-timeout")))
                .when(mockOcrExtractionService)
                .extract(any());

        String batchId = UUID.randomUUID().toString();
        String requestId = batchId + ":0";
        publishMainEvent(batchId, requestId, 1, createEvent(1L, "mock://file/dlq-1.png"));

        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            Optional<InvoiceFileProcessHistory> fileHistory = fileHistoryRepository.findByRequestId(requestId);
            Optional<InvoiceBatchProcessSummary> summary = batchSummaryRepository.findByBatchId(batchId);

            assertThat(fileHistory).isPresent();
            assertThat(fileHistory.get().getStatus()).isEqualTo(FileProcessStatus.DLQ);
            assertThat(fileHistory.get().getRetryCount()).isEqualTo(2);

            assertThat(summary).isPresent();
            assertThat(summary.get().getStatus()).isEqualTo(BatchProcessStatus.COMPLETED_WITH_FAILURES);
            assertThat(summary.get().getFailedCount()).isEqualTo(1);
        });
    }

    @Test
    @DisplayName("Business failure should be FAILED and counted in batch summary")
    void shouldMarkFailedOnBusinessFailure() {
        doThrow(new IllegalArgumentException("invalid tax data"))
                .when(mockExternalValidationService)
                .validate(any());

        String batchId = UUID.randomUUID().toString();
        String requestId = batchId + ":0";
        publishMainEvent(batchId, requestId, 1, createEvent(1L, "mock://file/business-failed-1.png"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Optional<InvoiceFileProcessHistory> fileHistory = fileHistoryRepository.findByRequestId(requestId);
            Optional<InvoiceBatchProcessSummary> summary = batchSummaryRepository.findByBatchId(batchId);

            assertThat(fileHistory).isPresent();
            assertThat(fileHistory.get().getStatus()).isEqualTo(FileProcessStatus.FAILED);
            assertThat(fileHistory.get().getFailureType()).isEqualTo(ProcessingFailureType.BUSINESS);

            assertThat(summary).isPresent();
            assertThat(summary.get().getStatus()).isEqualTo(BatchProcessStatus.COMPLETED_WITH_FAILURES);
            assertThat(summary.get().getSuccessCount()).isEqualTo(0);
            assertThat(summary.get().getFailedCount()).isEqualTo(1);
            assertThat(summary.get().getReviewRequiredCount()).isEqualTo(0);
        });
    }

    @Test
    @DisplayName("Review required failure should be REVIEW_REQUIRED and counted in batch summary")
    void shouldMarkReviewRequiredAndCountInBatchSummary() {
        doThrow(new ReviewRequiredException("ocr confidence low"))
                .when(mockExternalValidationService)
                .validate(any());

        String batchId = UUID.randomUUID().toString();
        String requestId = batchId + ":0";
        publishMainEvent(batchId, requestId, 1, createEvent(1L, "mock://file/review-required-1.png"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Optional<InvoiceFileProcessHistory> fileHistory = fileHistoryRepository.findByRequestId(requestId);
            Optional<InvoiceBatchProcessSummary> summary = batchSummaryRepository.findByBatchId(batchId);

            assertThat(fileHistory).isPresent();
            assertThat(fileHistory.get().getStatus()).isEqualTo(FileProcessStatus.REVIEW_REQUIRED);
            assertThat(fileHistory.get().getFailureType()).isEqualTo(ProcessingFailureType.REVIEW_REQUIRED);

            assertThat(summary).isPresent();
            assertThat(summary.get().getStatus()).isEqualTo(BatchProcessStatus.COMPLETED_WITH_FAILURES);
            assertThat(summary.get().getSuccessCount()).isEqualTo(0);
            assertThat(summary.get().getFailedCount()).isEqualTo(0);
            assertThat(summary.get().getReviewRequiredCount()).isEqualTo(1);
        });
    }

    private void publishMainEvent(String batchId, String requestId, int batchSize, OcrValidationRequest event) {
        ProducerRecord<String, List<OcrValidationRequest>> record =
                new ProducerRecord<>(mainTopic, requestId, List.of(event));
        record.headers().add(new RecordHeader(PRODUCED_AT_HEADER, Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(BATCH_ID_HEADER, batchId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(BATCH_SIZE_HEADER, Integer.toString(batchSize).getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record);
    }

    private OcrValidationRequest createEvent(Long empPk, String fileUrl) {
        return new OcrValidationRequest(
                empPk,
                fileUrl,
                OcrValidationRequest.TaxValidationInfo.from(
                        null, null, null, null, null, null, null, null, null
                )
        );
    }
}
