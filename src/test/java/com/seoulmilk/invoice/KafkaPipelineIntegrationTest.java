package com.seoulmilk.invoice;

import com.seoulmilk.config.IntegrationTestConfig;
import com.seoulmilk.invoice.application.MockOcrExtractionService;
import com.seoulmilk.receipt.application.TaxReceiptValidationService;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

@EmbeddedKafka(
        partitions = 1,
        topics = {"ocr_result_test", "ocr_result_retry_test", "ocr_result_dlq_test"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:0",
                "port=0"
        }
)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaPipelineIntegrationTest extends IntegrationTestConfig {

    @Autowired
    private KafkaTemplate<String, List<OcrValidationRequest>> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @SpyBean
    private TaxReceiptValidationService taxReceiptValidationService;

    @SpyBean
    private MockOcrExtractionService mockOcrExtractionService;

    @Value("${kafka.topic}")
    private String topic;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @Test
    @Order(1)
    @DisplayName("Producer가 이벤트를 발행하면 main consumer가 수신한다")
    void testProducerToConsumerFlow() {
        List<OcrValidationRequest> events = createMockEvents(3);
        kafkaTemplate.send(topic, events);

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(taxReceiptValidationService, atLeastOnce())
                        .listen(any(), any(), any(), any(), any()));
    }

    @Test
    @Order(2)
    @DisplayName("Transient 실패가 발생하면 retry topic으로 라우팅되어 retry consumer가 수신한다")
    void testTransientFailureRoutedToRetryTopic() {
        List<OcrValidationRequest> events = List.of(createMockEvent(1L, "transient-fail-" + UUID.randomUUID()));

        doThrow(new RuntimeException(new SocketTimeoutException("forced timeout")))
                .doCallRealMethod()
                .when(mockOcrExtractionService)
                .extract(any());

        reset(taxReceiptValidationService);
        kafkaTemplate.send(topic, events);

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(taxReceiptValidationService, atLeastOnce())
                        .listen(any(), any(), any(), any(), any()));

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(taxReceiptValidationService, atLeastOnce())
                        .listenRetry(any(), any(), any(), any(), any(), any()));
    }

    @Test
    @Order(3)
    @DisplayName("다수 이벤트를 발행하면 main consumer가 정상 처리한다")
    void testMultipleEventsProcessing() {
        List<OcrValidationRequest> events = createMockEvents(10);
        kafkaTemplate.send(topic, events);

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(taxReceiptValidationService, atLeastOnce())
                        .listen(any(), any(), any(), any(), any()));
    }

    @Test
    @Order(4)
    @DisplayName("빈 이벤트는 즉시 리턴한다")
    void testEmptyEventListIgnored() {
        taxReceiptValidationService.listen(List.of(), null, null, null, null);
    }

    @Test
    @Order(5)
    @DisplayName("null 이벤트는 즉시 리턴한다")
    void testNullEventIgnored() {
        taxReceiptValidationService.listen(null, null, null, null, null);
    }

    private List<OcrValidationRequest> createMockEvents(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(i -> createMockEvent(1L, "test-file-url-" + UUID.randomUUID()))
                .toList();
    }

    private OcrValidationRequest createMockEvent(Long empPk, String fileUrl) {
        return new OcrValidationRequest(
                empPk,
                fileUrl,
                OcrValidationRequest.TaxValidationInfo.from(
                        null, null, null, null, null, null, null, null, null
                )
        );
    }

    @AfterAll
    void tearDown() {
        if (kafkaListenerEndpointRegistry != null) {
            for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
                container.stop();
            }
        }

        if (embeddedKafkaBroker != null) {
            embeddedKafkaBroker.destroy();
        }

        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
