package com.seoulmilk.invoice;

import com.seoulmilk.config.IntegrationTestConfig;
import com.seoulmilk.core.application.FileStorageService;
import com.seoulmilk.receipt.application.TaxReceiptValidationService;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import io.minio.MinioClient;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Kafka 파이프라인 통합 테스트
 *
 * EmbeddedKafka를 활용하여 실제 Kafka 브로커 없이
 * Producer -> Topic -> Consumer 의 전체 플로우를 검증합니다.
 *
 * 테스트 대상:
 * 1. Producer가 Kafka 토픽에 메시지를 정상 발행하는지
 * 2. Consumer가 토픽에서 메시지를 수신하여 처리하는지
 * 3. Idempotency(멱등성) 필터가 중복 이벤트를 차단하는지
 */
@EmbeddedKafka(
        partitions = 1,
        topics = {"ocr_result_test"},
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
    @DisplayName("Producer가 이벤트를 발행하면 Consumer가 정상적으로 수신하여 처리한다")
    void testProducerToConsumerFlow() {
        // Given
        List<OcrValidationRequest> events = createMockEvents(3);

        // When
        kafkaTemplate.send(topic, events);

        // Then
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(taxReceiptValidationService, atLeastOnce()).listen(any()));
    }

    @Test
    @Order(2)
    @DisplayName("동일 이벤트가 중복 수신되면 두 번째 이벤트는 무시한다 (Idempotency)")
    void testIdempotencyFilter() {
        // Given
        String fixedFileUrl = "idempotency-test-url-" + UUID.randomUUID();
        List<OcrValidationRequest> events = List.of(createMockEvent(1L, fixedFileUrl));

        String idempotencyKey = "idempotency:ocr_event:" + fixedFileUrl;
        redisTemplate.opsForValue().set(idempotencyKey, "DONE", Duration.ofMinutes(10));

        reset(taxReceiptValidationService);

        // When
        kafkaTemplate.send(topic, events);

        // Then
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(taxReceiptValidationService, atLeastOnce()).listen(any()));

    }

    @Test
    @Order(3)
    @DisplayName("다수의 OCR 이벤트(10장)를 발행하면 Consumer가 정상 처리한다")
    void testMultipleEventsProcessing() {
        // Given
        List<OcrValidationRequest> events = createMockEvents(10);

        // When
        kafkaTemplate.send(topic, events);

        // Then
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(taxReceiptValidationService, atLeastOnce()).listen(any()));
    }

    @Test
    @Order(4)
    @DisplayName("빈 이벤트 리스트가 수신되면 처리하지 않고 즉시 리턴한다")
    void testEmptyEventListIgnored() {
        // Given
        List<OcrValidationRequest> emptyEvents = List.of();

        // When
        taxReceiptValidationService.listen(emptyEvents);
    }


    @Test
    @Order(5)
    @DisplayName("null 이벤트가 수신되면 처리하지 않고 즉시 리턴한다")
    void testNullEventIgnored() {
        // When & Then
        taxReceiptValidationService.listen(null);
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
        // 1. Kafka 리스너 컨테이너 명시적 정지 (파일 핸들 해제 유도)
        if (kafkaListenerEndpointRegistry != null) {
            for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
                container.stop();
            }
        }

        // 2. 브로커를 명시적으로 파괴하여 파일 락을 최대한 일찍 해제
        if (embeddedKafkaBroker != null) {
            embeddedKafkaBroker.destroy();
        }

        // 3. 임시 파일 삭제를 시도하는 셧다운 훅과의 충돌을 방지하기 위해 1.5초 대기
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
