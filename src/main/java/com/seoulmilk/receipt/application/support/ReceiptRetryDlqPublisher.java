package com.seoulmilk.receipt.application.support;

import com.seoulmilk.core.configuration.kafka.KafkaProperties;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
@RequiredArgsConstructor
public class ReceiptRetryDlqPublisher {
    private static final String PRODUCED_AT_HEADER = "x-produced-at";
    private static final String BATCH_ID_HEADER = "x-batch-id";
    private static final String BATCH_SIZE_HEADER = "x-batch-size";
    private static final String RETRY_COUNT_HEADER = "x-retry-count";

    private final KafkaTemplate<String, List<OcrValidationRequest>> kafkaTemplate;
    private final KafkaProperties kafkaProperties;

    public void publishRetry(
            OcrValidationRequest request,
            String requestId,
            String batchId,
            Integer batchSize,
            Long producedAt,
            int retryCount
    ) {
        ProducerRecord<String, List<OcrValidationRequest>> record =
                new ProducerRecord<>(kafkaProperties.getRetryTopic(), requestId, List.of(request));
        applyCommonHeaders(record, batchId, batchSize, producedAt, retryCount);
        kafkaTemplate.send(record);
    }

    public void publishDlq(
            OcrValidationRequest request,
            String requestId,
            String batchId,
            Integer batchSize,
            Long producedAt,
            int retryCount
    ) {
        ProducerRecord<String, List<OcrValidationRequest>> record =
                new ProducerRecord<>(kafkaProperties.getDlqTopic(), requestId, List.of(request));
        applyCommonHeaders(record, batchId, batchSize, producedAt, retryCount);
        kafkaTemplate.send(record);
    }

    private void applyCommonHeaders(
            ProducerRecord<String, List<OcrValidationRequest>> record,
            String batchId,
            Integer batchSize,
            Long producedAt,
            int retryCount
    ) {
        long producedAtValue = producedAt == null ? System.currentTimeMillis() : producedAt;
        if (batchId != null) {
            record.headers().add(new RecordHeader(BATCH_ID_HEADER, batchId.getBytes(StandardCharsets.UTF_8)));
        }
        if (batchSize != null) {
            record.headers().add(new RecordHeader(BATCH_SIZE_HEADER, Integer.toString(batchSize).getBytes(StandardCharsets.UTF_8)));
        }
        record.headers().add(new RecordHeader(PRODUCED_AT_HEADER, Long.toString(producedAtValue).getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(RETRY_COUNT_HEADER, Integer.toString(retryCount).getBytes(StandardCharsets.UTF_8)));
    }
}
