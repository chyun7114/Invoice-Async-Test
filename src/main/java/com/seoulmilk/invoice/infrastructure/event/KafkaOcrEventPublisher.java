package com.seoulmilk.invoice.infrastructure.event;

import com.seoulmilk.core.configuration.kafka.KafkaProperties;
import com.seoulmilk.invoice.application.OcrEventPublisher;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Log4j2
public class KafkaOcrEventPublisher implements OcrEventPublisher {
    private static final String PRODUCED_AT_HEADER = "x-produced-at";
    private static final String BATCH_ID_HEADER = "x-batch-id";
    private static final String BATCH_SIZE_HEADER = "x-batch-size";

    private final KafkaTemplate<String, List<OcrValidationRequest>> kafkaTemplate;
    private final KafkaProperties kafkaProperties;

    @Override
    public String publish(List<OcrValidationRequest> event) {
        String batchId = UUID.randomUUID().toString();
        String producedAtMillis = Long.toString(System.currentTimeMillis());

        for (int index = 0; index < event.size(); index++) {
            OcrValidationRequest singleEvent = event.get(index);
            String requestId = batchId + ":" + index;
            ProducerRecord<String, List<OcrValidationRequest>> record =
                    new ProducerRecord<>(kafkaProperties.getTopic(), requestId, List.of(singleEvent));
            record.headers().add(new RecordHeader(PRODUCED_AT_HEADER, producedAtMillis.getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(BATCH_ID_HEADER, batchId.getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(BATCH_SIZE_HEADER, Integer.toString(event.size()).getBytes(StandardCharsets.UTF_8)));

            kafkaTemplate.send(record).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("[Kafka-Publish-Failed] requestId={}, batchId={}, topic={}",
                            requestId, batchId, kafkaProperties.getTopic(), ex);
                    return;
                }

                log.debug("[Kafka-Publish-Success] requestId={}, batchId={}, topic={}, partition={}, offset={}",
                        requestId,
                        batchId,
                        kafkaProperties.getTopic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            });
        }

        log.info("[Kafka-Publish-Dispatch] batchId={}, topic={}, records={}",
                batchId, kafkaProperties.getTopic(), event.size());
        return batchId;
    }
}
