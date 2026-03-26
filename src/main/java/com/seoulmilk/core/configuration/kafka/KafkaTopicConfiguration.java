package com.seoulmilk.core.configuration.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfiguration {

    private final KafkaProperties kafkaProperties;

    @Bean
    public NewTopic ocrResultTopic() {
        return TopicBuilder.name(kafkaProperties.getTopic())
                .partitions(kafkaProperties.getPartitionCount())
                .replicas(kafkaProperties.getReplicaCount())
                .build();
    }

    @Bean
    public NewTopic ocrRetryTopic() {
        return TopicBuilder.name(kafkaProperties.getRetryTopic())
                .partitions(kafkaProperties.getPartitionCount())
                .replicas(kafkaProperties.getReplicaCount())
                .build();
    }

    @Bean
    public NewTopic ocrDlqTopic() {
        return TopicBuilder.name(kafkaProperties.getDlqTopic())
                .partitions(kafkaProperties.getPartitionCount())
                .replicas(kafkaProperties.getReplicaCount())
                .build();
    }
}
