package com.example.events_collector_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    @Value("${spring.kafka.consumer.topic.name}")
    private String consumerTopicName;

    @Value("${spring.kafka.consumer.topic.partitionCount}")
    private int consumerTopicPartitionCount;

    @Value("${spring.kafka.producer.topic.name}")
    private String producerTopicName;

    @Value("${spring.kafka.producer.topic.partitionCount}")
    private int producerTopicPartitionCount;


    @Bean
    public NewTopic events() {
        return TopicBuilder
                .name(consumerTopicName)
                .partitions(consumerTopicPartitionCount)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic devices() {
        return TopicBuilder
                .name(producerTopicName)
                .partitions(producerTopicPartitionCount)
                .replicas(1)
                .build();
    }
}
