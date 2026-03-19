package com.example.events_collector_service.it;

import com.example.events_collector_service.config.TestcontainersConfig;
import com.example.events_collector_service.scheduler.OutboxScheduler;
import com.example.events_collector_service.utils.DeviceDedupServiceTestUtils;
import com.nashkod.avro.DeviceEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
public class SystemTest extends TestcontainersConfig {

    @Autowired
    private OutboxScheduler outboxScheduler;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String SELECT_COUNT_EVENTS_FROM_DEVICE_EVENTS = """
            SELECT count() FROM events.device_events
            """;
    private static final String SELECT_COUNT_EVENTS_FROM_DEVICE_OUTBOX = """
            SELECT count() FROM events.device_outbox
            """;
    private static final String SELECT_COUNT_EVENTS_FROM_DEVICE_OUTBOX_WHERE_STATUS_IS_1 = """
            SELECT count() FROM events.device_outbox WHERE status=1
            """;
    private static final String SELECT_COUNT_EVENTS_FROM_DEVICE_OUTBOX_WHERE_STATUS_IS_0 = """
            SELECT count() FROM events.device_outbox WHERE status=0
            """;

    @Value("${spring.kafka.consumer.topic.name}")
    private String kafkaTopicEvents;

    @Value("${spring.kafka.producer.topic.name}")
    private String kafkaTopicDevices;

    @Autowired
    private Environment environment;

    @Test
    void shouldProcessEventEndToEndAndSkipDuplicateOutboxCreation() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.bootstrap-servers"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put("schema.registry.url", environment.getProperty("spring.kafka.consumer.properties.schema.registry.url"));

        try (KafkaProducer<String, DeviceEvent> producer = new KafkaProducer<>(properties)) {
            DeviceEvent deviceEvent = DeviceDedupServiceTestUtils.getDeviceEvent();
            producer.send(new ProducerRecord<>(kafkaTopicEvents, deviceEvent.getDeviceId().toString(), deviceEvent));
            await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        Long countEventsFromDeviceEvents = jdbcTemplate.queryForObject(
                                SELECT_COUNT_EVENTS_FROM_DEVICE_EVENTS, Long.class);
                        assertThat(countEventsFromDeviceEvents).isEqualTo(1L);

                        Long countEventsFromDeviceOutbox = jdbcTemplate.queryForObject(
                                SELECT_COUNT_EVENTS_FROM_DEVICE_OUTBOX, Long.class);
                        assertThat(countEventsFromDeviceOutbox).isEqualTo(1L);
                    });

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.bootstrap-servers"));
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-devices-" + UUID.randomUUID());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of(kafkaTopicDevices));
                outboxScheduler.searchDeviceIdInTableDeviceOutbox();

                await()
                        .atMost(30, TimeUnit.SECONDS)
                        .untilAsserted(() -> {
                            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                            List<ConsumerRecord<String, String>> list = new ArrayList<>();
                            records.records(kafkaTopicDevices).forEach(list::add);

                        boolean found = list.stream().anyMatch(r -> r.value().equals(deviceEvent.getDeviceId().toString()));
                        assertThat(found).isEqualTo(true);

                            Long countEventsFromDeviceOutbox = jdbcTemplate.queryForObject(
                                    SELECT_COUNT_EVENTS_FROM_DEVICE_OUTBOX_WHERE_STATUS_IS_1, Long.class);
                            assertThat(countEventsFromDeviceOutbox).isEqualTo(1L);

                            Long countEventsFromDevice = jdbcTemplate.queryForObject(
                                    SELECT_COUNT_EVENTS_FROM_DEVICE_OUTBOX_WHERE_STATUS_IS_0, Long.class);
                            assertThat(countEventsFromDevice).isEqualTo(0L);
                        });
            }

            producer.send(new ProducerRecord<>(kafkaTopicEvents, deviceEvent.getDeviceId().toString(), deviceEvent));
            await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        Long countEventsFromDeviceEvents = jdbcTemplate.queryForObject(
                                SELECT_COUNT_EVENTS_FROM_DEVICE_EVENTS, Long.class);
                        assertThat(countEventsFromDeviceEvents).isEqualTo(2L);

                        Long countEventsFromDeviceOutbox = jdbcTemplate.queryForObject(
                                SELECT_COUNT_EVENTS_FROM_DEVICE_OUTBOX, Long.class);
                        assertThat(countEventsFromDeviceOutbox).isEqualTo(1L);
                    });
        }

    }
}

