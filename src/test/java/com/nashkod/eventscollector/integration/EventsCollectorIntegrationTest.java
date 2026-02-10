package com.nashkod.eventscollector.integration;

import com.nashkod.avro.DeviceEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class EventsCollectorIntegrationTest {

    static final Network NETWORK = Network.newNetwork();
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1")).withNetwork(NETWORK).withNetworkAliases("kafka");
    static final GenericContainer<?> SCHEMA_REGISTRY = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1"))
            .withNetwork(NETWORK)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
            .dependsOn(KAFKA);
    static final ClickHouseContainer CLICKHOUSE = new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:24.8"));
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    com.nashkod.eventscollector.service.OutboxScheduler outboxScheduler;

    @BeforeAll
    static void start() throws Exception {
        KAFKA.start();
        SCHEMA_REGISTRY.start();
        CLICKHOUSE.start();
        REDIS.start();

        try (AdminClient adminClient = AdminClient.create(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            adminClient.createTopics(List.of(new NewTopic("events", 3, (short) 1), new NewTopic("devices", 3, (short) 1))).all().get();
        }
    }

    @AfterAll
    static void stop() {
        REDIS.stop();
        CLICKHOUSE.stop();
        SCHEMA_REGISTRY.stop();
        KAFKA.stop();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.kafka.properties.schema.registry.url", () -> "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081));
        r.add("spring.datasource.url", CLICKHOUSE::getJdbcUrl);
        r.add("spring.datasource.username", CLICKHOUSE::getUsername);
        r.add("spring.datasource.password", CLICKHOUSE::getPassword);
        r.add("spring.data.redis.host", REDIS::getHost);
        r.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        r.add("app.outbox.cron-delay-ms", () -> "600000");
    }

    @Test
    void shouldProcessAndPublishThroughOutbox() throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        p.put("schema.registry.url", "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081));

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(p)) {
            DeviceEvent event = DeviceEvent.newBuilder()
                    .setEventId("event-1")
                    .setDeviceId("device-1")
                    .setTimestamp(System.currentTimeMillis())
                    .setType("temperature")
                    .setPayload("{\"v\":10}")
                    .build();
            producer.send(new ProducerRecord<>("events", event.getDeviceId().toString(), event)).get();
        }

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Long count = jdbcTemplate.queryForObject("SELECT count() FROM device_events WHERE event_id='event-1'", Long.class);
            assertThat(count).isEqualTo(1L);
            Long outboxCount = jdbcTemplate.queryForObject("SELECT count() FROM device_outbox WHERE device_id='device-1'", Long.class);
            assertThat(outboxCount).isEqualTo(1L);
        });

        outboxScheduler.publishOutbox();

        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, "it-devices");
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (Consumer<String, String> consumer = new KafkaConsumer<>(cp)) {
            consumer.subscribe(List.of("devices"));
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                assertThat(records).isNotEmpty();
                ConsumerRecord<String, String> first = records.iterator().next();
                assertThat(first.value()).isEqualTo("device-1");
            });
        }

        Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Long sent = jdbcTemplate.queryForObject("SELECT count() FROM device_outbox WHERE device_id='device-1' AND status=1", Long.class);
            assertThat(sent).isEqualTo(1L);
        });
    }
}
