package com.example.events_collector_service.config;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.time.Duration;


public abstract class TestcontainersConfig {

    @Container
    static final ConfluentKafkaContainer KAFKA_CONTAINER;
    @Container
    static final GenericContainer<?> REDIS_CONTAINER;
    @Container
    static final GenericContainer<?> CLICKHOUSE_CONTAINER;
    @Container
    static final GenericContainer<?> SCHEMA_REGISTRY_CONTAINER;

    static final Network NETWORK = Network.newNetwork();

    static {
        KAFKA_CONTAINER = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.7"))
                .withNetwork(NETWORK)
                .withNetworkAliases("kafka")
                .withExposedPorts(9092)
                .withListener("kafka:19092");
        REDIS_CONTAINER = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                .withExposedPorts(6379);

        CLICKHOUSE_CONTAINER = new GenericContainer<>(DockerImageName.parse("clickhouse/clickhouse-server:latest"))
                .withExposedPorts(8123)
                .withEnv("CLICKHOUSE_DB", "events")
                .withEnv("CLICKHOUSE_USER", "test")
                .withEnv("CLICKHOUSE_PASSWORD", "test123")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(
                                Path.of("..", "infrastructure", "clickhouse_table")
                                        .toAbsolutePath()
                                        .normalize()
                        ),
                        "/docker-entrypoint-initdb.d/"
                )
                .waitingFor(
                        Wait.forHttp("/ping")
                                .forPort(8123)
                                .forStatusCode(200)
                )
                .withStartupTimeout(Duration.ofMinutes(2));;

        SCHEMA_REGISTRY_CONTAINER = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.0"))
                .withNetwork(NETWORK)
                .withNetworkAliases("schema-registry")
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");

        KAFKA_CONTAINER.start();
        REDIS_CONTAINER.start();
        CLICKHOUSE_CONTAINER.start();
        SCHEMA_REGISTRY_CONTAINER.start();
    }

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA_CONTAINER::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS_CONTAINER::getHost);
        registry.add("spring.data.redis.port", () -> REDIS_CONTAINER.getMappedPort(6379));
        registry.add("spring.datasource.url", () -> "jdbc:clickhouse://" + CLICKHOUSE_CONTAINER.getHost() + ":" + CLICKHOUSE_CONTAINER.getMappedPort(8123));
        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> "http://" + SCHEMA_REGISTRY_CONTAINER.getHost() + ":" + SCHEMA_REGISTRY_CONTAINER.getMappedPort(8081));
        registry.add("spring.kafka.consumer.properties.schema.registry.url", () -> "http://" + SCHEMA_REGISTRY_CONTAINER.getHost() + ":" + SCHEMA_REGISTRY_CONTAINER.getMappedPort(8081));
    }
}

