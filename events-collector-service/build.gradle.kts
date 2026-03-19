plugins {
	java
	id("org.springframework.boot") version "3.5.0"
	id("io.spring.dependency-management") version "1.1.7"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"
description = "Demo project for Spring Boot"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(24)
	}
}

repositories {
	mavenCentral()
	maven { url = uri("https://packages.confluent.io/maven/") }
}

val avroTools by configurations.creating

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.kafka:spring-kafka")
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("io.micrometer:micrometer-registry-prometheus")
	implementation("com.clickhouse:clickhouse-jdbc:0.9.4")
	implementation("org.springframework.boot:spring-boot-starter-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-data-redis")
	implementation("io.confluent:kafka-avro-serializer:7.6.0")
	implementation("org.apache.avro:avro:1.12.0")

	compileOnly("org.projectlombok:lombok")
	annotationProcessor("org.projectlombok:lombok")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.kafka:spring-kafka-test")
	testImplementation("org.testcontainers:junit-jupiter")
	testImplementation("org.testcontainers:kafka")
	testImplementation("org.testcontainers:clickhouse")
	testImplementation("org.testcontainers:testcontainers")
	testImplementation("org.awaitility:awaitility:4.2.1")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")

	avroTools("org.apache.avro:avro-tools:1.12.0")
}

sourceSets {
	main {
		java.srcDir("${buildDir}/generated/avro")
	}
}

tasks.register<JavaExec>("generateAvro") {
	group = "code generation"
	description = "Generate Java classes from Avro schema files"

	classpath = avroTools
	mainClass.set("org.apache.avro.tool.Main")

	args(
		"compile",
		"schema",
		"${projectDir}/src/main/resources/avro/DeviceEvent.avsc",
		"${buildDir}/generated/avro"
	)
}

tasks.named("compileJava") {
	dependsOn("generateAvro")
}

tasks.withType<Test> {
	useJUnitPlatform()
}