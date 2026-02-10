package com.nashkod.eventscollector.config;

import com.nashkod.eventscollector.repository.ClickHouseRepository;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SchemaInitializer {

    @Bean
    ApplicationRunner initClickHouseSchema(ClickHouseRepository clickHouseRepository) {
        return args -> clickHouseRepository.initSchema();
    }
}
