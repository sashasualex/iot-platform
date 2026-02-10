package com.nashkod.eventscollector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class EventsCollectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventsCollectorApplication.class, args);
    }
}
