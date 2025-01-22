package com.example.capstone.jfc.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Value("${jfc.topics.ingestion}")
    private String ingestionTopic;

    @Value("${jfc.topics.status}")
    private String statusTopic;

    @Value("${jfc.topics.toolA}")
    private String toolATopic;

    @Value("${jfc.topics.toolB}")
    private String toolBTopic;

    @Value("${jfc.topics.toolC}")
    private String toolCTopic;

    /**
     * Optionally create topics automatically if auto-create is enabled.
     */
    @Bean
    public NewTopic ingestionTopic() {
        return new NewTopic(ingestionTopic, 3, (short) 1);
    }

    @Bean
    public NewTopic statusTopic() {
        return new NewTopic(statusTopic, 3, (short) 1);
    }

    @Bean
    public NewTopic toolATopic() {
        return new NewTopic(toolATopic, 3, (short) 1);
    }

    @Bean
    public NewTopic toolBTopic() {
        return new NewTopic(toolBTopic, 3, (short) 1);
    }

    @Bean
    public NewTopic toolCTopic() {
        return new NewTopic(toolCTopic, 3, (short) 1);
    }
}