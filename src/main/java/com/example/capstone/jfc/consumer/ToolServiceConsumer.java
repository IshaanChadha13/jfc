package com.example.capstone.jfc.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Random;

@Component
public class ToolServiceConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ToolServiceConsumer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${jfc.topics.status}")
    private String commonStatusTopic;

    private final Random random = new Random();

    public ToolServiceConsumer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Single method that listens to multiple tool topics:
     *   - toolA-destination
     *   - toolB-destination
     *   - toolC-destination
     *
     * Use a comma-separated list in 'topics' or a pattern if you want.
     */
    @KafkaListener(topics = {
            "${jfc.topics.toolA}",
            "${jfc.topics.toolB}",
            "${jfc.topics.toolC}"
    },
            groupId = "simulated-tools-group")
    public void onToolMessage(ConsumerRecord<String, Object> record) {
        Object value = record.value();
        if (!(value instanceof Map)) {
            LOGGER.warn("Received unexpected message type: {}", value.getClass());
            return;
        }
        Map<?, ?> jobMessage = (Map<?, ?>) value;

        String jobId = (String) jobMessage.get("jobId");
        String toolId = (String) jobMessage.get("toolId");

        LOGGER.info("Received job {} for tool {}. Simulating processing...", jobId, toolId);

        // Simulate random processing time (1–5 seconds)
        try {
            int sleepMs = 1000 + random.nextInt(4000);
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 75% chance success, 25% fail
        boolean success = random.nextInt(100) < 75;
        String status = success ? "SUCCESS" : "FAIL";

        // Produce status to common-job-status
        Map<String, Object> statusMessage = Map.of(
                "jobId", jobId,
                "toolId", toolId,
                "status", status
        );

        kafkaTemplate.send(commonStatusTopic, statusMessage);

        LOGGER.info("Tool consumer for tool {} completed job {} with status {}", toolId, jobId, status);
    }
}
