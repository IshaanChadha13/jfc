package com.example.capstone.jfc.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.util.*;

@Component
public class LoadTestRunner implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoadTestRunner.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String ingestionTopic;

    // Potential tool IDs recognized by JFC
    private static final List<String> TOOLS = Arrays.asList("ToolA", "ToolB", "ToolC");

    public LoadTestRunner(KafkaTemplate<String, Object> kafkaTemplate,
                          // Read from application.yml
                          org.springframework.core.env.Environment env) {
        this.kafkaTemplate = kafkaTemplate;
        this.ingestionTopic = env.getProperty("jfc.topics.ingestion");
    }

    @Override
    public void run(String... args) throws Exception {
        // For demonstration, produce 10 random jobs on startup
        LOGGER.info("Generating 10 random jobs to ingestion topic...");
        for (int i = 1; i <= 50; i++) {
            // Random job data
            String jobId = "job-" + UUID.randomUUID();
            String toolId = TOOLS.get(new Random().nextInt(TOOLS.size()));
            String payload = "{\"data\":\"Random payload " + i + "\"}";
            int priority = new Random().nextInt(10) + 1; // 1–10

            Map<String, Object> message = new HashMap<>();
            message.put("jobId", jobId);
            message.put("toolId", toolId);
            message.put("payload", payload);
            message.put("priority", priority);

            kafkaTemplate.send(ingestionTopic, message);
            LOGGER.info("Sent random job {} for tool {}", jobId, toolId);
        }
    }
}
