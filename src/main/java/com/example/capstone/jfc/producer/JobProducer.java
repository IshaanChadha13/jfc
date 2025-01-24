package com.example.capstone.jfc.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class JobProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public JobProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendJobToTool(String topic, Map<String, Object> jobData) throws JsonProcessingException {
        kafkaTemplate.send(topic, jobData);
    }
}
