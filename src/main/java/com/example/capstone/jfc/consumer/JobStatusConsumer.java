package com.example.capstone.jfc.consumer;

import com.example.capstone.jfc.model.JobEntity;
import com.example.capstone.jfc.model.JobStatus;
import com.example.capstone.jfc.repository.JobRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

@Component
public class JobStatusConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobStatusConsumer.class);

    private final JobRepository jobRepository;

    public JobStatusConsumer(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    @KafkaListener(topics = "#{ '${jfc.topics.status}' }", groupId = "jfc-status-consumer")
    public void onStatusMessage(Map<String, Object> statusMessage) {
        try {
            String jobId = (String) statusMessage.get("jobId");
            String statusString = (String) statusMessage.get("status");

            JobStatus newStatus = JobStatus.valueOf(statusString);

            JobEntity job = jobRepository.findById(jobId).orElse(null);
            if (job == null) {
                LOGGER.warn("Received status update for unknown job ID: {}", jobId);
                return;
            }

            job.setStatus(newStatus);
            jobRepository.save(job);

            LOGGER.info("Updated job {} to status {}", jobId, newStatus);
        } catch (Exception e) {
            LOGGER.error("Error processing job status message", e);
        }
    }
}
