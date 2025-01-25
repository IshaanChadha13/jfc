package com.example.capstone.jfc.service;

import com.example.capstone.jfc.model.JobEntity;
import com.example.capstone.jfc.model.JobStatus;
import com.example.capstone.jfc.model.ToolConfigEntity;
import com.example.capstone.jfc.producer.JobProducer;
import com.example.capstone.jfc.repository.JobRepository;
import com.example.capstone.jfc.repository.ToolConfigRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class BatchDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchDispatcher.class);


    private final JobRepository jobRepository;
    private final ToolConfigRepository toolConfigRepository;
    private final JobProducer jobProducer;

    @Value("${jfc.global-concurrency-limit}")
    private int globalConcurrencyLimit;

    public BatchDispatcher(JobRepository jobRepository,
                           ToolConfigRepository toolConfigRepository,
                           JobProducer jobProducer) {
        this.jobRepository = jobRepository;
        this.toolConfigRepository = toolConfigRepository;
        this.jobProducer = jobProducer;
    }

    @Scheduled(fixedRate = 1000)
    public void dispatchJobs() throws JsonProcessingException {
        LOGGER.info("=== Starting dispatch cycle ===");

        int globalInProgress = jobRepository.countByStatus(JobStatus.IN_PROGRESS);
        LOGGER.info("Total IN_PROGRESS jobs (all tools): {}", globalInProgress);

        if (globalInProgress >= globalConcurrencyLimit) {
            LOGGER.info("Global concurrency limit reached. No new jobs can be dispatched.");
            return;
        }

        List<JobEntity> newJobs = jobRepository.findByStatus(JobStatus.NEW);
        if (newJobs.isEmpty()) {
            LOGGER.info("No NEW jobs available.");
            return;
        }
        newJobs.sort(Comparator.comparing(JobEntity::getTimestampCreated));

        int dispatchedCount = 0;

        for (JobEntity job : newJobs) {
            // global concurrency is at limit, break out entirely
            if (globalInProgress >= globalConcurrencyLimit) {
                LOGGER.info("Reached global concurrency limit while iterating jobs.");
                break;
            }

            String toolId = job.getToolId();
            ToolConfigEntity config = toolConfigRepository.findById(toolId).orElse(null);
            if (config == null) {
                LOGGER.warn("No config found for tool '{}'; skipping job {}", toolId, job.getJobId());
                continue; // skip this job, maybe the next job belongs to a valid tool
            }

            int toolLimit = config.getMaxConcurrentJobs();

            int toolInProgress = jobRepository.countByToolIdAndStatus(toolId, JobStatus.IN_PROGRESS);

            if (toolInProgress >= toolLimit) {
                LOGGER.info("Tool {} is at concurrency limit ({}). Cannot dispatch job {} right now.",
                        toolId, toolLimit, job.getJobId());
                continue;
            }

            job.setStatus(JobStatus.IN_PROGRESS);
            jobRepository.save(job);

            Map<String, Object> message = new HashMap<>();
            message.put("jobId", job.getJobId());
            message.put("toolId", job.getToolId());
            message.put("payload", job.getPayload());
            message.put("priority", job.getPriority());

            jobProducer.sendJobToTool(config.getDestinationTopic(), message);

            dispatchedCount++;
            globalInProgress++;

            LOGGER.info("Dispatched job {} for tool {}. (toolInProgress={} -> after increment, globalInProgress={})",
                    job.getJobId(), toolId, toolInProgress, globalInProgress);
        }

        LOGGER.info("Dispatch cycle complete. Dispatched {} new jobs. Now {} total IN_PROGRESS.",
                dispatchedCount, globalInProgress);
        LOGGER.info("=== End of dispatch cycle ===");
    }
}
