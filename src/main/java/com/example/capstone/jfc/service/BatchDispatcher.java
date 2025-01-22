package com.example.capstone.jfc.service;

import com.example.capstone.jfc.model.JobEntity;
import com.example.capstone.jfc.model.JobStatus;
import com.example.capstone.jfc.model.ToolConfigEntity;
import com.example.capstone.jfc.producer.JobProducer;
import com.example.capstone.jfc.repository.JobRepository;
import com.example.capstone.jfc.repository.ToolConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchDispatcher.class);

    private final JobRepository jobRepository;
    private final ToolConfigRepository toolConfigRepository;
    private final JobProducer jobProducer;

    public BatchDispatcher(JobRepository jobRepository,
                           ToolConfigRepository toolConfigRepository,
                           JobProducer jobProducer) {
        this.jobRepository = jobRepository;
        this.toolConfigRepository = toolConfigRepository;
        this.jobProducer = jobProducer;
    }

    // Runs every 10 seconds (for example)
    @Scheduled(fixedRate = 10000)
    public void dispatchJobs() {
        LOGGER.info("Starting batch dispatch...");

        // Find all NEW jobs
        List<JobEntity> newJobs = jobRepository.findByStatus(JobStatus.NEW);

        // Group jobs by tool_id
        Map<String, List<JobEntity>> jobsByTool = new HashMap<>();
        for (JobEntity job : newJobs) {
            jobsByTool.computeIfAbsent(job.getToolId(), k -> new java.util.ArrayList<>()).add(job);
        }

        // For each tool, check concurrency limit and dispatch
        for (String toolId : jobsByTool.keySet()) {
            ToolConfigEntity config = toolConfigRepository.findById(toolId).orElse(null);
            if (config == null) {
                LOGGER.warn("No tool config found for tool: {}", toolId);
                continue;
            }

            int maxConcurrent = config.getMaxConcurrentJobs();
            String destinationTopic = config.getDestinationTopic();

            List<JobEntity> toolJobs = jobsByTool.get(toolId);

            // Sort (optional) by priority descending, etc.
            // toolJobs.sort(Comparator.comparing(JobEntity::getPriority).reversed());

            // Take up to maxConcurrent
            List<JobEntity> batch = toolJobs.stream()
                    .limit(maxConcurrent)
                    .toList();

            // Dispatch each job
            for (JobEntity job : batch) {
                Map<String, Object> message = new HashMap<>();
                message.put("jobId", job.getJobId());
                message.put("toolId", job.getToolId());
                message.put("payload", job.getPayload());
                message.put("priority", job.getPriority());

                jobProducer.sendJobToTool(destinationTopic, message);

                // Update status to PENDING
                job.setStatus(JobStatus.PENDING);
                jobRepository.save(job);
            }
        }

        LOGGER.info("Batch dispatch complete.");
    }
}
