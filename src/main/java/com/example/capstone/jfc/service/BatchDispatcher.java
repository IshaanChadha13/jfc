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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
public class BatchDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchDispatcher.class);

    private final JobRepository jobRepository;
    private final ToolConfigRepository toolConfigRepository;
    private final JobProducer jobProducer;

    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    public BatchDispatcher(JobRepository jobRepository,
                           ToolConfigRepository toolConfigRepository,
                           JobProducer jobProducer) {
        this.jobRepository = jobRepository;
        this.toolConfigRepository = toolConfigRepository;
        this.jobProducer = jobProducer;
    }

    // Runs every 10 seconds (for example)
    @Scheduled(fixedRate = 10000)
    public void dispatchJobs() throws JsonProcessingException {
        LOGGER.info("Starting batch dispatch...");

        // Find all NEW jobs
        List<JobEntity> newJobs = jobRepository.findByStatus(JobStatus.NEW);

        if (newJobs.isEmpty()) {
            LOGGER.info("No NEW jobs found. Nothing to dispatch.");
            return;
        }
        // Group jobs by tool_id
        Map<String, List<JobEntity>> jobsByTool = newJobs.stream()
                .collect(Collectors.groupingBy(JobEntity::getToolId));
//        for (JobEntity job : newJobs) {
//            jobsByTool.computeIfAbsent(job.getToolId(), k -> new java.util.ArrayList<>()).add(job);
//        }

        for (String toolId : jobsByTool.keySet()) {
            executorService.submit(() -> processToolBatch(toolId, jobsByTool.get(toolId)));
        }

        LOGGER.info("Batch dispatch triggered for {} tool(s).", jobsByTool.size());
    }
    private void processToolBatch(String toolId, List<JobEntity> toolJobs) {
        try {
            ToolConfigEntity config = toolConfigRepository.findById(toolId).orElse(null);
            if (config == null) {
                LOGGER.warn("No tool config found for tool: {}", toolId);
                return;
            }

            int maxConcurrent = config.getMaxConcurrentJobs();
            String destinationTopic = config.getDestinationTopic();

                // 1) Sort or filter if needed by priority, time, etc.
                // toolJobs.sort(Comparator.comparing(JobEntity::getPriority).reversed());

                // 2) Take up to maxConcurrent jobs for this batch
            List<JobEntity> batch = toolJobs.stream()
                    .limit(maxConcurrent)
                    .collect(Collectors.toList());

            if (batch.isEmpty()) {
                LOGGER.info("No jobs to dispatch for tool {}", toolId);
                return;
            }

            LOGGER.info("Dispatching {} jobs for tool {} on thread {}",
                    batch.size(), toolId, Thread.currentThread().getName());

                // 3) Build a SINGLE message containing all jobs in this batch
            List<Map<String, Object>> jobsInBatch = new ArrayList<>();
            for (JobEntity job : batch) {
                    Map<String, Object> jobData = new HashMap<>();
                    jobData.put("jobId", job.getJobId());
                    jobData.put("toolId", job.getToolId());
                    jobData.put("payload", job.getPayload());
                    jobData.put("priority", job.getPriority());
                    jobsInBatch.add(jobData);
                }

                // The overall "batch" message
                Map<String, Object> batchMessage = new HashMap<>();
                batchMessage.put("toolId", toolId);
                batchMessage.put("jobs", jobsInBatch);

                // 4) Produce the single "batch" to the tool's destination topic
                jobProducer.sendJobToTool(destinationTopic, batchMessage);

                // 5) Update each job's status to PENDING
                for (JobEntity job : batch) {
                    job.setStatus(JobStatus.PENDING);
                }
                jobRepository.saveAll(batch);

                LOGGER.info("Finished dispatch for tool {} on thread {}", toolId, Thread.currentThread().getName());

            } catch (Exception e) {
                LOGGER.error("Error dispatching jobs for tool {}", toolId, e);
            }
    }
}
