package com.example.capstone.jfc.service;

import com.example.capstone.jfc.model.JobEntity;
import com.example.capstone.jfc.model.JobStatus;
import com.example.capstone.jfc.model.ToolConfigEntity;
import com.example.capstone.jfc.producer.JobProducer;
import com.example.capstone.jfc.repository.JobRepository;
import com.example.capstone.jfc.repository.ToolConfigRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
public class BatchDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchDispatcher.class);

    private final JobRepository jobRepository;
    private final ToolConfigRepository toolConfigRepository;
    private final JobProducer jobProducer;

    @Value("${dispatcher.thread.pool.size:3}")
    private int threadPoolSize;

    @Value("${dispatcher.jobpage.size:500}")
    private int jobPageSize;

//    private final ExecutorService executorService = Executors.newFixedThreadPool(3);
    private ExecutorService executorService;

    public BatchDispatcher(JobRepository jobRepository,
                           ToolConfigRepository toolConfigRepository,
                           JobProducer jobProducer) {
        this.jobRepository = jobRepository;
        this.toolConfigRepository = toolConfigRepository;
        this.jobProducer = jobProducer;
    }

    @PostConstruct
    public void init() {
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Scheduled(fixedRate = 10000)
    public void dispatchJobs() {
        LOGGER.info("Starting batch dispatch...");

        int pageNumber = 0;
        Page<JobEntity> page;
        do {
            PageRequest pageRequest = PageRequest.of(pageNumber, jobPageSize);
            page = jobRepository.findByStatus(JobStatus.NEW, pageRequest);

            // If the page has no content, log and break out.
            if (page.isEmpty()) {
                LOGGER.info("No NEW jobs found on page {}. Nothing to dispatch.", pageNumber);
                break;
            }

            // Get the list of NEW jobs from this page
            List<JobEntity> newJobs = page.getContent();

            // Group by toolId
            Map<String, List<JobEntity>> jobsByTool = newJobs.stream()
                    .collect(Collectors.groupingBy(JobEntity::getToolId));

            // Submit each tool’s batch for concurrent processing
            for (String toolId : jobsByTool.keySet()) {
                executorService.submit(() -> processToolBatch(toolId, jobsByTool.get(toolId)));
            }

            LOGGER.info("Batch dispatch triggered for {} tool(s) on page {}.", jobsByTool.size(), pageNumber);

            pageNumber++;

        } while (page.hasNext());
    }

    // Runs every 10 seconds (for example)
//    @Scheduled(fixedRate = 10000)
//    public void dispatchJobs() throws JsonProcessingException {
//        LOGGER.info("Starting batch dispatch...");
//
//        // Find all NEW jobs
//        List<JobEntity> newJobs = jobRepository.findByStatus(JobStatus.NEW);
//
//        if (newJobs.isEmpty()) {
//            LOGGER.info("No NEW jobs found. Nothing to dispatch.");
//            return;
//        }
//
//        Map<String, List<JobEntity>> jobsByTool = newJobs.stream()
//                .collect(Collectors.groupingBy(JobEntity::getToolId));
//
//        for (String toolId : jobsByTool.keySet()) {
//            executorService.submit(() -> processToolBatch(toolId, jobsByTool.get(toolId)));
//        }
//
//        LOGGER.info("Batch dispatch triggered for {} tool(s).", jobsByTool.size());
//    }
    private void processToolBatch(String toolId, List<JobEntity> toolJobs) {
        try {
            ToolConfigEntity config = toolConfigRepository.findById(toolId).orElse(null);
            if (config == null) {
                LOGGER.warn("No tool config found for tool: {}", toolId);
                return;
            }

            int maxConcurrent = config.getMaxConcurrentJobs();
            String destinationTopic = config.getDestinationTopic();

            toolJobs.sort(Comparator.comparing(JobEntity::getPriority).reversed());

            List<JobEntity> batch = toolJobs.stream()
                    .limit(maxConcurrent)
                    .collect(Collectors.toList());

            if (batch.isEmpty()) {
                LOGGER.info("No jobs to dispatch for tool {}", toolId);
                return;
            }

            LOGGER.info("Dispatching {} jobs for tool {} on thread {}",
                    batch.size(), toolId, Thread.currentThread().getName());

            List<Map<String, Object>> jobsInBatch = new ArrayList<>();
            for (JobEntity job : batch) {
                    Map<String, Object> jobData = new HashMap<>();
                    jobData.put("jobId", job.getJobId());
                    jobData.put("toolId", job.getToolId());
                    jobData.put("payload", job.getPayload());
                    jobData.put("priority", job.getPriority());
                    jobsInBatch.add(jobData);
                }

                Map<String, Object> batchMessage = new HashMap<>();
                batchMessage.put("toolId", toolId);
                batchMessage.put("jobs", jobsInBatch);

                jobProducer.sendJobToTool(destinationTopic, batchMessage);

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
