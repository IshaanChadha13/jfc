package com.example.capstone.jfc.controller;

import com.example.capstone.jfc.model.JobEntity;
import com.example.capstone.jfc.model.JobStatus;
import com.example.capstone.jfc.model.ToolConfigEntity;
import com.example.capstone.jfc.repository.JobRepository;
import com.example.capstone.jfc.repository.ToolConfigRepository;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
@CrossOrigin
public class VisualizationController {

    private final JobRepository jobRepository;
    private final ToolConfigRepository toolConfigRepository;

    public VisualizationController(JobRepository jobRepository, ToolConfigRepository toolConfigRepository) {
        this.jobRepository = jobRepository;
        this.toolConfigRepository = toolConfigRepository;
    }

    // Returns all jobs with status = NEW
    @GetMapping("/jobs/new")
    public List<JobEntity> getNewJobs() {
        return jobRepository.findByStatus(JobStatus.NEW);
    }

    // Returns all jobs with status = IN_PROGRESS
    @GetMapping("/jobs/inprogress")
    public List<JobEntity> getInProgressJobs() {
        return jobRepository.findByStatus(JobStatus.IN_PROGRESS);
    }

    // (Optional) Return the tool configs so we can see each tool's concurrency limit
    @GetMapping("/tools")
    public List<ToolConfigEntity> getAllTools() {
        return toolConfigRepository.findAll();
    }
}
