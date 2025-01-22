package com.example.capstone.jfc.model;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "jobs")
public class JobEntity {

    @Id
    @Column(name = "job_id", nullable = false, updatable = false)
    private String jobId;

    @Column(name = "tool_id")
    private String toolId;

    @Column(name = "payload")
    private String payload; // could store JSON as string

    @Column(name = "priority")
    private Integer priority;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private JobStatus status;

    @Column(name = "timestamp_created")
    private LocalDateTime timestampCreated;

    @Column(name = "timestamp_updated")
    private LocalDateTime timestampUpdated;

    @PrePersist
    public void onPrePersist() {
        if(timestampCreated == null) {
            timestampCreated = LocalDateTime.now();
        }
        timestampUpdated = LocalDateTime.now();
    }

    @PreUpdate
    public void onPreUpdate() {
        timestampUpdated = LocalDateTime.now();
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getToolId() {
        return toolId;
    }

    public void setToolId(String toolId) {
        this.toolId = toolId;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    public LocalDateTime getTimestampCreated() {
        return timestampCreated;
    }

    public void setTimestampCreated(LocalDateTime timestampCreated) {
        this.timestampCreated = timestampCreated;
    }

    public LocalDateTime getTimestampUpdated() {
        return timestampUpdated;
    }

    public void setTimestampUpdated(LocalDateTime timestampUpdated) {
        this.timestampUpdated = timestampUpdated;
    }
}
