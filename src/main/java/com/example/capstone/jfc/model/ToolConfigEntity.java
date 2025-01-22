package com.example.capstone.jfc.model;

import jakarta.persistence.*;

@Entity
@Table(name = "tool_config")
public class ToolConfigEntity {

    @Id
    @Column(name = "tool_id", nullable = false)
    private String toolId;

    @Column(name = "max_concurrent_jobs")
    private Integer maxConcurrentJobs;

    @Column(name = "destination_topic")
    private String destinationTopic;

    public ToolConfigEntity() {}

    // Getters and Setters
    public String getToolId() {
        return toolId;
    }

    public void setToolId(String toolId) {
        this.toolId = toolId;
    }

    public Integer getMaxConcurrentJobs() {
        return maxConcurrentJobs;
    }

    public void setMaxConcurrentJobs(Integer maxConcurrentJobs) {
        this.maxConcurrentJobs = maxConcurrentJobs;
    }

    public String getDestinationTopic() {
        return destinationTopic;
    }

    public void setDestinationTopic(String destinationTopic) {
        this.destinationTopic = destinationTopic;
    }
}
