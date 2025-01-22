package com.example.capstone.jfc.repository;

import com.example.capstone.jfc.model.JobEntity;
import com.example.capstone.jfc.model.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobRepository extends JpaRepository<JobEntity, String> {

    // Find jobs by status
    List<JobEntity> findByStatus(JobStatus status);

    // Potential custom queries:
    // e.g., find top N by tool ID and status
    List<JobEntity> findTop10ByToolIdAndStatusOrderByPriorityDesc(String toolId, JobStatus status);

    // Or similar queries for batching with concurrency
}
