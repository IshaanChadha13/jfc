package com.example.capstone.jfc.repository;

import com.example.capstone.jfc.model.JobEntity;
import com.example.capstone.jfc.model.JobStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobRepository extends JpaRepository<JobEntity, String> {

//    Page<JobEntity> findByStatus(JobStatus status, Pageable pageable);

    List<JobEntity> findByStatus(JobStatus status);

    int countByToolIdAndStatus(String toolId, JobStatus status); //change

    int countByStatus(JobStatus jobStatus);
}
