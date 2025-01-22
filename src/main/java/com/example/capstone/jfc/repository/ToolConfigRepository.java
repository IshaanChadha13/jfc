package com.example.capstone.jfc.repository;

import com.example.capstone.jfc.model.ToolConfigEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ToolConfigRepository extends JpaRepository<ToolConfigEntity, String> {
    // By default, JPA provides findById(toolId)
}
