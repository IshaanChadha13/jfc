package com.example.capstone.jfc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class JfcApplication {
	public static void main(String[] args) {
		SpringApplication.run(JfcApplication.class, args);
	}
}
