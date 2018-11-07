package com.saf.security;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackages = "com.saf.security.h2.entity")
@EnableJpaRepositories(basePackages = {"com.saf.security.h2.dao"})
@ComponentScan(basePackages = {"com.saf"})
public class TomcatRun {
    public static void main(String[] args) {
        SpringApplication.run(TomcatRun.class, args);
    }
}
