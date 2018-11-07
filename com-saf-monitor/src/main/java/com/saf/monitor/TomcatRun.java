package com.saf.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.saf"})
public class TomcatRun {
    public static void main(String[] args) {
        SpringApplication.run(TomcatRun.class, args);
    }
}
