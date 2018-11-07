package com.saf;

import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.saf"})
public class TomcatRun {
    private static final Logger LOGGER = Logger.getLogger(TomcatRun.class);

    public static void main(String[] args) {
        SpringApplication.run(TomcatRun.class, args);
    }
}
