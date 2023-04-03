package com.dk0124.cdr.pullapp.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * SCAN ALL COMPONENT
 */
@Configuration
@ComponentScan(basePackages = {"com.dk0124.cdr"})
public class ScanConfig {
}
