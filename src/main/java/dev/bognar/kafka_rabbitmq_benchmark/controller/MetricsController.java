package dev.bognar.kafka_rabbitmq_benchmark.controller;

import dev.bognar.kafka_rabbitmq_benchmark.service.MessagingPerformanceMonitor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private final MessagingPerformanceMonitor monitor;

    public MetricsController(MessagingPerformanceMonitor monitor) {
        this.monitor = monitor;
    }

    @GetMapping
    public Object getMetrics() {
        return Map.of(
                "kafka", monitor.getMetrics("kafka"),
                "rabbitmq", monitor.getMetrics("rabbitmq")
        );
    }
}