    package dev.bognar.kafka_rabbitmq_benchmark.service;

    import org.springframework.stereotype.Service;
    import java.io.FileWriter;
    import java.io.IOException;
    import java.time.LocalDateTime;
    import java.util.concurrent.ConcurrentHashMap;
    import java.util.concurrent.atomic.AtomicLong;
    import lombok.Data;

    @Service
    public class MessagingPerformanceMonitor {
        private final ConcurrentHashMap<String, MetricsSnapshot> metricsMap = new ConcurrentHashMap<>();
        private final String CSV_FILE = "messaging_metrics.csv";
        private static final String CSV_HEADER = "timestamp,broker,messageCount,throughput,latency,memoryUsage,cpuUsage\n";

        @Data
        public static class MetricsSnapshot {
            private LocalDateTime timestamp;
            private String broker; // "kafka" or "rabbitmq"
            private AtomicLong messageCount = new AtomicLong(0);
            private double throughput; // messages/second
            private double latency; // milliseconds
            private double memoryUsage; // MB
            private double cpuUsage; // percentage

            public MetricsSnapshot(String broker) {
                this.broker = broker;
                this.timestamp = LocalDateTime.now();
            }
        }

        public MessagingPerformanceMonitor() {
            initializeCsvFile();
        }

        private void initializeCsvFile() {
            try (FileWriter writer = new FileWriter(CSV_FILE)) {
                writer.append(CSV_HEADER);
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize CSV file", e);
            }
        }

        public void recordMetrics(String broker, double latency, double memoryUsage, double cpuUsage) {
            MetricsSnapshot metrics = metricsMap.computeIfAbsent(broker,
                    k -> new MetricsSnapshot(broker));

            metrics.setTimestamp(LocalDateTime.now());
            metrics.getMessageCount().incrementAndGet();
            metrics.setLatency(latency);
            metrics.setMemoryUsage(memoryUsage);
            metrics.setCpuUsage(cpuUsage);

            // Calculate throughput based on recent message count
            metrics.setThroughput(calculateThroughput(broker));

            // Write to CSV
            appendToCsv(metrics);
        }

        private void appendToCsv(MetricsSnapshot metrics) {
            try (FileWriter writer = new FileWriter(CSV_FILE, true)) {
                writer.append(String.format("%s,%s,%d,%.2f,%.2f,%.2f,%.2f\n",
                        metrics.getTimestamp(),
                        metrics.getBroker(),
                        metrics.getMessageCount().get(),
                        metrics.getThroughput(),
                        metrics.getLatency(),
                        metrics.getMemoryUsage(),
                        metrics.getCpuUsage()));
            } catch (IOException e) {
                throw new RuntimeException("Failed to write to CSV", e);
            }
        }

        private double calculateThroughput(String broker) {
            long currentTimeMillis = System.currentTimeMillis();
            long startTimeSeconds = getStartTime().toEpochSecond(java.time.ZoneOffset.UTC) * 1000; // Convert seconds to millis
            long elapsedMillis = currentTimeMillis - startTimeSeconds;

            if (elapsedMillis > 0) {
                return (double) metricsMap.get(broker).getMessageCount().get() / elapsedMillis * 1000; // Multiply by 1000 to get messages/second
            } else {
                return 0;
            }
        }

        public MetricsSnapshot getMetrics(String broker) {
            return metricsMap.get(broker);
        }

        private LocalDateTime getStartTime() {
            return LocalDateTime.now().minusSeconds(1);
        }
    }