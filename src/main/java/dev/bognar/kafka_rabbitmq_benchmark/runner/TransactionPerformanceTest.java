package dev.bognar.kafka_rabbitmq_benchmark.runner;

import dev.bognar.kafka_rabbitmq_benchmark.model.Transaction;
import dev.bognar.kafka_rabbitmq_benchmark.service.MessageProducerService;
import dev.bognar.kafka_rabbitmq_benchmark.service.MessagingPerformanceMonitor;
import dev.bognar.kafka_rabbitmq_benchmark.util.TransactionLoader;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionPerformanceTest implements CommandLineRunner {
    private final TransactionLoader transactionLoader;
    private final MessageProducerService producer;
    private final MessagingPerformanceMonitor monitor;

    private static final int BATCH_SIZE = 1000;
    private static final Duration TEST_DURATION = Duration.ofMinutes(1);

    @Override
    public void run(String... args) {
        String dataDir = System.getProperty("data.dir", "./data");
        log.info("Starting performance test using data from: {}", dataDir);

        // Load all transaction batches
        List<List<Transaction>> allBatches = transactionLoader
                .loadTransactionBatches(dataDir)
                .toList();

        log.info("Loaded {} transaction batches", allBatches.size());

        // Run performance test
        Instant startTime = Instant.now();
        AtomicInteger batchIndex = new AtomicInteger(0);
        AtomicInteger totalProcessed = new AtomicInteger(0);

        while (Duration.between(startTime, Instant.now()).compareTo(TEST_DURATION) < 0) {
            // Get next batch (cycling through all batches)
            List<Transaction> currentBatch = allBatches.get(
                    batchIndex.getAndUpdate(i -> (i + 1) % allBatches.size())
            );

            // Process batch through both systems concurrently
            CompletableFuture.allOf(
                    CompletableFuture.runAsync(() -> processBatchKafka(currentBatch)),
                    CompletableFuture.runAsync(() -> processBatchRabbitMQ(currentBatch))
            ).join();

            int processed = totalProcessed.addAndGet(currentBatch.size() * 2);
            log.info("Processed {} transactions total", processed);

            // Optional: Add small delay between batches to prevent overwhelming
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.info("Performance test completed. Total duration: {}",
                Duration.between(startTime, Instant.now()));
    }

    private void processBatchKafka(List<Transaction> batch) {
        batch.forEach(transaction -> {
            try {
                producer.sendKafkaTransaction(transaction);
            } catch (Exception e) {
                log.error("Failed to send transaction to Kafka: {}", transaction.getTransactionId(), e);
            }
        });
    }

    private void processBatchRabbitMQ(List<Transaction> batch) {
        batch.forEach(transaction -> {
            try {
                producer.sendRabbitTransaction(transaction);
            } catch (Exception e) {
                log.error("Failed to send transaction to RabbitMQ: {}", transaction.getTransactionId(), e);
            }
        });
    }
}