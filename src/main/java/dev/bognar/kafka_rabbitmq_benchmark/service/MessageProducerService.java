package dev.bognar.kafka_rabbitmq_benchmark.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.bognar.kafka_rabbitmq_benchmark.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RabbitTemplate rabbitTemplate;
    private final MessagingPerformanceMonitor monitor;
    private final ObjectMapper objectMapper;

    public void sendKafkaTransaction(Transaction transaction) {
        try {
            long startTime = System.nanoTime();
            String jsonMessage = objectMapper.writeValueAsString(transaction);

            kafkaTemplate.send("transactions", transaction.getTransactionId(), jsonMessage)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            long latency = (System.nanoTime() - startTime) / 1_000_000;
                            Runtime runtime = Runtime.getRuntime();
                            double memoryUsage = (runtime.totalMemory() - runtime.freeMemory())
                                    / (1024.0 * 1024.0);

                            com.sun.management.OperatingSystemMXBean osBean =
                                    (com.sun.management.OperatingSystemMXBean)
                                            java.lang.management.ManagementFactory.getOperatingSystemMXBean();
                            double cpuUsage = osBean.getCpuLoad() * 100;

                            monitor.recordMetrics("kafka", latency, memoryUsage, cpuUsage);
                        } else {
                            log.error("Failed to send Kafka transaction: {} - {}", transaction.getTransactionId(), ex.getMessage());
                        }
                    });
        } catch (Exception e) {
            log.error("Failed to send Kafka transaction: {} - {}", transaction.getTransactionId(), e.getMessage());
        }
    }

    public void sendRabbitTransaction(Transaction transaction) {
        try {
            long startTime = System.nanoTime();
            String jsonMessage = objectMapper.writeValueAsString(transaction);

            rabbitTemplate.convertAndSend("transactions", jsonMessage, message -> {
                MessageProperties messageProperties = message.getMessageProperties();
                if (messageProperties != null) {
                    messageProperties.setMessageId(transaction.getTransactionId());
                } else {
                    log.warn("Message properties are null for RabbitMQ message");
                }
                return message;
            });

            long endTime = System.nanoTime();
            long latency = (endTime - startTime) / 1_000_000;
            Runtime runtime = Runtime.getRuntime();
            double memoryUsage = (runtime.totalMemory() - runtime.freeMemory())
                    / (1024.0 * 1024.0);

            com.sun.management.OperatingSystemMXBean osBean =
                    (com.sun.management.OperatingSystemMXBean)
                            java.lang.management.ManagementFactory.getOperatingSystemMXBean();
            double cpuUsage = osBean.getCpuLoad() * 100;

            monitor.recordMetrics("rabbitmq", latency, memoryUsage, cpuUsage);

        } catch (Exception e) {
            log.error("Failed to send RabbitMQ transaction: {} - {}", transaction.getTransactionId(), e.getMessage());
        }
    }
}