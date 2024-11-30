package dev.bognar.kafka_rabbitmq_benchmark.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction {
    private String transactionId;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS", timezone = "GMT")
    private Instant timestamp;
    private String type;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String merchant;
    private String customerId;
    private String paymentMethod;
    private Map<String, String> metadata;
}