package dev.bognar.kafka_rabbitmq_benchmark.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.bognar.kafka_rabbitmq_benchmark.model.Transaction;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionLoader {
    private final ObjectMapper objectMapper;

    public Stream<List<Transaction>> loadTransactionBatches(String dataDir) {
        try {
            Path directoryPath = Paths.get(dataDir);
            Path manifestPath = directoryPath.resolve("manifest.json");

            if (!Files.exists(manifestPath)) {
                throw new RuntimeException("Manifest file not found in " + dataDir);
            }

            // Read manifest to get batch files
            ManifestFile manifest = objectMapper.readValue(manifestPath.toFile(), ManifestFile.class);
            log.info("Found manifest with {} total transactions in {} batches",
                    manifest.getTotalTransactions(), manifest.getTotalBatches());

            // Return a stream of transaction batches
            return manifest.getFiles().stream()
                    .map(filename -> directoryPath.resolve(filename))
                    .filter(Files::exists)
                    .map(path -> {
                        try {
                            return objectMapper.readValue(path.toFile(),
                                    new TypeReference<List<Transaction>>() {});
                        } catch (Exception e) {
                            log.error("Failed to read batch file: " + path, e);
                            return List.<Transaction>of();
                        }
                    })
                    .filter(batch -> !batch.isEmpty());

        } catch (Exception e) {
            throw new RuntimeException("Failed to load transaction data", e);
        }
    }

    @Data
    private static class ManifestFile {
        private int totalTransactions;
        private int totalBatches;
        private int batchSize;
        private String generatedAt;
        private List<String> files;
    }
}