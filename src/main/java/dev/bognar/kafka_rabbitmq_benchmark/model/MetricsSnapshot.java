package dev.bognar.kafka_rabbitmq_benchmark.model;

public class MetricsSnapshot {

    private long messageCount;
    private double throughput;
    private double latency;
    private double memoryUsage;
    private double cpuUsage;

    // Constructor, getters, and setters
    public MetricsSnapshot() {
    }

    public MetricsSnapshot(long messageCount, double throughput, double latency, double memoryUsage, double cpuUsage) {
        this.messageCount = messageCount;
        this.throughput = throughput;
        this.latency = latency;
        this.memoryUsage = memoryUsage;
        this.cpuUsage = cpuUsage;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(long messageCount) {
        this.messageCount = messageCount;
    }

    public double getThroughput() {
        return throughput;
    }

    public void setThroughput(double throughput) {
        this.throughput = throughput;
    }

    public double getLatency() {
        return latency;
    }

    public void setLatency(double latency) {
        this.latency = latency;
    }

    public double getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(double memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }
}