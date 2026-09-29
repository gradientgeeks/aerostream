package io.aerostream.client;

import java.util.Collections;
import java.util.Map;

public class TestResult {
    private final String testId;
    private final String name;
    private final String status; // PASS / FAIL
    private final long latencyMs;
    private final double throughputMsgSec;
    private final String details;
    private final String error;

    public TestResult(String testId, String name, String status, long latencyMs, double throughputMsgSec, String details, String error) {
        this.testId = testId;
        this.name = name;
        this.status = status;
        this.latencyMs = latencyMs;
        this.throughputMsgSec = throughputMsgSec;
        this.details = details != null ? details : "";
        this.error = error;
    }

    public static TestResult pass(String testId, String name, long latencyMs, String details) {
        return new TestResult(testId, name, "PASS", latencyMs, 0.0, details, null);
    }

    public static TestResult pass(String testId, String name, long latencyMs, double throughputMsgSec, String details) {
        return new TestResult(testId, name, "PASS", latencyMs, throughputMsgSec, details, null);
    }

    public static TestResult fail(String testId, String name, long latencyMs, String error) {
        return new TestResult(testId, name, "FAIL", latencyMs, 0.0, "", error);
    }

    public String getTestId() {
        return testId;
    }

    public String getName() {
        return name;
    }

    public String getStatus() {
        return status;
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public double getThroughputMsgSec() {
        return throughputMsgSec;
    }

    public String getDetails() {
        return details;
    }

    public String getError() {
        return error;
    }
}
