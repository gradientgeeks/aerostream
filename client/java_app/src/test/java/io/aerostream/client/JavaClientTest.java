package io.aerostream.client;

import org.junit.jupiter.api.*;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JavaClientTest {
    private static final String BOOTSTRAP_SERVERS = System.getProperty("bootstrap.servers", "127.0.0.1:9092");
    private static final String BASE_TOPIC = "junit-e2e-" + System.currentTimeMillis();

    @Test
    @Order(1)
    @DisplayName("Scenario 1: JavaProducerTest Suite")
    void testProducerScenarios() {
        List<TestResult> results = JavaProducerTest.runAll(BOOTSTRAP_SERVERS, BASE_TOPIC);
        for (TestResult r : results) {
            assertEquals("PASS", r.getStatus(), "Producer test failed: " + r.getTestId() + " - " + r.getError());
        }
    }

    @Test
    @Order(2)
    @DisplayName("Scenario 2: JavaConsumerTest Suite")
    void testConsumerScenarios() {
        List<TestResult> results = JavaConsumerTest.runAll(BOOTSTRAP_SERVERS, BASE_TOPIC);
        for (TestResult r : results) {
            assertEquals("PASS", r.getStatus(), "Consumer test failed: " + r.getTestId() + " - " + r.getError());
        }
    }

    @Test
    @Order(3)
    @DisplayName("Scenario 3: JavaTransactionalEosTest Suite")
    void testTransactionalScenarios() {
        List<TestResult> results = JavaTransactionalEosTest.runAll(BOOTSTRAP_SERVERS, BASE_TOPIC);
        for (TestResult r : results) {
            assertEquals("PASS", r.getStatus(), "Transactional test failed: " + r.getTestId() + " - " + r.getError());
        }
    }

    @Test
    @Order(4)
    @DisplayName("Scenario 4: JavaIdempotenceTest Suite")
    void testIdempotenceScenarios() {
        List<TestResult> results = JavaIdempotenceTest.runAll(BOOTSTRAP_SERVERS, BASE_TOPIC);
        for (TestResult r : results) {
            assertEquals("PASS", r.getStatus(), "Idempotence test failed: " + r.getTestId() + " - " + r.getError());
        }
    }
}
