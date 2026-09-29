package io.aerostream.client;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Main CLI Runner & E2E Test Suite for AeroStream Kafka Wire Protocol.
 */
public class JavaClientE2ESuite {
    private static final Logger log = LoggerFactory.getLogger(JavaClientE2ESuite.class);

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        for (int i = 0; i < args.length; i++) {
            if ("--bootstrap-servers".equals(args[i]) && i + 1 < args.length) {
                bootstrapServers = args[++i];
            }
        }

        System.out.println("==========================================================================================");
        System.out.println("            AEROSTREAM KAFKA WIRE PROTOCOL - JAVA CLIENT E2E TEST SUITE                   ");
        System.out.println("   Target Bootstrap Servers: " + bootstrapServers);
        System.out.println("   Kafka Client Version    : 3.7.0 (Official Apache Kafka Java Client)");
        System.out.println("==========================================================================================");

        // Pre-create required topics using AdminClient
        ensureTopicsExist(bootstrapServers);

        List<TestResult> allResults = new ArrayList<>();
        long totalStart = System.currentTimeMillis();

        String baseTopic = "e2e-java-" + System.currentTimeMillis();

        System.out.println("\n>>> [1/4] Running Scenario 1: JavaProducerTest (Batching, Headers, Codecs, Routing, Acks)...");
        allResults.addAll(JavaProducerTest.runAll(bootstrapServers, baseTopic));

        System.out.println("\n>>> [2/4] Running Scenario 2: JavaConsumerTest (Rebalance, Sync/Async Commits, Seeks)...");
        allResults.addAll(JavaConsumerTest.runAll(bootstrapServers, baseTopic));

        System.out.println("\n>>> [3/4] Running Scenario 3: JavaTransactionalEosTest (KIP-98 EOS, Abort/Commit, Read-Committed)...");
        allResults.addAll(JavaTransactionalEosTest.runAll(bootstrapServers, baseTopic));

        System.out.println("\n>>> [4/4] Running Scenario 4: JavaIdempotenceTest (Idempotence, Monotonic Sequence, Retries)...");
        allResults.addAll(JavaIdempotenceTest.runAll(bootstrapServers, baseTopic));

        long totalElapsed = System.currentTimeMillis() - totalStart;

        // Print Structured Summary Table
        printSummaryTable(allResults, totalElapsed);

        boolean allPassed = allResults.stream().allMatch(r -> "PASS".equalsIgnoreCase(r.getStatus()));
        if (!allPassed) {
            System.err.println("\n[ERROR] One or more tests FAILED!");
            System.exit(1);
        } else {
            System.out.println("\n[SUCCESS] All AeroStream Java Kafka Client E2E tests passed successfully!");
            System.exit(0);
        }
    }

    private static void ensureTopicsExist(String bootstrapServers) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get();
            List<String> neededTopics = Arrays.asList(
                    "test-wire-basic",
                    "test-wire-compressed",
                    "test-wire-partitions",
                    "test-wire-txns",
                    "test-wire-groups"
            );

            List<NewTopic> toCreate = new ArrayList<>();
            for (String t : neededTopics) {
                if (!existing.contains(t)) {
                    toCreate.add(new NewTopic(t, 3, (short) 1));
                }
            }

            if (!toCreate.isEmpty()) {
                System.out.println("Creating " + toCreate.size() + " test topics via AdminClient...");
                try {
                    admin.createTopics(toCreate).all().get();
                } catch (ExecutionException e) {
                    // Ignore if topics already exist concurrently
                    log.info("Topic creation note: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("AdminClient topic provisioning note: {}", e.getMessage());
        }
    }

    private static void printSummaryTable(List<TestResult> results, long totalElapsedMs) {
        System.out.println("\n");
        System.out.println("+---------+------------------------------------------------------+--------+--------------+--------------------------------------------------------------+");
        System.out.println("| Test ID | Scenario / Test Name                                 | Status | Latency (ms) | Details / Metrics                                            |");
        System.out.println("+---------+------------------------------------------------------+--------+--------------+--------------------------------------------------------------+");

        int passed = 0;
        int failed = 0;

        for (TestResult r : results) {
            if ("PASS".equalsIgnoreCase(r.getStatus())) {
                passed++;
            } else {
                failed++;
            }

            String details = r.getStatus().equals("PASS") ? r.getDetails() : ("ERR: " + r.getError());
            if (details.length() > 60) {
                details = details.substring(0, 57) + "...";
            }

            System.out.printf("| %-7s | %-52s | %-6s | %12d | %-60s |\n",
                    r.getTestId(),
                    truncate(r.getName(), 52),
                    r.getStatus(),
                    r.getLatencyMs(),
                    details);
        }

        System.out.println("+---------+------------------------------------------------------+--------+--------------+--------------------------------------------------------------+");
        System.out.printf("| SUMMARY : Total: %d  |  Passed: %d  |  Failed: %d  | Total Time: %d ms                                               |\n",
                results.size(), passed, failed, totalElapsedMs);
        System.out.println("+-------------------------------------------------------------------------------------------------------------------------------------+");
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max - 3) + "...";
    }
}
