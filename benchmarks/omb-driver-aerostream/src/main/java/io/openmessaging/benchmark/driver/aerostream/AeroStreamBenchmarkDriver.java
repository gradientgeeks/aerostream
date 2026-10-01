/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.aerostream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * OMB driver for the AeroStream native protocol. Topics are created through the controller REST API; data goes
 * over the broker's native TCP data plane. Consumers of one subscription split the partitions statically (every
 * n-th partition), which matches OMB's local mode where all consumers are created in one call.
 */
@Slf4j
public class AeroStreamBenchmarkDriver implements BenchmarkDriver {
    private Config config;
    private HttpClient http;
    private final Map<String, Integer> partitionsByTopic = new ConcurrentHashMap<>();
    private final List<AutoCloseable> resources = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = mapper.readValue(configurationFile, Config.class);
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        log.info(
                "AeroStream native driver: controller {}, broker {}",
                config.controllerHttpUrl,
                config.brokerAddress);
    }

    @Override
    public String getTopicNamePrefix() {
        return "test-topic";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        String body =
                String.format(
                        "{\"name\":\"%s\",\"partitions\":%d,\"replication_factor\":%d}",
                        topic, partitions, config.replicationFactor);
        HttpRequest req =
                HttpRequest.newBuilder(URI.create(config.controllerHttpUrl + "/api/topics"))
                        .timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
        return http.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenAccept(
                        resp -> {
                            if (resp.statusCode() / 100 != 2) {
                                throw new IllegalStateException(
                                        "create topic " + topic + " failed: " + resp.statusCode() + " " + resp.body());
                            }
                            partitionsByTopic.put(topic, partitions);
                        });
    }

    private int partitionsOf(String topic) {
        Integer n = partitionsByTopic.get(topic);
        if (n == null) {
            throw new IllegalStateException("unknown topic " + topic + " (not created by this driver)");
        }
        return n;
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        try {
            AeroStreamBenchmarkProducer p = new AeroStreamBenchmarkProducer(config, topic, partitionsOf(topic));
            resources.add(p);
            return CompletableFuture.completedFuture(p);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        List<Integer> all = new ArrayList<>();
        for (int p = 0; p < partitionsOf(topic); p++) {
            all.add(p);
        }
        return newConsumer(topic, all, consumerCallback);
    }

    @Override
    public CompletableFuture<List<BenchmarkConsumer>> createConsumers(List<ConsumerInfo> consumers) {
        Map<String, List<ConsumerInfo>> bySubscription = new LinkedHashMap<>();
        for (ConsumerInfo ci : consumers) {
            bySubscription
                    .computeIfAbsent(ci.getTopic() + "\u0000" + ci.getSubscriptionName(), k -> new ArrayList<>())
                    .add(ci);
        }
        List<CompletableFuture<BenchmarkConsumer>> futures = new ArrayList<>();
        for (List<ConsumerInfo> members : bySubscription.values()) {
            String topic = members.get(0).getTopic();
            int n = partitionsOf(topic);
            for (int i = 0; i < members.size(); i++) {
                List<Integer> assigned = new ArrayList<>();
                for (int p = i; p < n; p += members.size()) {
                    assigned.add(p);
                }
                futures.add(newConsumer(topic, assigned, members.get(i).getConsumerCallback()));
            }
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(java.util.stream.Collectors.toList()));
    }

    private CompletableFuture<BenchmarkConsumer> newConsumer(
            String topic, List<Integer> partitions, ConsumerCallback callback) {
        try {
            AeroStreamBenchmarkConsumer c =
                    new AeroStreamBenchmarkConsumer(config, topic, partitions, callback);
            resources.add(c);
            return CompletableFuture.completedFuture(c);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void close() throws Exception {
        for (AutoCloseable r : resources) {
            r.close();
        }
        resources.clear();
    }

    private static final ObjectMapper mapper =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
}
