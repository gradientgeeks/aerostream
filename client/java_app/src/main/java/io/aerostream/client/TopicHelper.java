package io.aerostream.client;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TopicHelper {
    private static final Logger log = LoggerFactory.getLogger(TopicHelper.class);

    public static void ensureTopic(String bootstrapServers, String topic, int partitions) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get();
            if (!existing.contains(topic)) {
                log.info("Creating topic '{}' with {} partitions on broker 1...", topic, partitions);
                Map<Integer, List<Integer>> replicaAssignments = new HashMap<>();
                for (int p = 0; p < partitions; p++) {
                    replicaAssignments.put(p, Collections.singletonList(1));
                }
                NewTopic newTopic = new NewTopic(topic, replicaAssignments);
                admin.createTopics(Collections.singleton(newTopic)).all().get();
                // Brief pause to allow metadata propagation
                Thread.sleep(300);
            }
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                log.warn("Notice while ensuring topic {}: {}", topic, e.getMessage());
            }
        } catch (Exception e) {
            log.warn("Notice while ensuring topic {}: {}", topic, e.getMessage());
        }
    }
}
