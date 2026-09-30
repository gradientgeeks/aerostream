package org.gradientgeeks.aerostream.client;

import org.gradientgeeks.aerostream.common.AeroException;
import org.gradientgeeks.aerostream.common.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;

/**
 * Top-level client for AeroStream broker connectivity and resource factory.
 */
public class AeroClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AeroClient.class);

    private final List<String> bootstrapServers;
    private final String token;
    private final boolean useTls;
    private final SSLContext sslContext;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;
    private final int maxReconnectAttempts;
    private final long initialBackoffMs;

    private final Map<String, AeroConnection> connections = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "aerostream-client-worker");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean closed = false;

    public AeroClient(AeroClientBuilder builder) {
        if (builder.bootstrapServers().isEmpty()) {
            throw new IllegalArgumentException("At least one bootstrap server must be configured");
        }
        this.bootstrapServers = new ArrayList<>(builder.bootstrapServers());
        this.token = builder.token();
        this.useTls = builder.useTls();
        this.sslContext = builder.sslContext();
        this.connectTimeoutMs = builder.connectTimeoutMs();
        this.socketTimeoutMs = builder.socketTimeoutMs();
        this.maxReconnectAttempts = builder.maxReconnectAttempts();
        this.initialBackoffMs = builder.initialBackoffMs();
    }

    public static AeroClientBuilder builder() {
        return new AeroClientBuilder();
    }

    public static AeroClient connect(String bootstrapServer) {
        return builder().bootstrapServer(bootstrapServer).build();
    }

    public static AeroClient connect(String bootstrapServer, String token) {
        return builder().bootstrapServer(bootstrapServer).token(token).build();
    }

    /**
     * Obtains or establishes an active connection to the primary bootstrap server.
     */
    public AeroConnection getConnection() {
        return getConnection(bootstrapServers.get(0));
    }

    /**
     * Obtains or establishes an active connection to the specified broker host:port.
     */
    public AeroConnection getConnection(String brokerAddress) {
        if (closed) {
            throw new ConnectionException("AeroClient is closed");
        }
        return connections.computeIfAbsent(brokerAddress, addr -> {
            String[] parts = addr.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9091;
            AeroConnection conn = new AeroConnection(host, port, token, useTls, sslContext,
                    connectTimeoutMs, socketTimeoutMs, maxReconnectAttempts, initialBackoffMs);
            conn.connect();
            return conn;
        });
    }

    /**
     * Creates a new AeroProducer builder.
     */
    public ProducerBuilder producerBuilder() {
        return new ProducerBuilder(this);
    }

    /**
     * Creates a new AeroProducer with default configuration.
     */
    public AeroProducer producer() {
        return producerBuilder().build();
    }

    /**
     * Creates a new AeroConsumer builder.
     */
    public ConsumerBuilder consumerBuilder() {
        return new ConsumerBuilder(this);
    }

    /**
     * Creates a new AeroConsumer for topic and partition.
     */
    public AeroConsumer consumer(String topic, int partition) {
        return consumerBuilder().topic(topic).partition(partition).build();
    }

    public List<String> bootstrapServers() {
        return Collections.unmodifiableList(bootstrapServers);
    }

    public String token() {
        return token;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            for (AeroConnection conn : connections.values()) {
                try {
                    conn.close();
                } catch (Exception ignored) {
                }
            }
            connections.clear();
            scheduler.shutdownNow();
            log.debug("AeroClient closed successfully");
        }
    }
}
