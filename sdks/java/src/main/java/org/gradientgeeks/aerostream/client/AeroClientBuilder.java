package org.gradientgeeks.aerostream.client;

import javax.net.ssl.SSLContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Fluent builder for {@link AeroClient}.
 */
public class AeroClientBuilder {

    private final List<String> bootstrapServers = new ArrayList<>();
    private String token;
    private boolean useTls = false;
    private SSLContext sslContext;
    private int connectTimeoutMs = 5000;
    private int socketTimeoutMs = 30000;
    private int maxReconnectAttempts = 5;
    private long initialBackoffMs = 50;

    public AeroClientBuilder bootstrapServer(String server) {
        if (server != null && !server.trim().isEmpty()) {
            this.bootstrapServers.add(server.trim());
        }
        return this;
    }

    public AeroClientBuilder bootstrapServers(String... servers) {
        if (servers != null) {
            Arrays.stream(servers).forEach(this::bootstrapServer);
        }
        return this;
    }

    public AeroClientBuilder bootstrapServers(List<String> servers) {
        if (servers != null) {
            servers.forEach(this::bootstrapServer);
        }
        return this;
    }

    public AeroClientBuilder token(String token) {
        this.token = token;
        return this;
    }

    public AeroClientBuilder tls(boolean useTls) {
        this.useTls = useTls;
        return this;
    }

    public AeroClientBuilder sslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
        this.useTls = sslContext != null;
        return this;
    }

    public AeroClientBuilder connectTimeout(Duration timeout) {
        this.connectTimeoutMs = (int) timeout.toMillis();
        return this;
    }

    public AeroClientBuilder socketTimeout(Duration timeout) {
        this.socketTimeoutMs = (int) timeout.toMillis();
        return this;
    }

    public AeroClientBuilder maxReconnectAttempts(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
        return this;
    }

    public AeroClientBuilder initialBackoff(Duration backoff) {
        this.initialBackoffMs = backoff.toMillis();
        return this;
    }

    public List<String> bootstrapServers() {
        return bootstrapServers;
    }

    public String token() {
        return token;
    }

    public boolean useTls() {
        return useTls;
    }

    public SSLContext sslContext() {
        return sslContext;
    }

    public int connectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int socketTimeoutMs() {
        return socketTimeoutMs;
    }

    public int maxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    public long initialBackoffMs() {
        return initialBackoffMs;
    }

    public AeroClient build() {
        return new AeroClient(this);
    }
}
