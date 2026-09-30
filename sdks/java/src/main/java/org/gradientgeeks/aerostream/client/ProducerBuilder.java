package org.gradientgeeks.aerostream.client;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Fluent builder for {@link AeroProducer}.
 */
public class ProducerBuilder {

    private final AeroClient client;
    private int maxInFlight = 256;
    private int maxRetries = 3;
    private long retryBackoffMs = 50;
    private ExecutorService executor;

    public ProducerBuilder(AeroClient client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    public ProducerBuilder maxInFlight(int maxInFlight) {
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("maxInFlight must be positive");
        }
        this.maxInFlight = maxInFlight;
        return this;
    }

    public ProducerBuilder maxRetries(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
        this.maxRetries = maxRetries;
        return this;
    }

    public ProducerBuilder retryBackoff(Duration backoff) {
        this.retryBackoffMs = backoff.toMillis();
        return this;
    }

    public ProducerBuilder executor(ExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public AeroClient client() {
        return client;
    }

    public int maxInFlight() {
        return maxInFlight;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public long retryBackoffMs() {
        return retryBackoffMs;
    }

    public ExecutorService executor() {
        return executor;
    }

    public AeroProducer build() {
        return new AeroProducer(this);
    }
}
