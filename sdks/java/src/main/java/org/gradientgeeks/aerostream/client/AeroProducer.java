package org.gradientgeeks.aerostream.client;

import org.gradientgeeks.aerostream.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * Thread-safe message publisher with backpressure semaphore and retry policy.
 */
public class AeroProducer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AeroProducer.class);

    private final AeroClient client;
    private final int maxInFlight;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final Semaphore inFlightSemaphore;
    private final ExecutorService executor;
    private final boolean ownsExecutor;
    private volatile boolean closed = false;

    public AeroProducer(ProducerBuilder builder) {
        this.client = Objects.requireNonNull(builder.client(), "client must not be null");
        this.maxInFlight = builder.maxInFlight();
        this.maxRetries = builder.maxRetries();
        this.retryBackoffMs = builder.retryBackoffMs();
        this.inFlightSemaphore = new Semaphore(maxInFlight);

        if (builder.executor() != null) {
            this.executor = builder.executor();
            this.ownsExecutor = false;
        } else {
            this.executor = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors()), r -> {
                Thread t = new Thread(r, "aerostream-producer-pool");
                t.setDaemon(true);
                return t;
            });
            this.ownsExecutor = true;
        }
    }

    /**
     * Publishes a message synchronously and returns the committed offset.
     */
    public long send(String topic, int partition, byte[] payload) {
        ensureOpen();
        acquirePermit();
        try {
            return sendWithRetries(topic, partition, payload);
        } finally {
            inFlightSemaphore.release();
        }
    }

    /**
     * Publishes a message asynchronously and returns a CompletableFuture with the committed offset.
     */
    public CompletableFuture<Long> sendAsync(String topic, int partition, byte[] payload) {
        ensureOpen();
        acquirePermit();

        CompletableFuture<Long> future = CompletableFuture.supplyAsync(() -> {
            try {
                return sendWithRetries(topic, partition, payload);
            } catch (Exception e) {
                if (e instanceof CompletionException) {
                    throw (CompletionException) e;
                }
                throw new CompletionException(e);
            }
        }, executor);

        future.whenComplete((res, ex) -> inFlightSemaphore.release());
        return future;
    }

    /**
     * Publishes a batch of messages sequentially and returns an array of assigned offsets.
     */
    public long[] sendBatch(String topic, int partition, List<byte[]> payloads) {
        ensureOpen();
        if (payloads == null || payloads.isEmpty()) {
            return new long[0];
        }
        long[] offsets = new long[payloads.size()];
        for (int i = 0; i < payloads.size(); i++) {
            offsets[i] = send(topic, partition, payloads.get(i));
        }
        return offsets;
    }

    private long sendWithRetries(String topic, int partition, byte[] payload) {
        int attempts = 0;
        AeroException lastError = null;

        while (attempts <= maxRetries && !closed) {
            attempts++;
            try {
                AeroConnection conn = client.getConnection();
                return conn.produce(topic, partition, payload);
            } catch (AuthenticationException | OutOfOrderException e) {
                // Non-retryable protocol rejection
                throw e;
            } catch (AeroException e) {
                lastError = e;
                if (attempts > maxRetries) {
                    break;
                }
                log.warn("Produce attempt {}/{} failed for topic={} partition={}: {}",
                        attempts, maxRetries + 1, topic, partition, e.getMessage());
                try {
                    Thread.sleep(retryBackoffMs * attempts);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new AeroException("Interrupted during produce retry backoff", ie);
                }
            }
        }
        throw lastError != null ? lastError : new AeroException("Produce failed after " + attempts + " attempts");
    }

    private void acquirePermit() {
        try {
            inFlightSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AeroException("Interrupted waiting for in-flight backpressure permit", e);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new AeroException("AeroProducer is closed");
        }
    }

    public int availablePermits() {
        return inFlightSemaphore.availablePermits();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (ownsExecutor) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
