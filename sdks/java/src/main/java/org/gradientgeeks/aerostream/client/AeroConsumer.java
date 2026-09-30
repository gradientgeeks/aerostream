package org.gradientgeeks.aerostream.client;

import org.gradientgeeks.aerostream.common.AeroException;
import org.gradientgeeks.aerostream.common.AeroRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Thread-safe consumer for fetching and streaming records from AeroStream.
 */
public class AeroConsumer implements Iterable<AeroRecord>, Closeable {

    private static final Logger log = LoggerFactory.getLogger(AeroConsumer.class);

    private final AeroClient client;
    private final String topic;
    private final int partition;
    private final AtomicLong currentOffset;
    private final int defaultMaxBytes;
    private final int defaultMaxWaitMs;
    private volatile boolean closed = false;

    public AeroConsumer(ConsumerBuilder builder) {
        this.client = Objects.requireNonNull(builder.client(), "client must not be null");
        this.topic = Objects.requireNonNull(builder.topic(), "topic must not be null");
        this.partition = builder.partition();
        this.currentOffset = new AtomicLong(builder.initialOffset());
        this.defaultMaxBytes = builder.maxBytes();
        this.defaultMaxWaitMs = (int) builder.maxWait().toMillis();
    }

    /**
     * Fetches records using current topic, partition, and tracked offset.
     */
    public List<AeroRecord> fetch() {
        return fetch(defaultMaxBytes, Duration.ofMillis(defaultMaxWaitMs));
    }

    /**
     * Fetches records using current offset with custom byte limit and timeout.
     */
    public synchronized List<AeroRecord> fetch(int maxBytes, Duration maxWait) {
        ensureOpen();
        long startOffset = currentOffset.get();
        List<AeroRecord> records = fetchExplicit(topic, partition, startOffset, maxBytes, maxWait);
        if (!records.isEmpty()) {
            long nextOffset = records.get(records.size() - 1).offset() + 1;
            currentOffset.set(nextOffset);
        }
        return records;
    }

    /**
     * Fetches records for explicit topic, partition, and starting offset.
     */
    public List<AeroRecord> fetch(String topic, int partition, long startOffset, int maxBytes, Duration maxWait) {
        ensureOpen();
        return fetchExplicit(topic, partition, startOffset, maxBytes, maxWait);
    }

    private List<AeroRecord> fetchExplicit(String topic, int partition, long startOffset, int maxBytes, Duration maxWait) {
        AeroConnection conn = client.getConnection();
        int waitMs = (int) Math.max(0, maxWait.toMillis());
        return conn.fetchMulti(topic, partition, startOffset, maxBytes, waitMs);
    }

    /**
     * Updates the current fetch offset.
     */
    public void seek(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }
        currentOffset.set(offset);
    }

    /**
     * Returns the current tracked offset for the next fetch.
     */
    public long position() {
        return currentOffset.get();
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    /**
     * Returns an iterator that continuously long-polls for records until closed.
     */
    @Override
    public Iterator<AeroRecord> iterator() {
        return new RecordIterator();
    }

    /**
     * Creates a sequential Stream of records from this consumer.
     */
    public Stream<AeroRecord> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    private void ensureOpen() {
        if (closed) {
            throw new AeroException("AeroConsumer is closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            log.debug("Closed consumer for topic={} partition={}", topic, partition);
        }
    }

    private class RecordIterator implements Iterator<AeroRecord> {
        private final ArrayDeque<AeroRecord> buffer = new ArrayDeque<>();

        @Override
        public boolean hasNext() {
            if (closed) {
                return !buffer.isEmpty();
            }
            while (buffer.isEmpty() && !closed) {
                List<AeroRecord> fetched = fetch();
                if (!fetched.isEmpty()) {
                    buffer.addAll(fetched);
                    break;
                }
                // If nothing was returned and consumer closed, break
                if (closed) {
                    break;
                }
            }
            return !buffer.isEmpty();
        }

        @Override
        public AeroRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more records available in consumer");
            }
            return buffer.pollFirst();
        }
    }
}
