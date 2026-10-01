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

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * A few native connections per producer (connectionsPerProducer, default 1), each carrying every partition
 * assigned to it (partition % connections). A connection pipelines up to maxInFlightPerConnection Produce
 * requests: a writer thread drains the send queue, groups the drained requests by partition (stable, so order
 * within a partition is kept) and sends them in one socket write; a reader thread matches the in-order acks to
 * the pending futures. Grouping gives the broker runs of same-partition frames, which it appends under one lock
 * in one write. Messages without a key are spread round-robin over the partitions.
 */
@Slf4j
public class AeroStreamBenchmarkProducer implements BenchmarkProducer {
    private final List<PartitionConnection> connections = new ArrayList<>();
    private final int partitionCount;
    private final AtomicLong roundRobin = new AtomicLong();

    public AeroStreamBenchmarkProducer(Config config, String topic, int partitionCount) throws IOException {
        this.partitionCount = partitionCount;
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        try {
            int conns = Math.max(1, Math.min(config.connectionsPerProducer, partitionCount));
            for (int c = 0; c < conns; c++) {
                connections.add(new PartitionConnection(config, topicBytes, c));
            }
        } catch (IOException e) {
            close();
            throw e;
        }
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        int n = partitionCount;
        int p =
                key.map(k -> Math.floorMod(k.hashCode(), n))
                        .orElseGet(() -> (int) (roundRobin.getAndIncrement() % n));
        return connections.get(p % connections.size()).send(p, payload);
    }

    @Override
    public void close() {
        connections.forEach(PartitionConnection::close);
    }

    private static final class Request {
        final int partition;
        final byte[] payload;
        final long timestamp;
        final CompletableFuture<Void> future = new CompletableFuture<>();

        Request(int partition, byte[] payload) {
            this.partition = partition;
            this.payload = payload;
            this.timestamp = System.currentTimeMillis();
        }
    }

    private static final class PartitionConnection {
        private final NativeProtocol.Conn conn;
        private final byte[] topic;
        private final int id;
        private final Semaphore inFlight;
        private final LinkedBlockingQueue<Request> sendQueue = new LinkedBlockingQueue<>();
        private final ConcurrentLinkedQueue<CompletableFuture<Void>> awaitingAck =
                new ConcurrentLinkedQueue<>();
        private static final int MAX_BATCH = 4096;
        private final long lingerNanos;
        private static final java.util.Comparator<Request> BY_PARTITION =
                java.util.Comparator.comparingInt(r -> r.partition);
        private final Thread writer;
        private final Thread reader;
        private volatile boolean closed;

        PartitionConnection(Config config, byte[] topic, int id) throws IOException {
            this.conn = NativeProtocol.connect(config);
            this.topic = topic;
            this.id = id;
            this.lingerNanos = config.lingerMicros * 1000L;
            this.inFlight = new Semaphore(config.maxInFlightPerConnection);
            String name = new String(topic, StandardCharsets.UTF_8) + "-" + id;
            this.writer = new Thread(this::writeLoop, "aero-produce-w-" + name);
            this.reader = new Thread(this::readLoop, "aero-produce-r-" + name);
            writer.setDaemon(true);
            reader.setDaemon(true);
            writer.start();
            reader.start();
        }

        CompletableFuture<Void> send(int partition, byte[] payload) {
            if (closed) {
                return CompletableFuture.failedFuture(new IOException("producer closed"));
            }
            try {
                inFlight.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CompletableFuture.failedFuture(e);
            }
            Request r = new Request(partition, payload);
            sendQueue.add(r);
            return r.future;
        }

        private void writeLoop() {
            List<Request> batch = new ArrayList<>(MAX_BATCH);
            try {
                while (!closed) {
                    batch.clear();
                    batch.add(sendQueue.take());
                    sendQueue.drainTo(batch, MAX_BATCH - 1);
                    // linger like Kafka's linger.ms: give a small batch a moment to fill, so the broker sees runs of
                    // frames per partition (one syscall pair and one ack write per run) instead of one per message
                    long deadline = System.nanoTime() + lingerNanos;
                    while (batch.size() < MAX_BATCH && lingerNanos > 0) {
                        long left = deadline - System.nanoTime();
                        if (left <= 0) {
                            break;
                        }
                        Request r = sendQueue.poll(left, java.util.concurrent.TimeUnit.NANOSECONDS);
                        if (r == null) {
                            break;
                        }
                        batch.add(r);
                        sendQueue.drainTo(batch, MAX_BATCH - batch.size());
                    }
                    batch.sort(BY_PARTITION); // stable
                    for (Request r : batch) {
                        // register before the bytes leave, so the reader always finds the future of an ack
                        awaitingAck.add(r.future);
                        NativeProtocol.writeProduce(conn.out, topic, r.partition, r.timestamp, r.payload);
                    }
                    conn.out.flush();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                if (!closed) {
                    log.error("produce write failed on connection {}", id, e);
                }
            }
            failAll();
        }

        private void readLoop() {
            try {
                while (!closed) {
                    int status = NativeProtocol.readStatus(conn.in);
                    CompletableFuture<Void> f = awaitingAck.poll();
                    inFlight.release();
                    if (status == NativeProtocol.STATUS_OK) {
                        conn.in.readLong(); // assigned offset
                        if (f != null) {
                            f.complete(null);
                        }
                    } else if (f != null) {
                        f.completeExceptionally(new IOException("produce failed, broker status " + status));
                    }
                }
            } catch (IOException e) {
                if (!closed) {
                    log.error("produce read failed on connection {}", id, e);
                }
            }
            failAll();
        }

        private void failAll() {
            IOException e = new IOException("connection closed");
            CompletableFuture<Void> f;
            while ((f = awaitingAck.poll()) != null) {
                f.completeExceptionally(e);
            }
            Request r;
            while ((r = sendQueue.poll()) != null) {
                r.future.completeExceptionally(e);
            }
        }

        void close() {
            closed = true;
            writer.interrupt();
            conn.close();
        }
    }
}
