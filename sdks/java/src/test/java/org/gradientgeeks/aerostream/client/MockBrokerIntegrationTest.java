package org.gradientgeeks.aerostream.client;

import org.gradientgeeks.aerostream.common.AeroRecord;
import org.gradientgeeks.aerostream.common.AuthenticationException;
import org.gradientgeeks.aerostream.common.OutOfOrderException;
import org.gradientgeeks.aerostream.protocol.ProtocolConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

class MockBrokerIntegrationTest {

    private MockBroker mockBroker;

    @AfterEach
    void tearDown() {
        if (mockBroker != null) {
            mockBroker.close();
        }
    }

    @Test
    @DisplayName("Authentication handshake success and failure")
    void testAuthHandshake() {
        mockBroker = new MockBroker("secret-token");
        mockBroker.start();

        // Invalid token should fail with AuthenticationException
        AeroClient badClient = AeroClient.builder()
                .bootstrapServer(mockBroker.address())
                .token("wrong-token")
                .connectTimeout(Duration.ofSeconds(1))
                .build();

        try {
            AeroProducer badProducer = badClient.producer();
            assertThatThrownBy(() -> badProducer.send("test", 0, "payload".getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(AuthenticationException.class);
        } finally {
            badClient.close();
        }

        // Valid token should succeed
        AeroClient goodClient = AeroClient.builder()
                .bootstrapServer(mockBroker.address())
                .token("secret-token")
                .connectTimeout(Duration.ofSeconds(1))
                .build();

        try {
            AeroProducer goodProducer = goodClient.producer();
            long offset = goodProducer.send("test", 0, "ok-payload".getBytes(StandardCharsets.UTF_8));
            assertThat(offset).isEqualTo(0L);
        } finally {
            goodClient.close();
        }
    }

    @Test
    @DisplayName("End-to-end produce and multi-fetch")
    void testEndToEndProduceAndMultiFetch() {
        mockBroker = new MockBroker(null);
        mockBroker.start();

        try (AeroClient client = AeroClient.connect(mockBroker.address())) {
            AeroProducer producer = client.producer();
            String topic = "orders";
            int partition = 0;

            for (int i = 0; i < 5; i++) {
                long offset = producer.send(topic, partition, ("order-" + i).getBytes(StandardCharsets.UTF_8));
                assertThat(offset).isEqualTo((long) i);
            }

            AeroConsumer consumer = client.consumerBuilder()
                    .topic(topic)
                    .partition(partition)
                    .initialOffset(0)
                    .build();

            List<AeroRecord> records = consumer.fetch(1048576, Duration.ofMillis(200));
            assertThat(records).hasSize(5);

            for (int i = 0; i < 5; i++) {
                assertThat(records.get(i).offset()).isEqualTo((long) i);
                assertThat(records.get(i).payloadAsString()).isEqualTo("order-" + i);
            }

            // Fetch from offset 3
            consumer.seek(3);
            List<AeroRecord> partial = consumer.fetch();
            assertThat(partial).hasSize(2);
            assertThat(partial.get(0).offset()).isEqualTo(3L);
            assertThat(partial.get(1).offset()).isEqualTo(4L);
        }
    }

    @Test
    @DisplayName("Produce and fetch single via Command 2")
    void testProduceAndFetchSingle() {
        mockBroker = new MockBroker(null);
        mockBroker.start();

        try (AeroClient client = AeroClient.connect(mockBroker.address())) {
            AeroConnection conn = client.getConnection();
            String topic = "single-test";
            int partition = 0;

            long off0 = conn.produce(topic, partition, "payload-0".getBytes(StandardCharsets.UTF_8));
            long off1 = conn.produce(topic, partition, "payload-1".getBytes(StandardCharsets.UTF_8));
            assertThat(off0).isEqualTo(0L);
            assertThat(off1).isEqualTo(1L);

            byte[] data0 = conn.fetch(topic, partition, 0L, 65536);
            assertThat(data0).isNotNull();
            assertThat(new String(data0, StandardCharsets.UTF_8)).isEqualTo("payload-0");

            byte[] data1 = conn.fetch(topic, partition, 1L, 65536);
            assertThat(data1).isNotNull();
            assertThat(new String(data1, StandardCharsets.UTF_8)).isEqualTo("payload-1");

            byte[] dataMissing = conn.fetch(topic, partition, 999L, 65536);
            assertThat(dataMissing).isNull();
        }
    }

    @Test
    @DisplayName("Out-of-order sequence error handling")
    void testOutOfOrderSequenceError() {
        mockBroker = new MockBroker(null);
        mockBroker.start();

        try (AeroClient client = AeroClient.connect(mockBroker.address())) {
            AeroProducer producer = client.producer();
            byte[] triggerPayload = "TRIGGER_OUT_OF_ORDER".getBytes(StandardCharsets.UTF_8);

            assertThatThrownBy(() -> producer.send("audit", 0, triggerPayload))
                    .isInstanceOf(OutOfOrderException.class)
                    .hasMessageContaining("out of order sequence");
        }
    }

    @Test
    @DisplayName("Continuous stream consumption")
    void testContinuousStreamConsumption() throws Exception {
        mockBroker = new MockBroker(null);
        mockBroker.start();

        try (AeroClient client = AeroClient.connect(mockBroker.address())) {
            String topic = "telemetry";
            int partition = 0;
            AeroProducer producer = client.producer();
            AeroConsumer consumer = client.consumer(topic, partition);

            // Produce in background after small delay
            CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(50);
                    for (int i = 0; i < 4; i++) {
                        producer.send(topic, partition, ("metric-" + i).getBytes(StandardCharsets.UTF_8));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            List<AeroRecord> collected = consumer.stream()
                    .limit(4)
                    .collect(Collectors.toList());

            assertThat(collected).hasSize(4);
            for (int i = 0; i < 4; i++) {
                assertThat(collected.get(i).offset()).isEqualTo((long) i);
                assertThat(collected.get(i).payloadAsString()).isEqualTo("metric-" + i);
            }
        }
    }

    @Test
    @DisplayName("Concurrent produce and in-flight backpressure")
    void testProducerConcurrencyAndBackpressure() throws Exception {
        mockBroker = new MockBroker(null);
        mockBroker.start();

        try (AeroClient client = AeroClient.connect(mockBroker.address())) {
            AeroProducer producer = client.producerBuilder()
                    .maxInFlight(10)
                    .build();

            int concurrency = 20;
            ExecutorService pool = Executors.newFixedThreadPool(10);
            List<Future<Long>> futures = new ArrayList<>();

            for (int i = 0; i < concurrency; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> producer.send("bench", 0, ("data-" + idx).getBytes(StandardCharsets.UTF_8))));
            }

            Set<Long> offsets = new HashSet<>();
            for (Future<Long> f : futures) {
                offsets.add(f.get(5, TimeUnit.SECONDS));
            }

            pool.shutdown();
            assertThat(offsets).hasSize(concurrency);
        }
    }

    @Test
    @DisplayName("Batch produce sequential offsets")
    void testBatchProduce() {
        mockBroker = new MockBroker(null);
        mockBroker.start();

        try (AeroClient client = AeroClient.connect(mockBroker.address())) {
            AeroProducer producer = client.producer();

            List<byte[]> payloads = List.of(
                    "batch-1".getBytes(StandardCharsets.UTF_8),
                    "batch-2".getBytes(StandardCharsets.UTF_8),
                    "batch-3".getBytes(StandardCharsets.UTF_8)
            );

            long[] offsets = producer.sendBatch("batch-topic", 0, payloads);
            assertThat(offsets).containsExactly(0L, 1L, 2L);
        }
    }

    @Test
    @DisplayName("Automatic reconnection and re-authentication upon broker restart")
    void testAutoReconnect() throws Exception {
        mockBroker = new MockBroker("reconnect-secret");
        mockBroker.start();
        int brokerPort = mockBroker.port();

        AeroClient client = AeroClient.builder()
                .bootstrapServer("127.0.0.1:" + brokerPort)
                .token("reconnect-secret")
                .maxReconnectAttempts(5)
                .initialBackoff(Duration.ofMillis(50))
                .build();

        try {
            AeroProducer producer = client.producer();
            long off1 = producer.send("status", 0, "msg-1".getBytes(StandardCharsets.UTF_8));
            assertThat(off1).isEqualTo(0L);

            // Restart broker on exact same port
            mockBroker.close();
            Thread.sleep(60);

            mockBroker = new MockBroker("reconnect-secret", brokerPort);
            mockBroker.start();

            // Next produce should automatically reconnect and succeed
            long off2 = producer.send("status", 0, "msg-2".getBytes(StandardCharsets.UTF_8));
            assertThat(off2).isEqualTo(0L);
        } finally {
            client.close();
        }
    }

    /**
     * In-process mock broker implementing AeroStream native binary protocol.
     */
    static class MockBroker implements Closeable {
        private final String expectedToken;
        private final int explicitPort;
        private ServerSocket serverSocket;
        private int port;
        private volatile boolean running = true;
        private final Map<String, List<byte[]>> store = new ConcurrentHashMap<>();
        private final List<Socket> clientSockets = new CopyOnWriteArrayList<>();
        private ExecutorService acceptExecutor;

        MockBroker(String expectedToken) {
            this(expectedToken, 0);
        }

        MockBroker(String expectedToken, int port) {
            this.expectedToken = expectedToken;
            this.explicitPort = port;
        }

        synchronized void start() {
            try {
                serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress("127.0.0.1", explicitPort));
                this.port = serverSocket.getLocalPort();
                this.acceptExecutor = Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "mock-broker-worker");
                    t.setDaemon(true);
                    return t;
                });
                acceptExecutor.submit(this::acceptLoop);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        int port() {
            return port;
        }

        String address() {
            return "127.0.0.1:" + port;
        }

        private void acceptLoop() {
            while (running && !serverSocket.isClosed()) {
                try {
                    Socket sock = serverSocket.accept();
                    sock.setTcpNoDelay(true);
                    clientSockets.add(sock);
                    acceptExecutor.submit(() -> handleClient(sock));
                } catch (IOException ignored) {
                    break;
                }
            }
        }

        private void handleClient(Socket socket) {
            boolean authenticated = expectedToken == null || expectedToken.isEmpty();
            try (InputStream in = new BufferedInputStream(socket.getInputStream());
                 OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

                byte[] header = new byte[ProtocolConstants.HEADER_LEN];

                while (running && !socket.isClosed()) {
                    if (!readFully(in, header)) {
                        break;
                    }
                    if (!ProtocolConstants.isValidMagic(header[0], header[1])) {
                        break;
                    }

                    byte cmd = header[2];
                    int bodyLen = ByteBuffer.wrap(header, 3, 4).order(ByteOrder.BIG_ENDIAN).getInt();
                    byte[] body = new byte[bodyLen];
                    if (!readFully(in, body)) {
                        break;
                    }

                    if (cmd == ProtocolConstants.CMD_AUTH) {
                        String tokenStr = new String(body, StandardCharsets.UTF_8);
                        if (expectedToken == null || expectedToken.equals(tokenStr)) {
                            authenticated = true;
                            out.write(new byte[]{ProtocolConstants.MAGIC_0, ProtocolConstants.MAGIC_1, ProtocolConstants.STATUS_OK});
                            out.flush();
                        } else {
                            out.write(new byte[]{ProtocolConstants.MAGIC_0, ProtocolConstants.MAGIC_1, ProtocolConstants.STATUS_AUTH_FAILED});
                            out.flush();
                            break;
                        }
                        continue;
                    }

                    if (!authenticated) {
                        out.write(new byte[]{ProtocolConstants.MAGIC_0, ProtocolConstants.MAGIC_1, ProtocolConstants.STATUS_AUTH_FAILED});
                        out.flush();
                        break;
                    }

                    if (cmd == ProtocolConstants.CMD_PRODUCE) {
                        ByteBuffer bb = ByteBuffer.wrap(body).order(ByteOrder.BIG_ENDIAN);
                        int topicLen = Short.toUnsignedInt(bb.getShort());
                        byte[] topicBytes = new byte[topicLen];
                        bb.get(topicBytes);
                        String topic = new String(topicBytes, StandardCharsets.UTF_8);

                        int partition = bb.getInt();
                        int payloadLen = bb.getInt();
                        byte[] payload = new byte[payloadLen];
                        bb.get(payload);

                        String key = topic + ":" + partition;
                        if (new String(payload, StandardCharsets.UTF_8).equals("TRIGGER_OUT_OF_ORDER")) {
                            out.write(new byte[]{ProtocolConstants.MAGIC_0, ProtocolConstants.MAGIC_1, ProtocolConstants.STATUS_OUT_OF_ORDER});
                            out.flush();
                            continue;
                        }

                        List<byte[]> list = store.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>());
                        long offset = list.size();
                        list.add(payload);

                        ByteBuffer resp = ByteBuffer.allocate(11).order(ByteOrder.BIG_ENDIAN);
                        resp.put(ProtocolConstants.MAGIC_0);
                        resp.put(ProtocolConstants.MAGIC_1);
                        resp.put(ProtocolConstants.STATUS_OK);
                        resp.putLong(offset);
                        out.write(resp.array());
                        out.flush();
                    } else if (cmd == ProtocolConstants.CMD_FETCH) {
                        ByteBuffer bb = ByteBuffer.wrap(body).order(ByteOrder.BIG_ENDIAN);
                        int topicLen = Short.toUnsignedInt(bb.getShort());
                        byte[] topicBytes = new byte[topicLen];
                        bb.get(topicBytes);
                        String topic = new String(topicBytes, StandardCharsets.UTF_8);

                        int partition = bb.getInt();
                        long startOffset = bb.getLong();
                        int maxBytes = bb.getInt();

                        String key = topic + ":" + partition;
                        List<byte[]> list = store.get(key);
                        byte[] found = null;
                        if (list != null && startOffset >= 0 && startOffset < list.size()) {
                            found = list.get((int) startOffset);
                        }

                        if (found == null) {
                            out.write(new byte[]{ProtocolConstants.MAGIC_0, ProtocolConstants.MAGIC_1, ProtocolConstants.STATUS_EMPTY});
                        } else {
                            ByteBuffer resp = ByteBuffer.allocate(7 + found.length).order(ByteOrder.BIG_ENDIAN);
                            resp.put(ProtocolConstants.MAGIC_0);
                            resp.put(ProtocolConstants.MAGIC_1);
                            resp.put(ProtocolConstants.STATUS_DATA);
                            resp.putInt(found.length);
                            resp.put(found);
                            out.write(resp.array());
                        }
                        out.flush();
                    } else if (cmd == ProtocolConstants.CMD_FETCH_MULTI) {
                        ByteBuffer bb = ByteBuffer.wrap(body).order(ByteOrder.BIG_ENDIAN);
                        int topicLen = Short.toUnsignedInt(bb.getShort());
                        byte[] topicBytes = new byte[topicLen];
                        bb.get(topicBytes);
                        String topic = new String(topicBytes, StandardCharsets.UTF_8);

                        int partition = bb.getInt();
                        long startOffset = bb.getLong();
                        int maxBytes = bb.getInt();
                        int maxWait = bb.getInt();

                        String key = topic + ":" + partition;
                        List<byte[]> list = store.get(key);
                        List<Map.Entry<Long, byte[]>> matching = new ArrayList<>();
                        if (list != null) {
                            for (int i = (int) Math.max(0, startOffset); i < list.size(); i++) {
                                matching.add(Map.entry((long) i, list.get(i)));
                            }
                        }

                        if (matching.isEmpty()) {
                            out.write(new byte[]{ProtocolConstants.MAGIC_0, ProtocolConstants.MAGIC_1, ProtocolConstants.STATUS_EMPTY});
                        } else {
                            int totalPayloadLen = matching.stream().mapToInt(e -> e.getValue().length).sum();
                            int totalSize = 3 + 4 + (matching.size() * 12) + totalPayloadLen;

                            ByteBuffer resp = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);
                            resp.put(ProtocolConstants.MAGIC_0);
                            resp.put(ProtocolConstants.MAGIC_1);
                            resp.put(ProtocolConstants.STATUS_DATA);
                            resp.putInt(matching.size());

                            for (Map.Entry<Long, byte[]> e : matching) {
                                resp.putLong(e.getKey());
                                resp.putInt(e.getValue().length);
                            }
                            for (Map.Entry<Long, byte[]> e : matching) {
                                resp.put(e.getValue());
                            }
                            out.write(resp.array());
                        }
                        out.flush();
                    }
                }
            } catch (IOException ignored) {
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
                clientSockets.remove(socket);
            }
        }

        private boolean readFully(InputStream in, byte[] b) throws IOException {
            int total = 0;
            while (total < b.length) {
                int n = in.read(b, total, b.length - total);
                if (n < 0) return false;
                total += n;
            }
            return true;
        }

        @Override
        public synchronized void close() {
            running = false;
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException ignored) {
            }
            for (Socket s : clientSockets) {
                try {
                    s.close();
                } catch (IOException ignored) {
                }
            }
            clientSockets.clear();
            if (acceptExecutor != null) {
                acceptExecutor.shutdownNow();
            }
        }
    }
}
