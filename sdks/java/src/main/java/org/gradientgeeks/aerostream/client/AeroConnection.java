package org.gradientgeeks.aerostream.client;

import org.gradientgeeks.aerostream.common.*;
import org.gradientgeeks.aerostream.protocol.FrameCodec;
import org.gradientgeeks.aerostream.protocol.ProtocolConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe socket transport for AeroStream native binary protocol.
 */
public class AeroConnection implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AeroConnection.class);

    private final String host;
    private final int port;
    private final String token;
    private final boolean useTls;
    private final SSLContext sslContext;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;
    private final int maxReconnectAttempts;
    private final long initialBackoffMs;

    private final ReentrantLock lock = new ReentrantLock();
    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private volatile boolean closed = false;

    public AeroConnection(String host, int port, String token, boolean useTls, SSLContext sslContext,
                          int connectTimeoutMs, int socketTimeoutMs, int maxReconnectAttempts, long initialBackoffMs) {
        this.host = host;
        this.port = port;
        this.token = token;
        this.useTls = useTls;
        this.sslContext = sslContext;
        this.connectTimeoutMs = connectTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
        this.maxReconnectAttempts = maxReconnectAttempts;
        this.initialBackoffMs = initialBackoffMs;
    }

    /**
     * Connects and executes authentication handshake if token is configured.
     */
    public void connect() {
        lock.lock();
        try {
            if (closed) {
                throw new ConnectionException("Connection is closed");
            }
            closeSocket();
            connectInternal();
        } finally {
            lock.unlock();
        }
    }

    private void connectInternal() {
        try {
            Socket s;
            if (useTls) {
                SSLSocketFactory factory = sslContext != null ? sslContext.getSocketFactory() : (SSLSocketFactory) SSLSocketFactory.getDefault();
                s = factory.createSocket();
            } else {
                s = new Socket();
            }
            s.setTcpNoDelay(true);
            s.setKeepAlive(true);
            s.setSoTimeout(socketTimeoutMs);
            s.connect(new InetSocketAddress(host, port), connectTimeoutMs);

            if (useTls && s instanceof SSLSocket sslSocket) {
                sslSocket.startHandshake();
            }

            this.socket = s;
            this.in = new BufferedInputStream(s.getInputStream(), 65536);
            this.out = new BufferedOutputStream(s.getOutputStream(), 65536);

            if (token != null && !token.isEmpty()) {
                authenticate();
            }
            log.debug("Connected to broker at {}:{}", host, port);
        } catch (AeroException e) {
            closeSocket();
            throw e;
        } catch (IOException e) {
            closeSocket();
            throw new ConnectionException("Failed to connect to " + host + ":" + port + ": " + e.getMessage(), e);
        }
    }

    private void authenticate() throws IOException {
        ByteBuffer authReq = FrameCodec.encodeAuthRequest(token);
        writeBuffer(authReq);

        byte[] prefix = new byte[3];
        readFully(prefix);
        byte status = FrameCodec.decodeStatusPrefix(prefix);
        if (status == ProtocolConstants.STATUS_AUTH_FAILED) {
            throw new AuthenticationException("Authentication handshake failed with status: " + status);
        } else if (status != ProtocolConstants.STATUS_OK) {
            throw new AeroException("Unexpected authentication response status: " + status);
        }
        log.debug("Authenticated successfully with {}:{}", host, port);
    }

    /**
     * Checks if socket is open and connected.
     */
    public boolean isAlive() {
        lock.lock();
        try {
            return socket != null && socket.isConnected() && !socket.isClosed() && !closed;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sends Command 1 (PRODUCE) and returns the written offset.
     */
    public long produce(String topic, int partition, byte[] payload) {
        return executeWithReconnect(() -> {
            ByteBuffer req = FrameCodec.encodeProduceRequest(topic, partition, payload);
            writeBuffer(req);

            byte[] prefix = new byte[3];
            readFully(prefix);
            byte status = FrameCodec.decodeStatusPrefix(prefix);

            if (status == ProtocolConstants.STATUS_OK) {
                byte[] offsetBuf = new byte[8];
                readFully(offsetBuf);
                return ByteBuffer.wrap(offsetBuf).order(ByteOrder.BIG_ENDIAN).getLong();
            } else if (status == ProtocolConstants.STATUS_AUTH_FAILED) {
                throw new AuthenticationException("Produce rejected: not authenticated");
            } else if (status == ProtocolConstants.STATUS_OUT_OF_ORDER) {
                throw new OutOfOrderException("Produce rejected: out of order sequence number");
            } else {
                throw new AeroException("Produce rejected with status: " + status);
            }
        });
    }

    /**
     * Sends Command 2 (FETCH) single-range fetch.
     */
    public byte[] fetch(String topic, int partition, long startOffset, int maxBytes) {
        return executeWithReconnect(() -> {
            ByteBuffer req = FrameCodec.encodeFetchRequest(topic, partition, startOffset, maxBytes);
            writeBuffer(req);

            byte[] prefix = new byte[3];
            readFully(prefix);
            byte status = FrameCodec.decodeStatusPrefix(prefix);

            if (status == ProtocolConstants.STATUS_EMPTY) {
                return null;
            } else if (status == ProtocolConstants.STATUS_AUTH_FAILED) {
                throw new AuthenticationException("Fetch rejected: not authenticated");
            } else if (status == ProtocolConstants.STATUS_DATA) {
                byte[] lenBuf = new byte[4];
                readFully(lenBuf);
                int bytesToRead = ByteBuffer.wrap(lenBuf).order(ByteOrder.BIG_ENDIAN).getInt();
                if (bytesToRead < 0) {
                    throw new AeroException("Negative bytes_to_read in fetch response: " + bytesToRead);
                }
                byte[] data = new byte[bytesToRead];
                readFully(data);
                return data;
            } else {
                throw new AeroException("Fetch rejected with status: " + status);
            }
        });
    }

    /**
     * Sends Command 4 (FETCH_MULTI) multi-entry long-polling fetch.
     */
    public List<AeroRecord> fetchMulti(String topic, int partition, long startOffset, int maxBytes, int maxWaitMs) {
        return executeWithReconnect(() -> {
            ByteBuffer req = FrameCodec.encodeMultiFetchRequest(topic, partition, startOffset, maxBytes, maxWaitMs);
            writeBuffer(req);

            byte[] prefix = new byte[3];
            readFully(prefix);
            byte status = FrameCodec.decodeStatusPrefix(prefix);

            if (status == ProtocolConstants.STATUS_EMPTY) {
                return Collections.emptyList();
            } else if (status == ProtocolConstants.STATUS_AUTH_FAILED) {
                throw new AuthenticationException("MultiFetch rejected: not authenticated");
            } else if (status == ProtocolConstants.STATUS_DATA) {
                byte[] countBuf = new byte[4];
                readFully(countBuf);
                int entryCount = ByteBuffer.wrap(countBuf).order(ByteOrder.BIG_ENDIAN).getInt();
                if (entryCount <= 0) {
                    return Collections.emptyList();
                }

                int indexLen = entryCount * 12;
                byte[] indexBytes = new byte[indexLen];
                readFully(indexBytes);

                ByteBuffer indexBuf = ByteBuffer.wrap(indexBytes).order(ByteOrder.BIG_ENDIAN);
                long[] offsets = new long[entryCount];
                int[] lengths = new int[entryCount];
                int totalPayloadLen = 0;

                for (int i = 0; i < entryCount; i++) {
                    offsets[i] = indexBuf.getLong();
                    int len = indexBuf.getInt();
                    if (len < 0) {
                        throw new AeroException("Negative record length in multi-fetch: " + len);
                    }
                    lengths[i] = len;
                    totalPayloadLen += len;
                }

                byte[] payloadBytes = new byte[totalPayloadLen];
                readFully(payloadBytes);

                List<AeroRecord> records = new ArrayList<>(entryCount);
                int currentPos = 0;
                for (int i = 0; i < entryCount; i++) {
                    byte[] recData = new byte[lengths[i]];
                    System.arraycopy(payloadBytes, currentPos, recData, 0, lengths[i]);
                    currentPos += lengths[i];
                    records.add(new AeroRecord(offsets[i], recData, topic, partition));
                }
                return records;
            } else {
                throw new AeroException("MultiFetch rejected with status: " + status);
            }
        });
    }

    private <T> T executeWithReconnect(IoOperation<T> op) {
        lock.lock();
        try {
            if (closed) {
                throw new ConnectionException("Connection is closed");
            }
            if (socket == null || socket.isClosed()) {
                connectInternal();
            }
            try {
                return op.execute();
            } catch (AuthenticationException | OutOfOrderException e) {
                throw e;
            } catch (Exception initialError) {
                log.warn("Connection error on {}:{} ({}), attempting reconnect", host, port, initialError.getMessage());
                closeSocket();
                return reconnectAndRetry(op, initialError);
            }
        } finally {
            lock.unlock();
        }
    }

    private <T> T reconnectAndRetry(IoOperation<T> op, Exception lastError) {
        int attempts = 0;
        long backoff = initialBackoffMs;

        while (attempts < maxReconnectAttempts && !closed) {
            attempts++;
            try {
                long jitter = ThreadLocalRandom.current().nextLong(backoff / 2, backoff + 1);
                Thread.sleep(jitter);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new ConnectionException("Interrupted during reconnect backoff", ie);
            }

            try {
                connectInternal();
                return op.execute();
            } catch (AuthenticationException | OutOfOrderException e) {
                throw e;
            } catch (Exception e) {
                lastError = e;
                closeSocket();
                backoff = Math.min(backoff * 2, 5000);
            }
        }
        throw new ConnectionException("Failed operation after " + attempts + " reconnect attempts: " + lastError.getMessage(), lastError);
    }

    private void writeBuffer(ByteBuffer buf) throws IOException {
        if (out == null) {
            throw new IOException("Output stream is closed");
        }
        out.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        out.flush();
    }

    private void readFully(byte[] b) throws IOException {
        if (in == null) {
            throw new IOException("Input stream is closed");
        }
        int total = 0;
        int n;
        while (total < b.length) {
            n = in.read(b, total, b.length - total);
            if (n < 0) {
                throw new EOFException("Premature EOF reading from socket (got " + total + " of " + b.length + " bytes)");
            }
            total += n;
        }
    }

    private void closeSocket() {
        try {
            if (in != null) in.close();
        } catch (Exception ignored) {
        }
        try {
            if (out != null) out.close();
        } catch (Exception ignored) {
        }
        try {
            if (socket != null) socket.close();
        } catch (Exception ignored) {
        }
        this.in = null;
        this.out = null;
        this.socket = null;
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                closeSocket();
                log.debug("Closed connection to {}:{}", host, port);
            }
        } finally {
            lock.unlock();
        }
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @FunctionalInterface
    private interface IoOperation<T> {
        T execute() throws Exception;
    }
}
