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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * AeroStream native data-plane framing (rust-broker/src/net/server.rs).
 *
 * <pre>
 * request : [0xAE 0x01][cmd(1)][body_len(4)][body]
 *   cmd 0 AUTH    body = token                                 -> [AE 01][status]
 *   cmd 1 PRODUCE body = [topic_len(2)][topic][partition(4)][len(4)][payload]
 *                                                             -> [AE 01][status=0][offset(8)] | [AE 01][status]
 *   cmd 2 FETCH   body = [topic_len(2)][topic][partition(4)][offset(8)][max_bytes(4)]
 *                                                             -> [AE 01][2][len(4)][entry] | [AE 01][1] (no data)
 *   cmd 4 FETCH_MULTI body = [topic_len(2)][topic][partition(4)][offset(8)][max_bytes(4)][max_wait_ms(4)][linger_us(4), optional]
 *                    -> [AE 01][2][count(4)] count x [offset(8)][len(4)] [entries...] | [AE 01][1] (no data by max_wait)
 * </pre>
 *
 * All integers are big-endian. The broker answers requests on a connection in order.
 */
final class NativeProtocol {
    static final byte CMD_AUTH = 0;
    static final byte CMD_PRODUCE = 1;
    static final byte CMD_FETCH = 2;
    static final byte CMD_FETCH_MULTI = 4;
    static final int STATUS_OK = 0;
    static final int STATUS_NO_DATA = 1;
    static final int STATUS_DATA = 2;

    private NativeProtocol() {}

    static final class Conn implements AutoCloseable {
        final Socket socket;
        final DataInputStream in;
        final DataOutputStream out;

        Conn(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 256 * 1024));
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 256 * 1024));
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing
            }
        }
    }

    static Conn connect(Config config) throws IOException {
        String[] hp = config.brokerAddress.split(":");
        Socket s = new Socket();
        s.setTcpNoDelay(true);
        s.setSendBufferSize(4 * 1024 * 1024);
        s.setReceiveBufferSize(4 * 1024 * 1024);
        s.connect(new InetSocketAddress(hp[0], Integer.parseInt(hp[1])), 10_000);
        Conn c = new Conn(s);
        if (config.authToken != null && !config.authToken.isEmpty()) {
            byte[] token = config.authToken.getBytes(StandardCharsets.UTF_8);
            writeHeader(c.out, CMD_AUTH, token.length);
            c.out.write(token);
            c.out.flush();
            int status = readStatus(c.in);
            if (status != STATUS_OK) {
                c.close();
                throw new IOException("AeroStream auth failed, status " + status);
            }
        }
        return c;
    }

    static void writeHeader(DataOutputStream out, byte cmd, int bodyLen) throws IOException {
        out.writeByte(0xAE);
        out.writeByte(0x01);
        out.writeByte(cmd);
        out.writeInt(bodyLen);
    }

    /** Produce frame; the payload is prefixed with the 8-byte publish timestamp (ms) for end-to-end latency. */
    static void writeProduce(
            DataOutputStream out, byte[] topic, int partition, long publishTimestamp, byte[] payload)
            throws IOException {
        int len = 8 + payload.length;
        writeHeader(out, CMD_PRODUCE, 2 + topic.length + 4 + 4 + len);
        out.writeShort(topic.length);
        out.write(topic);
        out.writeInt(partition);
        out.writeInt(len);
        out.writeLong(publishTimestamp);
        out.write(payload);
    }

    static void writeFetch(DataOutputStream out, byte[] topic, int partition, long offset, int maxBytes)
            throws IOException {
        writeHeader(out, CMD_FETCH, 2 + topic.length + 4 + 8 + 4);
        out.writeShort(topic.length);
        out.write(topic);
        out.writeInt(partition);
        out.writeLong(offset);
        out.writeInt(maxBytes);
    }

    static void writeFetchMulti(
            DataOutputStream out,
            byte[] topic,
            int partition,
            long offset,
            int maxBytes,
            int maxWaitMs,
            int lingerMicros)
            throws IOException {
        writeHeader(out, CMD_FETCH_MULTI, 2 + topic.length + 4 + 8 + 4 + 4 + 4);
        out.writeShort(topic.length);
        out.write(topic);
        out.writeInt(partition);
        out.writeLong(offset);
        out.writeInt(maxBytes);
        out.writeInt(maxWaitMs);
        out.writeInt(lingerMicros);
    }

    /** Reads [AE 01][status] and returns the status. */
    static int readStatus(DataInputStream in) throws IOException {
        int m0 = in.readUnsignedByte();
        int m1 = in.readUnsignedByte();
        if (m0 != 0xAE || m1 != 0x01) {
            throw new IOException(String.format("bad response magic %02x %02x", m0, m1));
        }
        return in.readUnsignedByte();
    }
}
