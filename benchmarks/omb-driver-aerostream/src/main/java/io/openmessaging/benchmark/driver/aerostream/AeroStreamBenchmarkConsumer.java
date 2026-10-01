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

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * One fetch thread and connection per assigned partition, using the multi-entry Fetch (cmd 4): each request
 * returns every available entry up to fetchMaxBytes, and the broker holds it up to fetchMaxWaitMs when there is
 * no data.
 */
@Slf4j
public class AeroStreamBenchmarkConsumer implements BenchmarkConsumer {
    private final List<Thread> threads = new ArrayList<>();
    private final List<NativeProtocol.Conn> conns = new ArrayList<>();
    private volatile boolean closed;

    public AeroStreamBenchmarkConsumer(
            Config config, String topic, List<Integer> partitions, ConsumerCallback callback)
            throws IOException {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        try {
            for (int p : partitions) {
                NativeProtocol.Conn conn = NativeProtocol.connect(config);
                conns.add(conn);
                Thread t =
                        new Thread(
                                () -> fetchLoop(conn, topicBytes, p, config, callback),
                                "aero-fetch-" + topic + "-" + p);
                t.setDaemon(true);
                threads.add(t);
            }
        } catch (IOException e) {
            close();
            throw e;
        }
        threads.forEach(Thread::start);
    }

    private void fetchLoop(
            NativeProtocol.Conn conn, byte[] topic, int partition, Config config, ConsumerCallback callback) {
        long offset = 0;
        long[] offsets = new long[0];
        int[] lens = new int[0];
        try {
            while (!closed) {
                NativeProtocol.writeFetchMulti(
                        conn.out,
                        topic,
                        partition,
                        offset,
                        config.fetchMaxBytes,
                        config.fetchMaxWaitMs,
                        config.fetchLingerMicros);
                conn.out.flush();
                int status = NativeProtocol.readStatus(conn.in);
                if (status == NativeProtocol.STATUS_NO_DATA) {
                    continue; // nothing arrived within fetchMaxWaitMs
                }
                if (status != NativeProtocol.STATUS_DATA) {
                    throw new IOException("fetch failed, broker status " + status);
                }
                int count = conn.in.readInt();
                if (offsets.length < count) {
                    offsets = new long[count];
                    lens = new int[count];
                }
                for (int i = 0; i < count; i++) {
                    offsets[i] = conn.in.readLong();
                    lens[i] = conn.in.readInt();
                }
                for (int i = 0; i < count; i++) {
                    byte[] entry = new byte[lens[i]];
                    conn.in.readFully(entry);
                    offset = offsets[i] + 1;
                    if (entry.length < 8) {
                        continue; // not written by this driver
                    }
                    long publishTimestamp = ByteBuffer.wrap(entry, 0, 8).getLong();
                    callback.messageReceived(ByteBuffer.wrap(entry, 8, entry.length - 8), publishTimestamp);
                }
            }
        } catch (IOException e) {
            if (!closed) {
                log.error("fetch failed on partition {} at offset {}", partition, offset, e);
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        conns.forEach(NativeProtocol.Conn::close);
    }
}
