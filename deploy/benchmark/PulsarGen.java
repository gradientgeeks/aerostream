import org.apache.pulsar.client.api.*;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Pipelined Pulsar producer load generator with the same semantics as kafka-producer-perf-test:
 * unthrottled async sends, per-message send->ack latency, exact wall time for the whole run.
 * Usage: PulsarGen <topic> <sizeBytes> <count> [chunk] [maxPending]
 */
public class PulsarGen {
    public static void main(String[] a) throws Exception {
        String topic = a[0];
        int size = Integer.parseInt(a[1]);
        int count = Integer.parseInt(a[2]);
        boolean chunk = a.length > 3 && a[3].equals("chunk");
        int maxPending = a.length > 4 ? Integer.parseInt(a[4]) : 1000;

        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650")
                .ioThreads(2).operationTimeout(120, TimeUnit.SECONDS).build();
        ProducerBuilder<byte[]> pb = client.newProducer().topic(topic)
                .blockIfQueueFull(true).maxPendingMessages(maxPending).sendTimeout(0, TimeUnit.SECONDS);
        if (chunk) {
            pb.enableChunking(true).enableBatching(false);
        } else {
            pb.enableBatching(true).batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS).batchingMaxMessages(1000);
        }
        Producer<byte[]> producer = pb.create();
        byte[] payload = new byte[size];
        Arrays.fill(payload, (byte) 'x');

        long[] lat = new long[count];
        CountDownLatch done = new CountDownLatch(count);
        int[] errors = {0};
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            final int idx = i;
            final long t0 = System.nanoTime();
            producer.sendAsync(payload).whenComplete((id, ex) -> {
                lat[idx] = System.nanoTime() - t0;
                if (ex != null) synchronized (errors) { errors[0]++; }
                done.countDown();
            });
        }
        done.await();
        double secs = (System.nanoTime() - start) / 1e9;
        Arrays.sort(lat);
        double sum = 0;
        for (long l : lat) sum += l;
        System.out.printf("RESULT {\"records\": %d, \"errors\": %d, \"seconds\": %.4f, \"rec_per_sec\": %.2f, \"mb_per_sec\": %.2f, "
                        + "\"avg_ms\": %.3f, \"p50_ms\": %.3f, \"p95_ms\": %.3f, \"p99_ms\": %.3f, \"max_ms\": %.3f}%n",
                count, errors[0], secs, count / secs, (double) count * size / secs / 1048576.0,
                sum / count / 1e6, lat[(int) (count * 0.50)] / 1e6, lat[Math.min(count - 1, (int) (count * 0.95))] / 1e6,
                lat[Math.min(count - 1, (int) (count * 0.99))] / 1e6, lat[count - 1] / 1e6);
        producer.close();
        client.close();
    }
}
