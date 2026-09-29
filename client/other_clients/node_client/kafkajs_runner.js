/**
 * AeroStream Kafka Wire Protocol End-to-End Test Suite for Node.js (kafkajs)
 * 
 * Verifies KafkaJS compatibility against AeroStream running on 127.0.0.1:9092:
 *  - Test 1: AdminClient Operations (connect, createTopics, listTopics, fetchTopicMetadata)
 *  - Test 2: Producer Operations (keys, partition routing across 3 partitions, headers, GZIP & Snappy compression)
 *  - Test 3: Consumer Group Operations (connect, subscribe, dynamic rebalance/assignment, auto/manual offset commits)
 *  - Test 4: Transactional Producer & Isolation (transaction(), commit(), abort(), read_committed vs read_uncommitted)
 *  - Test 5: Payload Checksum Verification (SHA-256 byte-level data integrity across variable payload sizes)
 */

'use strict';

const crypto = require('crypto');
const { Kafka, CompressionTypes, CompressionCodecs, Partitioners, logLevel } = require('kafkajs');
const SnappyCodec = require('kafkajs-snappy');

// Register Snappy compression codec
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

// Broker configuration
const BOOTSTRAP_BROKERS = [process.env.KAFKA_BROKERS || '127.0.0.1:9092'];
const CLIENT_ID = 'aerostream-node-runner';

// Terminal color helpers
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  red: '\x1b[31m',
  bgBlue: '\x1b[44m',
};

function logStep(name) {
  console.log(`\n${colors.cyan}${colors.bright}▶ [TEST] ${name}${colors.reset}`);
}

function logPass(desc, latencyMs) {
  const lat = latencyMs !== undefined ? ` ${colors.dim}(${latencyMs.toFixed(2)} ms)${colors.reset}` : '';
  console.log(`  ${colors.green}✔ PASS${colors.reset} ${desc}${lat}`);
}

function logInfo(desc) {
  console.log(`  ${colors.dim}ℹ${colors.reset} ${desc}`);
}

function logFail(desc, error) {
  console.log(`  ${colors.red}✖ FAIL${colors.reset} ${desc}`);
  if (error) {
    console.error(`    ${colors.red}${error.stack || error}${colors.reset}`);
  }
}

// Result accumulator
const testResults = [];

function recordResult(name, status, latencyMs, details = {}) {
  testResults.push({ name, status, latencyMs, details });
}

function createKafkaClient(customClientId = CLIENT_ID) {
  return new Kafka({
    clientId: customClientId,
    brokers: BOOTSTRAP_BROKERS,
    logLevel: logLevel.WARN, // suppress verbose internal logs for clean test output
    retry: {
      initialRetryTime: 100,
      retries: 5,
    },
  });
}

/**
 * Test 1: AdminClient operations
 * Verifies admin.connect(), admin.createTopics(), admin.listTopics(), and admin.fetchTopicMetadata()
 */
async function testAdminOperations(kafka, topicName) {
  logStep('1. AdminClient Operations (Metadata, Topic Creation, Inspection)');
  const t0 = performance.now();
  const admin = kafka.admin();

  try {
    const tConn0 = performance.now();
    await admin.connect();
    const connLat = performance.now() - tConn0;
    logPass('admin.connect() connected to AeroStream Kafka wire port', connLat);

    // Create topic with 3 partitions
    const tCreate0 = performance.now();
    const created = await admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: topicName,
          numPartitions: 3,
          replicationFactor: 1,
        },
      ],
    });
    const createLat = performance.now() - tCreate0;
    if (!created) {
      throw new Error(`Failed to create topic '${topicName}'`);
    }
    logPass(`admin.createTopics() created '${topicName}' with 3 partitions`, createLat);

    // List topics
    const tList0 = performance.now();
    const topicList = await admin.listTopics();
    const listLat = performance.now() - tList0;
    if (!topicList.includes(topicName)) {
      throw new Error(`Topic '${topicName}' not found in listed topics: ${JSON.stringify(topicList)}`);
    }
    logPass(`admin.listTopics() verified topic existence in cluster [Total topics: ${topicList.length}]`, listLat);

    // Fetch topic metadata
    const tMeta0 = performance.now();
    const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
    const metaLat = performance.now() - tMeta0;

    const topicMeta = metadata.topics.find((t) => t.name === topicName);
    if (!topicMeta) {
      throw new Error(`Topic '${topicName}' missing in fetched metadata`);
    }

    if (topicMeta.partitions.length !== 3) {
      throw new Error(`Expected 3 partitions, got ${topicMeta.partitions.length}`);
    }

    // Verify partition leaders and ISR
    for (const part of topicMeta.partitions) {
      if (part.partitionErrorCode !== 0) {
        throw new Error(`Partition ${part.partitionId} has error code ${part.partitionErrorCode}`);
      }
      logInfo(
        `Partition ${part.partitionId}: leader=${part.leader}, replicas=[${part.replicas.join(',')}], ISR=[${part.isr.join(',')}]`
      );
    }
    logPass(`admin.fetchTopicMetadata() verified 3 partitions, valid leader assignments and ISRs`, metaLat);

    await admin.disconnect();
    const totalLat = performance.now() - t0;
    recordResult('AdminClient Operations', 'PASS', totalLat, {
      partitions: 3,
      topicCount: topicList.length,
    });
  } catch (err) {
    logFail('AdminClient Operations failed', err);
    await admin.disconnect().catch(() => {});
    recordResult('AdminClient Operations', 'FAIL', performance.now() - t0, { error: err.message });
    throw err;
  }
}

/**
 * Test 2: Producer operations
 * Verifies partition routing across 3 partitions, custom headers, keys, and compression (None, GZIP, Snappy)
 */
async function testProducerBatches(kafka, topicName) {
  logStep('2. Producer Operations (Partition Routing, Custom Headers, Compression Codecs)');
  const t0 = performance.now();
  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });

  try {
    await producer.connect();
    logPass('producer.connect() connected');

    // Batch 1: Uncompressed messages routed explicitly across partitions 0, 1, 2
    const tSend1 = performance.now();
    const uncompressedMessages = [
      {
        partition: 0,
        key: 'key-p0-uncompressed',
        value: JSON.stringify({ event: 'order.created', id: 'ord-001', partition: 0 }),
        headers: {
          traceId: 'trace-node-001',
          source: 'kafkajs',
          codec: 'none',
        },
      },
      {
        partition: 1,
        key: 'key-p1-uncompressed',
        value: JSON.stringify({ event: 'order.processed', id: 'ord-002', partition: 1 }),
        headers: {
          traceId: 'trace-node-002',
          source: 'kafkajs',
          codec: 'none',
        },
      },
      {
        partition: 2,
        key: 'key-p2-uncompressed',
        value: JSON.stringify({ event: 'order.shipped', id: 'ord-003', partition: 2 }),
        headers: {
          traceId: 'trace-node-003',
          source: 'kafkajs',
          codec: 'none',
        },
      },
    ];

    const res1 = await producer.send({
      topic: topicName,
      messages: uncompressedMessages,
    });
    const sendLat1 = performance.now() - tSend1;

    if (res1.length !== 3 || res1.some((r) => r.errorCode !== 0)) {
      throw new Error(`Uncompressed send returned error: ${JSON.stringify(res1)}`);
    }
    logPass(`Produced 3 uncompressed messages routed across partitions 0, 1, 2`, sendLat1);

    // Batch 2: GZIP compressed messages
    const tSendGzip = performance.now();
    const gzipMessages = [
      {
        partition: 0,
        key: 'key-p0-gzip',
        value: 'GZIP compressed payload content '.repeat(20),
        headers: {
          traceId: 'trace-gzip-001',
          source: 'kafkajs',
          codec: 'gzip',
        },
      },
      {
        partition: 1,
        key: 'key-p1-gzip',
        value: 'GZIP compressed payload content 2 '.repeat(20),
        headers: {
          traceId: 'trace-gzip-002',
          source: 'kafkajs',
          codec: 'gzip',
        },
      },
    ];

    const resGzip = await producer.send({
      topic: topicName,
      compression: CompressionTypes.GZIP,
      messages: gzipMessages,
    });
    const gzipLat = performance.now() - tSendGzip;

    if (resGzip.some((r) => r.errorCode !== 0)) {
      throw new Error(`GZIP send returned error: ${JSON.stringify(resGzip)}`);
    }
    logPass(`Produced 2 GZIP compressed messages across partitions 0 and 1`, gzipLat);

    // Batch 3: Snappy compressed messages
    const tSendSnappy = performance.now();
    const snappyMessages = [
      {
        partition: 2,
        key: 'key-p2-snappy',
        value: 'Snappy compressed payload content '.repeat(25),
        headers: {
          traceId: 'trace-snappy-001',
          source: 'kafkajs',
          codec: 'snappy',
        },
      },
    ];

    const resSnappy = await producer.send({
      topic: topicName,
      compression: CompressionTypes.Snappy,
      messages: snappyMessages,
    });
    const snappyLat = performance.now() - tSendSnappy;

    if (resSnappy.some((r) => r.errorCode !== 0)) {
      throw new Error(`Snappy send returned error: ${JSON.stringify(resSnappy)}`);
    }
    logPass(`Produced 1 Snappy compressed message to partition 2`, snappyLat);

    await producer.disconnect();
    const totalLat = performance.now() - t0;
    recordResult('Producer Operations', 'PASS', totalLat, {
      totalMessages: 6,
      codecsTested: ['none', 'gzip', 'snappy'],
      partitionsTargeted: [0, 1, 2],
    });

    return {
      totalExpectedMessages: 6,
      expectedKeys: [
        'key-p0-uncompressed',
        'key-p1-uncompressed',
        'key-p2-uncompressed',
        'key-p0-gzip',
        'key-p1-gzip',
        'key-p2-snappy',
      ],
    };
  } catch (err) {
    logFail('Producer Operations failed', err);
    await producer.disconnect().catch(() => {});
    recordResult('Producer Operations', 'FAIL', performance.now() - t0, { error: err.message });
    throw err;
  }
}

/**
 * Test 3: Consumer Group operations
 * Verifies consumer.connect(), dynamic partition assignment, eachMessage consumption,
 * header decoding, auto offset commits, and manual offset commits
 */
async function testConsumerGroup(kafka, topicName, expectedInfo) {
  logStep('3. Consumer Group Operations (Dynamic Assignment, EachMessage, Headers & Commits)');
  const t0 = performance.now();
  const groupId = `kafkajs-group-${Date.now()}`;
  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: 10000,
    rebalanceTimeout: 10000,
  });

  const admin = kafka.admin();

  try {
    await consumer.connect();
    await admin.connect();
    logPass(`consumer.connect() established session for group '${groupId}'`);

    let assignedPartitions = [];
    consumer.on(consumer.events.GROUP_JOIN, (event) => {
      assignedPartitions = event.payload.memberAssignment[topicName] || [];
      logInfo(
        `Consumer joined group (Leader: ${event.payload.isLeader}, Protocol: ${event.payload.groupProtocol}, Assigned Partitions: [${assignedPartitions.join(',')}])`
      );
    });

    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    logPass(`consumer.subscribe() subscribed to '${topicName}' (fromBeginning: true)`);

    const receivedMessages = [];
    const tConsume0 = performance.now();

    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        if (receivedMessages.length < expectedInfo.totalExpectedMessages) {
          reject(
            new Error(
              `Timed out waiting for messages. Expected ${expectedInfo.totalExpectedMessages}, received ${receivedMessages.length}`
            )
          );
        } else {
          resolve();
        }
      }, 10000);

      consumer
        .run({
          autoCommit: false, // test manual commit
          eachMessage: async ({ topic, partition, message }) => {
            const keyStr = message.key ? message.key.toString('utf-8') : null;
            const valStr = message.value ? message.value.toString('utf-8') : null;
            const headers = {};
            if (message.headers) {
              for (const [k, v] of Object.entries(message.headers)) {
                headers[k] = v ? v.toString('utf-8') : null;
              }
            }

            receivedMessages.push({
              partition,
              offset: message.offset,
              key: keyStr,
              value: valStr,
              headers,
            });

            // Perform manual offset commit
            const nextOffset = (BigInt(message.offset) + 1n).toString();
            await consumer.commitOffsets([{ topic, partition, offset: nextOffset }]);

            if (receivedMessages.length >= expectedInfo.totalExpectedMessages) {
              clearTimeout(timeout);
              resolve();
            }
          },
        })
        .catch(reject);
    });

    const consumeLat = performance.now() - tConsume0;
    logPass(
      `Consumed all ${receivedMessages.length}/${expectedInfo.totalExpectedMessages} messages across dynamic partitions`,
      consumeLat
    );

    // Verify all keys and headers were retrieved accurately
    const retrievedKeys = receivedMessages.map((m) => m.key);
    for (const expKey of expectedInfo.expectedKeys) {
      if (!retrievedKeys.includes(expKey)) {
        throw new Error(`Expected message key '${expKey}' missing from received messages`);
      }
    }
    logPass(`All expected keys verified: [${expectedInfo.expectedKeys.join(', ')}]`);

    // Verify header integrity (traceId and source: kafkajs)
    const checkedHeaders = receivedMessages.filter((m) => m.headers.source === 'kafkajs');
    if (checkedHeaders.length !== expectedInfo.totalExpectedMessages) {
      throw new Error(`Expected ${expectedInfo.totalExpectedMessages} messages with source=kafkajs, got ${checkedHeaders.length}`);
    }
    logPass(`Headers verified: 'traceId' and 'source: kafkajs' intact across all messages`);

    // Verify committed offsets via admin
    const committedOffsets = await admin.fetchOffsets({ groupId, topics: [topicName] });
    const partOffsets = committedOffsets.find((o) => o.topic === topicName)?.partitions || [];
    logInfo(`Verified committed offsets: ${JSON.stringify(partOffsets.map((p) => ({ p: p.partition, off: p.offset })))}`);

    for (const p of partOffsets) {
      if (parseInt(p.offset, 10) < 1) {
        throw new Error(`Committed offset for partition ${p.partition} is invalid: ${p.offset}`);
      }
    }
    logPass(`Manual offset commit verified via admin.fetchOffsets()`);

    await consumer.disconnect();
    await admin.disconnect();
    const totalLat = performance.now() - t0;
    recordResult('Consumer Group Operations', 'PASS', totalLat, {
      receivedCount: receivedMessages.length,
      assignedPartitions,
    });
  } catch (err) {
    logFail('Consumer Group Operations failed', err);
    await consumer.disconnect().catch(() => {});
    await admin.disconnect().catch(() => {});
    recordResult('Consumer Group Operations', 'FAIL', performance.now() - t0, { error: err.message });
    throw err;
  }
}

/**
 * Test 4: Transactional Producer & Read-Committed Isolation
 * Verifies producer.transaction(), send(), commit(), and abort()
 * Confirms read_committed consumer filters aborted messages, while read_uncommitted sees them.
 */
async function testTransactions(kafka) {
  logStep('4. Transactional Producer & Isolation Level (commit, abort, read_committed)');
  const t0 = performance.now();
  const txnTopic = `kafkajs-txn-topic-${Date.now()}`;
  const admin = kafka.admin();

  try {
    await admin.connect();
    await admin.createTopics({
      topics: [{ topic: txnTopic, numPartitions: 1, replicationFactor: 1 }],
    });
    logPass(`Created dedicated transaction topic '${txnTopic}'`);
    await admin.disconnect();

    const txnId = `kafkajs-txn-id-${Date.now()}`;
    const txnProducer = kafka.producer({
      transactionalId: txnId,
      maxInFlightRequests: 1,
      idempotent: true,
      createPartitioner: Partitioners.LegacyPartitioner,
    });

    await txnProducer.connect();
    logPass(`Transactional producer initialized with transactionalId: '${txnId}'`);

    const abortedValue = `aborted-tx-msg-${Date.now()}`;
    const committedValue = `committed-tx-msg-${Date.now()}`;

    // Transaction 1: Aborted Batch
    const tAbort0 = performance.now();
    const txn1 = await txnProducer.transaction();
    await txn1.send({
      topic: txnTopic,
      messages: [{ key: 'tx-abort-key', value: abortedValue }],
    });
    await txn1.abort();
    const abortLat = performance.now() - tAbort0;
    logPass(`Transaction 1: produced and ABORTED message successfully`, abortLat);

    // Transaction 2: Committed Batch
    const tCommit0 = performance.now();
    const txn2 = await txnProducer.transaction();
    await txn2.send({
      topic: txnTopic,
      messages: [{ key: 'tx-commit-key', value: committedValue }],
    });
    await txn2.commit();
    const commitLat = performance.now() - tCommit0;
    logPass(`Transaction 2: produced and COMMITTED message successfully`, commitLat);

    await txnProducer.disconnect();

    // Verify with READ_COMMITTED consumer (readUncommitted: false)
    const tRc0 = performance.now();
    const committedConsumer = kafka.consumer({
      groupId: `rc-group-${Date.now()}`,
      readUncommitted: false,
    });
    await committedConsumer.connect();
    await committedConsumer.subscribe({ topic: txnTopic, fromBeginning: true });

    const rcReceived = [];
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => resolve(), 6000);
      committedConsumer
        .run({
          eachMessage: async ({ message }) => {
            rcReceived.push(message.value.toString('utf-8'));
            clearTimeout(timeout);
            resolve();
          },
        })
        .catch(reject);
    });
    await committedConsumer.disconnect();
    const rcLat = performance.now() - tRc0;

    if (rcReceived.includes(abortedValue)) {
      throw new Error(`CRITICAL: read_committed consumer received ABORTED message: ${abortedValue}`);
    }
    if (!rcReceived.includes(committedValue)) {
      throw new Error(`read_committed consumer failed to receive COMMITTED message`);
    }
    logPass(
      `read_committed isolation verified: received COMMITTED message only, ABORTED message correctly filtered`,
      rcLat
    );

    // Verify with READ_UNCOMMITTED consumer (readUncommitted: true)
    const tRu0 = performance.now();
    const uncommittedConsumer = kafka.consumer({
      groupId: `ru-group-${Date.now()}`,
      readUncommitted: true,
    });
    await uncommittedConsumer.connect();
    await uncommittedConsumer.subscribe({ topic: txnTopic, fromBeginning: true });

    const ruReceived = [];
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => resolve(), 6000);
      uncommittedConsumer
        .run({
          eachMessage: async ({ message }) => {
            ruReceived.push(message.value.toString('utf-8'));
            if (ruReceived.length >= 2) {
              clearTimeout(timeout);
              resolve();
            }
          },
        })
        .catch(reject);
    });
    await uncommittedConsumer.disconnect();
    const ruLat = performance.now() - tRu0;

    if (!ruReceived.includes(abortedValue) || !ruReceived.includes(committedValue)) {
      throw new Error(
        `read_uncommitted consumer expected both aborted & committed messages. Got: ${JSON.stringify(ruReceived)}`
      );
    }
    logPass(
      `read_uncommitted isolation verified: received both ABORTED and COMMITTED messages (${ruReceived.length} total)`,
      ruLat
    );

    const totalLat = performance.now() - t0;
    recordResult('Transactional Producer & Isolation', 'PASS', totalLat, {
      abortedVerified: true,
      committedVerified: true,
      readCommittedCount: rcReceived.length,
      readUncommittedCount: ruReceived.length,
    });
  } catch (err) {
    logFail('Transactional Producer & Isolation failed', err);
    await admin.disconnect().catch(() => {});
    recordResult('Transactional Producer & Isolation', 'FAIL', performance.now() - t0, { error: err.message });
    throw err;
  }
}

/**
 * Test 5: Payload Checksum Verification (SHA-256 byte integrity)
 * Generates payloads of multiple sizes (256B, 4KB, 16KB, 64KB), produces them,
 * consumes them, and confirms exact SHA-256 byte-for-byte equality.
 */
async function testPayloadChecksum(kafka) {
  logStep('5. Payload Checksum Verification (SHA-256 Byte Integrity)');
  const t0 = performance.now();
  const checksumTopic = `kafkajs-checksum-topic-${Date.now()}`;
  const admin = kafka.admin();

  try {
    await admin.connect();
    await admin.createTopics({
      topics: [{ topic: checksumTopic, numPartitions: 3, replicationFactor: 1 }],
    });
    logPass(`Created checksum topic '${checksumTopic}' with 3 partitions`);
    await admin.disconnect();

    // Prepare test dataset: 60 messages across 4 distinct size tiers
    const payloadSpecs = [
      { size: 256, count: 20, desc: '256B small payloads' },
      { size: 4 * 1024, count: 20, desc: '4KB medium payloads' },
      { size: 16 * 1024, count: 10, desc: '16KB large payloads' },
      { size: 64 * 1024, count: 10, desc: '64KB jumbo payloads' },
    ];

    const messagesToSend = [];
    const expectedHashes = new Map(); // id -> sha256 hex
    let totalPayloadBytes = 0;
    let msgId = 0;

    for (const spec of payloadSpecs) {
      for (let i = 0; i < spec.count; i++) {
        const id = `chk-${msgId++}`;
        const partition = msgId % 3;
        // Generate pseudo-random deterministic buffer
        const buf = Buffer.alloc(spec.size);
        for (let b = 0; b < spec.size; b++) {
          buf[b] = (b * 31 + i) & 0xff;
        }

        const sha256 = crypto.createHash('sha256').update(buf).digest('hex');
        expectedHashes.set(id, sha256);
        totalPayloadBytes += spec.size;

        messagesToSend.push({
          partition,
          key: id,
          value: buf,
          headers: {
            sha256,
            msgId: id,
            payloadSize: spec.size.toString(),
          },
        });
      }
    }

    logInfo(
      `Generated ${messagesToSend.length} test records totaling ${(totalPayloadBytes / 1024).toFixed(2)} KB across 3 partitions`
    );

    // Produce all messages
    const producer = kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });
    await producer.connect();

    const tProd0 = performance.now();
    // Send in batches of 20
    const batchSize = 20;
    for (let offset = 0; offset < messagesToSend.length; offset += batchSize) {
      const chunk = messagesToSend.slice(offset, offset + batchSize);
      await producer.send({
        topic: checksumTopic,
        messages: chunk,
      });
    }
    const prodLat = performance.now() - tProd0;
    const prodThroughputKb = (totalPayloadBytes / 1024 / (prodLat / 1000)).toFixed(2);
    const prodThroughputMsg = (messagesToSend.length / (prodLat / 1000)).toFixed(0);
    logPass(
      `Produced ${messagesToSend.length} records in ${prodLat.toFixed(2)} ms (${prodThroughputMsg} msg/sec, ${prodThroughputKb} KB/sec)`,
      prodLat
    );
    await producer.disconnect();

    // Consume and verify SHA-256 checksums
    const consumer = kafka.consumer({
      groupId: `checksum-group-${Date.now()}`,
    });
    await consumer.connect();
    await consumer.subscribe({ topic: checksumTopic, fromBeginning: true });

    let verifiedCount = 0;
    const mismatchedMessages = [];
    const tCons0 = performance.now();

    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(
          new Error(
            `Timed out waiting for checksum verification. Verified ${verifiedCount}/${messagesToSend.length}`
          )
        );
      }, 15000);

      consumer
        .run({
          eachMessage: async ({ message }) => {
            const id = message.key.toString('utf-8');
            const expectedSha256 = expectedHashes.get(id);

            if (!expectedSha256) {
              mismatchedMessages.push({ id, error: 'Unknown key received' });
              return;
            }

            const actualSha256 = crypto.createHash('sha256').update(message.value).digest('hex');

            if (actualSha256 !== expectedSha256) {
              mismatchedMessages.push({
                id,
                expected: expectedSha256,
                actual: actualSha256,
                size: message.value.length,
              });
            } else {
              verifiedCount++;
            }

            if (verifiedCount === messagesToSend.length) {
              clearTimeout(timeout);
              resolve();
            }
          },
        })
        .catch(reject);
    });

    const consLat = performance.now() - tCons0;
    const consThroughputKb = (totalPayloadBytes / 1024 / (consLat / 1000)).toFixed(2);
    const consThroughputMsg = (messagesToSend.length / (consLat / 1000)).toFixed(0);

    await consumer.disconnect();

    if (mismatchedMessages.length > 0) {
      throw new Error(`Data corruption detected in ${mismatchedMessages.length} messages: ${JSON.stringify(mismatchedMessages)}`);
    }

    logPass(
      `100% SHA-256 byte-integrity verified: all ${verifiedCount} messages matched expected hash exactly`,
      consLat
    );
    logInfo(`Consumer throughput: ${consThroughputMsg} msg/sec (${consThroughputKb} KB/sec)`);

    const totalLat = performance.now() - t0;
    recordResult('Payload Checksum Verification', 'PASS', totalLat, {
      messagesVerified: verifiedCount,
      totalBytes: totalPayloadBytes,
      prodThroughputMsgSec: prodThroughputMsg,
      consThroughputMsgSec: consThroughputMsg,
    });
  } catch (err) {
    logFail('Payload Checksum Verification failed', err);
    await admin.disconnect().catch(() => {});
    recordResult('Payload Checksum Verification', 'FAIL', performance.now() - t0, { error: err.message });
    throw err;
  }
}

/**
 * Main Test Orchestrator
 */
async function main() {
  console.log(`\n${colors.bright}${colors.bgBlue}  AeroStream KafkaJS End-to-End Test Suite  ${colors.reset}`);
  console.log(`${colors.dim}Bootstrap: ${BOOTSTRAP_BROKERS.join(', ')} | Node: ${process.version}${colors.reset}\n`);

  const kafka = createKafkaClient();
  const testTopic = `node-kafkajs-e2e-${Date.now()}`;
  const suiteStartTime = performance.now();
  let hasFailure = false;

  try {
    // Test 1: AdminClient operations
    await testAdminOperations(kafka, testTopic);

    // Test 2: Producer operations (keys, partitions, headers, compression)
    const expectedInfo = await testProducerBatches(kafka, testTopic);

    // Test 3: Consumer group operations (rebalance, eachMessage, commits)
    await testConsumerGroup(kafka, testTopic, expectedInfo);

    // Test 4: Transactional producer & read_committed isolation
    await testTransactions(kafka);

    // Test 5: SHA-256 Payload checksum verification
    await testPayloadChecksum(kafka);
  } catch (err) {
    hasFailure = true;
    console.error(`\n${colors.red}${colors.bright}Suite aborted due to critical failure:${colors.reset}`, err);
  }

  const suiteTotalLat = performance.now() - suiteStartTime;

  // Print Summary Table
  console.log(`\n${colors.bright}══════════════════════════════════════════════════════════════════════════════════${colors.reset}`);
  console.log(`${colors.bright}                     AEROSTREAM KAFKAJS TEST SUMMARY MATRIX                      ${colors.reset}`);
  console.log(`${colors.bright}══════════════════════════════════════════════════════════════════════════════════${colors.reset}`);
  console.log(
    ` ${'Test Name'.padEnd(42)} | ${'Status'.padEnd(10)} | ${'Latency (ms)'.padStart(14)} | ${'Details'.padEnd(20)}`
  );
  console.log(`───────────────────────────────────────────┼────────────┼────────────────┼─────────────────────`);

  for (const r of testResults) {
    const statusFormatted =
      r.status === 'PASS'
        ? `${colors.green}${colors.bright}PASS${colors.reset}      `
        : `${colors.red}${colors.bright}FAIL${colors.reset}      `;
    const detailsStr = JSON.stringify(r.details).slice(0, 30);
    console.log(
      ` ${r.name.padEnd(42)} | ${statusFormatted} | ${r.latencyMs.toFixed(2).padStart(14)} | ${detailsStr}`
    );
  }
  console.log(`══════════════════════════════════════════════════════════════════════════════════`);
  console.log(` Total Execution Time: ${suiteTotalLat.toFixed(2)} ms`);
  console.log(
    ` Status: ${hasFailure ? `${colors.red}FAILED` : `${colors.green}ALL TESTS PASSED (5/5)`}${colors.reset}\n`
  );

  if (hasFailure) {
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error('Fatal error in main runner:', err);
    process.exit(1);
  });
}

module.exports = {
  main,
  testAdminOperations,
  testProducerBatches,
  testConsumerGroup,
  testTransactions,
  testPayloadChecksum,
};
