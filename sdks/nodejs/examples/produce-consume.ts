import { AeroClient } from '../src/index.js';

async function main() {
  console.log('Connecting to AeroStream native port 9091...');

  const client = await AeroClient.connect({
    host: '127.0.0.1',
    port: 9091,
    authToken: 'optional-auth-token',
  });

  try {
    const producer = client.producer({ maxInFlight: 128 });
    const consumer = client.consumer();

    const topic = 'quickstart-events';
    const partition = 0;

    // Produce messages
    console.log(`Producing records to topic '${topic}' partition ${partition}...`);
    for (let i = 0; i < 5; i++) {
      const payload = JSON.stringify({ eventId: i, timestamp: new Date().toISOString() });
      const offset = await producer.send(topic, partition, payload);
      console.log(`Produced event ${i} at offset ${offset}`);
    }

    // Fetch batch
    console.log('Fetching batch from offset 0...');
    const records = await consumer.fetch(topic, partition, 0n);
    console.log(`Fetched ${records.length} records:`);
    for (const record of records) {
      console.log(` - Offset: ${record.offset}, Payload: ${record.payload.toString('utf-8')}`);
    }

    // Stream consumption with AsyncGenerator
    console.log('Streaming records with AsyncGenerator...');
    const controller = new AbortController();
    let count = 0;

    for await (const record of consumer.stream(topic, partition, {
      signal: controller.signal,
      startOffset: 0n,
    })) {
      console.log(`Streamed: [${record.offset}] ${record.payload.toString('utf-8')}`);
      count++;
      if (count >= 5) {
        controller.abort();
      }
    }
  } finally {
    await client.close();
    console.log('Client closed.');
  }
}

main().catch(console.error);
