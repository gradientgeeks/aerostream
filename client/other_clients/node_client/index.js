/**
 * AeroStream Node.js Kafka Client Entrypoint
 */

'use strict';

const runner = require('./kafkajs_runner');

if (require.main === module) {
  runner.main().catch((err) => {
    console.error('Fatal error running KafkaJS test suite:', err);
    process.exit(1);
  });
}

module.exports = runner;
