import { test, describe } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  encodeAuthRequest,
  encodeProduceRequest,
  encodeFetchRequest,
  encodeMultiFetchRequest,
  decodeAuthResponse,
  decodeProduceResponse,
  decodeMultiFetchResponse,
} from '../src/protocol/frame.js';
import {
  MAGIC_0,
  MAGIC_1,
  CMD_AUTH,
  CMD_PRODUCE,
  CMD_FETCH,
  CMD_FETCH_MULTI,
  STATUS_OK,
  STATUS_AUTH_FAILED,
  STATUS_OUT_OF_ORDER,
  STATUS_EMPTY,
  STATUS_DATA,
} from '../src/protocol/constants.js';
import { AuthenticationError, OutOfOrderError, AeroError } from '../src/errors.js';

describe('AeroStream Protocol Framing', () => {
  test('encodeAuthRequest should correctly frame auth token', () => {
    const token = 'my-token';
    const buf = encodeAuthRequest(token);

    assert.equal(buf[0], MAGIC_0);
    assert.equal(buf[1], MAGIC_1);
    assert.equal(buf[2], CMD_AUTH);
    assert.equal(buf.readUInt32BE(3), Buffer.byteLength(token));
    assert.equal(buf.subarray(7).toString('utf-8'), token);
  });

  test('decodeAuthResponse should handle OK status and reject AUTH_FAILED', () => {
    const okBuf = Buffer.from([MAGIC_0, MAGIC_1, STATUS_OK]);
    const res = decodeAuthResponse(okBuf);
    assert.equal(res.status, STATUS_OK);

    const failBuf = Buffer.from([MAGIC_0, MAGIC_1, STATUS_AUTH_FAILED]);
    assert.throws(() => decodeAuthResponse(failBuf), AuthenticationError);

    const badMagicBuf = Buffer.from([0x00, 0x00, STATUS_OK]);
    assert.throws(() => decodeAuthResponse(badMagicBuf), AeroError);
  });

  test('encodeProduceRequest should construct correct binary frame', () => {
    const topic = 'orders';
    const partition = 2;
    const payload = Buffer.from('order-data-123');

    const buf = encodeProduceRequest(topic, partition, payload);
    assert.equal(buf[0], MAGIC_0);
    assert.equal(buf[1], MAGIC_1);
    assert.equal(buf[2], CMD_PRODUCE);

    let offset = 7;
    const topicLen = buf.readUInt16BE(offset);
    offset += 2;
    assert.equal(topicLen, Buffer.byteLength(topic));
    assert.equal(buf.subarray(offset, offset + topicLen).toString('utf-8'), topic);
    offset += topicLen;
    assert.equal(buf.readUInt32BE(offset), partition);
    offset += 4;
    const payloadLen = buf.readUInt32BE(offset);
    offset += 4;
    assert.equal(payloadLen, payload.length);
    assert.deepEqual(buf.subarray(offset, offset + payloadLen), payload);
  });

  test('decodeProduceResponse should decode 64-bit offset and handle error status', () => {
    const buf = Buffer.alloc(11);
    buf[0] = MAGIC_0;
    buf[1] = MAGIC_1;
    buf[2] = STATUS_OK;
    buf.writeBigUInt64BE(9999999999n, 3);

    const res = decodeProduceResponse(buf);
    assert.equal(res.status, STATUS_OK);
    assert.equal(res.offset, 9999999999n);

    const errBuf = Buffer.from([MAGIC_0, MAGIC_1, STATUS_OUT_OF_ORDER]);
    assert.throws(() => decodeProduceResponse(errBuf), OutOfOrderError);
  });

  test('encodeFetchRequest and encodeMultiFetchRequest framing', () => {
    const fetchBuf = encodeFetchRequest('events', 1, 100n, 65536);
    assert.equal(fetchBuf[0], MAGIC_0);
    assert.equal(fetchBuf[1], MAGIC_1);
    assert.equal(fetchBuf[2], CMD_FETCH);

    const multiBuf = encodeMultiFetchRequest('events', 1, 100n, 65536, 500);
    assert.equal(multiBuf[0], MAGIC_0);
    assert.equal(multiBuf[1], MAGIC_1);
    assert.equal(multiBuf[2], CMD_FETCH_MULTI);
  });

  test('decodeMultiFetchResponse handles empty and multi-entry payload', () => {
    const emptyBuf = Buffer.from([MAGIC_0, MAGIC_1, STATUS_EMPTY]);
    const emptyRecords = decodeMultiFetchResponse(emptyBuf, 'test', 0);
    assert.equal(emptyRecords.length, 0);

    // Multi-entry: 2 records:
    // record 1: offset 10n, len 5 ("hello")
    // record 2: offset 11n, len 5 ("world")
    const p1 = Buffer.from('hello');
    const p2 = Buffer.from('world');

    const entryCount = 2;
    const totalLen = 3 + 4 + entryCount * 12 + p1.length + p2.length;
    const buf = Buffer.alloc(totalLen);
    buf[0] = MAGIC_0;
    buf[1] = MAGIC_1;
    buf[2] = STATUS_DATA;
    buf.writeUInt32BE(entryCount, 3);

    // Entry 0
    buf.writeBigUInt64BE(10n, 7);
    buf.writeUInt32BE(p1.length, 15);

    // Entry 1
    buf.writeBigUInt64BE(11n, 19);
    buf.writeUInt32BE(p2.length, 27);

    // Payloads
    p1.copy(buf, 31);
    p2.copy(buf, 31 + p1.length);

    const records = decodeMultiFetchResponse(buf, 'test-topic', 0);
    assert.equal(records.length, 2);
    assert.equal(records[0].offset, 10n);
    assert.equal(records[0].payload.toString('utf-8'), 'hello');
    assert.equal(records[1].offset, 11n);
    assert.equal(records[1].payload.toString('utf-8'), 'world');
  });
});
