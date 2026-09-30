import {
  MAGIC_0,
  MAGIC_1,
  HEADER_SIZE,
  CMD_AUTH,
  CMD_PRODUCE,
  CMD_FETCH,
  CMD_FETCH_MULTI,
  STATUS_OK,
  STATUS_EMPTY,
  STATUS_DATA,
  STATUS_AUTH_FAILED,
  STATUS_OUT_OF_ORDER,
} from './constants.js';
import { AeroRecord, ProduceResponse } from './types.js';
import { AeroError, AuthenticationError, OutOfOrderError } from '../errors.js';

export function encodeAuthRequest(token: string): Buffer {
  const tokenBytes = Buffer.from(token, 'utf-8');
  const buf = Buffer.allocUnsafe(HEADER_SIZE + tokenBytes.length);
  buf[0] = MAGIC_0;
  buf[1] = MAGIC_1;
  buf[2] = CMD_AUTH;
  buf.writeUInt32BE(tokenBytes.length, 3);
  tokenBytes.copy(buf, HEADER_SIZE);
  return buf;
}

export function encodeProduceRequest(
  topic: string,
  partition: number,
  payload: Uint8Array
): Buffer {
  const topicBytes = Buffer.from(topic, 'utf-8');
  const bodyLen = 2 + topicBytes.length + 4 + 4 + payload.length;
  const buf = Buffer.allocUnsafe(HEADER_SIZE + bodyLen);

  buf[0] = MAGIC_0;
  buf[1] = MAGIC_1;
  buf[2] = CMD_PRODUCE;
  buf.writeUInt32BE(bodyLen, 3);

  let offset = HEADER_SIZE;
  buf.writeUInt16BE(topicBytes.length, offset);
  offset += 2;
  topicBytes.copy(buf, offset);
  offset += topicBytes.length;
  buf.writeUInt32BE(partition, offset);
  offset += 4;
  buf.writeUInt32BE(payload.length, offset);
  offset += 4;
  Buffer.from(payload).copy(buf, offset);

  return buf;
}

export function encodeFetchRequest(
  topic: string,
  partition: number,
  startOffset: bigint,
  maxBytes: number
): Buffer {
  const topicBytes = Buffer.from(topic, 'utf-8');
  const bodyLen = 2 + topicBytes.length + 4 + 8 + 4;
  const buf = Buffer.allocUnsafe(HEADER_SIZE + bodyLen);

  buf[0] = MAGIC_0;
  buf[1] = MAGIC_1;
  buf[2] = CMD_FETCH;
  buf.writeUInt32BE(bodyLen, 3);

  let offset = HEADER_SIZE;
  buf.writeUInt16BE(topicBytes.length, offset);
  offset += 2;
  topicBytes.copy(buf, offset);
  offset += topicBytes.length;
  buf.writeUInt32BE(partition, offset);
  offset += 4;
  buf.writeBigUInt64BE(startOffset, offset);
  offset += 8;
  buf.writeUInt32BE(maxBytes, offset);

  return buf;
}

export function encodeMultiFetchRequest(
  topic: string,
  partition: number,
  startOffset: bigint,
  maxBytes: number,
  maxWaitMs: number
): Buffer {
  const topicBytes = Buffer.from(topic, 'utf-8');
  const bodyLen = 2 + topicBytes.length + 4 + 8 + 4 + 4;
  const buf = Buffer.allocUnsafe(HEADER_SIZE + bodyLen);

  buf[0] = MAGIC_0;
  buf[1] = MAGIC_1;
  buf[2] = CMD_FETCH_MULTI;
  buf.writeUInt32BE(bodyLen, 3);

  let offset = HEADER_SIZE;
  buf.writeUInt16BE(topicBytes.length, offset);
  offset += 2;
  topicBytes.copy(buf, offset);
  offset += topicBytes.length;
  buf.writeUInt32BE(partition, offset);
  offset += 4;
  buf.writeBigUInt64BE(startOffset, offset);
  offset += 8;
  buf.writeUInt32BE(maxBytes, offset);
  offset += 4;
  buf.writeUInt32BE(maxWaitMs, offset);

  return buf;
}

export function decodeAuthResponse(buf: Buffer): { status: number } {
  if (buf.length < 3) {
    throw new AeroError(`Auth response frame too short: ${buf.length} bytes`);
  }
  if (buf[0] !== MAGIC_0 || buf[1] !== MAGIC_1) {
    throw new AeroError(`Invalid magic bytes: 0x${buf[0].toString(16)} 0x${buf[1].toString(16)}`);
  }
  const status = buf[2];
  if (status === STATUS_AUTH_FAILED) {
    throw new AuthenticationError('Broker rejected client authentication token');
  }
  if (status !== STATUS_OK) {
    throw new AeroError(`Unexpected auth response status: ${status}`);
  }
  return { status };
}

export function decodeProduceResponse(buf: Buffer): ProduceResponse {
  if (buf.length < 3) {
    throw new AeroError(`Produce response frame too short: ${buf.length} bytes`);
  }
  if (buf[0] !== MAGIC_0 || buf[1] !== MAGIC_1) {
    throw new AeroError(`Invalid magic bytes: 0x${buf[0].toString(16)} 0x${buf[1].toString(16)}`);
  }
  const status = buf[2];
  if (status === STATUS_OUT_OF_ORDER) {
    throw new OutOfOrderError();
  }
  if (status !== STATUS_OK) {
    throw new AeroError(`Produce failed with status: ${status}`);
  }
  if (buf.length < 11) {
    throw new AeroError(`Produce response missing offset: ${buf.length} bytes`);
  }
  const offset = buf.readBigUInt64BE(3);
  return { status, offset };
}

export function decodeMultiFetchResponse(
  buf: Buffer,
  topic: string,
  partition: number
): AeroRecord[] {
  if (buf.length < 3) {
    throw new AeroError(`Fetch response frame too short: ${buf.length} bytes`);
  }
  if (buf[0] !== MAGIC_0 || buf[1] !== MAGIC_1) {
    throw new AeroError(`Invalid magic bytes: 0x${buf[0].toString(16)} 0x${buf[1].toString(16)}`);
  }

  const status = buf[2];
  if (status === STATUS_EMPTY) {
    return [];
  }
  if (status !== STATUS_DATA) {
    throw new AeroError(`Fetch failed with status code: ${status}`);
  }

  if (buf.length < 7) {
    throw new AeroError(`MultiFetch frame missing entry count: ${buf.length} bytes`);
  }

  const entryCount = buf.readUInt32BE(3);
  if (entryCount === 0) {
    return [];
  }

  const headerTableSize = entryCount * 12;
  const payloadStart = 7 + headerTableSize;
  if (buf.length < payloadStart) {
    throw new AeroError(
      `MultiFetch frame truncated: expected at least ${payloadStart} bytes, got ${buf.length}`
    );
  }

  const entries: { offset: bigint; len: number }[] = [];
  let tableCursor = 7;
  for (let i = 0; i < entryCount; i++) {
    const offset = buf.readBigUInt64BE(tableCursor);
    const len = buf.readUInt32BE(tableCursor + 8);
    tableCursor += 12;
    entries.push({ offset, len });
  }

  const records: AeroRecord[] = [];
  let payloadCursor = payloadStart;
  const now = new Date();

  for (const entry of entries) {
    if (payloadCursor + entry.len > buf.length) {
      throw new AeroError(
        `MultiFetch payload truncated: needed ${entry.len} bytes at ${payloadCursor}, total ${buf.length}`
      );
    }
    const payload = Buffer.from(buf.subarray(payloadCursor, payloadCursor + entry.len));
    payloadCursor += entry.len;

    records.push({
      offset: entry.offset,
      payload,
      topic,
      partition,
      timestamp: now,
    });
  }

  return records;
}
