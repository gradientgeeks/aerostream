export { AeroClient } from './client/client.js';
export { AeroProducer } from './client/producer.js';
export { AeroConsumer } from './client/consumer.js';
export type { AeroClientOptions, ProducerOptions, ConsumerOptions } from './client/options.js';
export type { AeroRecord, ProduceRequest, ProduceResponse, FetchRequest, MultiFetchResponse } from './protocol/types.js';
export {
  AeroError,
  AuthenticationError,
  OutOfOrderError,
  ConnectionError,
  TimeoutError,
} from './errors.js';
export {
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
} from './protocol/constants.js';
export {
  encodeAuthRequest,
  encodeProduceRequest,
  encodeFetchRequest,
  encodeMultiFetchRequest,
  decodeAuthResponse,
  decodeProduceResponse,
  decodeMultiFetchResponse,
} from './protocol/frame.js';
export { AeroConnection } from './transport/connection.js';
