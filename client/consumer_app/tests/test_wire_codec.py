"""Unit test verifying Kafka wire protocol codec functions."""

import io
import struct
import unittest

from consumer_app.utils.wire_protocol import (
    crc32c,
    decode_varint,
    decode_varlong,
    encode_varint,
    encode_varlong,
    put_bytes,
    put_string,
    read_bytes,
    read_string,
    wrap_kafka_v0_message,
    wrap_kafka_v2_record_batch,
)


class TestWireCodec(unittest.TestCase):
    def test_varint_roundtrip(self):
        for val in [0, 1, -1, 42, -42, 127, 128, 300, 65535, -65535, 2147483647, -2147483648]:
            encoded = encode_varint(val)
            decoded = decode_varint(io.BytesIO(encoded))
            self.assertEqual(decoded, val, f"Varint mismatch for {val}")

    def test_varlong_roundtrip(self):
        for val in [0, 1, -1, 1000000000, -1000000000, 9223372036854775807, -9223372036854775808]:
            encoded = encode_varlong(val)
            decoded = decode_varlong(io.BytesIO(encoded))
            self.assertEqual(decoded, val, f"Varlong mismatch for {val}")

    def test_string_roundtrip(self):
        # Normal string
        s = "aerostream-kafka-test"
        packed = put_string(s)
        unpacked, offset = read_string(packed, 0)
        self.assertEqual(unpacked, s)
        self.assertEqual(offset, len(packed))

        # None / Null string
        packed_none = put_string(None)
        unpacked_none, off = read_string(packed_none, 0)
        self.assertIsNone(unpacked_none)
        self.assertEqual(off, 2)

    def test_bytes_roundtrip(self):
        b = b"\x01\x02\x03\x04hello"
        packed = put_bytes(b)
        unpacked, offset = read_bytes(packed, 0)
        self.assertEqual(unpacked, b)

        packed_none = put_bytes(None)
        unpacked_none, off = read_bytes(packed_none, 0)
        self.assertIsNone(unpacked_none)

    def test_crc32c_castagnoli(self):
        # Known standard test vectors for Castagnoli CRC32C:
        # 32 zeroes -> 0x8a9136aa
        zeroes = b"\x00" * 32
        self.assertEqual(crc32c(zeroes), 0x8a9136aa)

    def test_record_batch_v2_structure(self):
        records = [
            (b"key1", b"val1", [("header1", b"v1")]),
            (b"key2", b"val2", [("header2", b"v2"), ("header3", b"v3")]),
        ]
        batch = wrap_kafka_v2_record_batch(base_offset=10, records=records, producer_id=1005, producer_epoch=2)
        self.assertTrue(len(batch) > 60)
        base_off, batch_len, leader_epoch, magic = struct.unpack_from(">qiib", batch, 0)
        self.assertEqual(base_off, 10)
        self.assertEqual(magic, 2)
        self.assertEqual(len(batch), 12 + batch_len)


if __name__ == "__main__":
    unittest.main()
