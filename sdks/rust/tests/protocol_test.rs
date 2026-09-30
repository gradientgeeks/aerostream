use bytes::Bytes;
use aerostream_client::protocol::*;
use aerostream_client::AeroError;

#[test]
fn test_magic_and_constants() {
    assert_eq!(MAGIC, [0xAE, 0x01]);
    assert_eq!(HEADER_LEN, 7);
    assert!(is_valid_magic(0xAE, 0x01));
    assert!(!is_valid_magic(0xAE, 0x02));
    assert!(!is_valid_magic(0x00, 0x01));
}

#[test]
fn test_auth_request_encoding() {
    let token = "test-secret-token";
    let frame = encode_auth_request(token);

    assert_eq!(frame.len(), 7 + token.len());
    assert_eq!(&frame[0..2], &[0xAE, 0x01]);
    assert_eq!(frame[2], 0); // Command::Auth
    let body_len = u32::from_be_bytes(frame[3..7].try_into().unwrap());
    assert_eq!(body_len as usize, token.len());
    assert_eq!(&frame[7..], token.as_bytes());
}

#[test]
fn test_produce_request_encoding() {
    let topic = "payments";
    let partition = 3u32;
    let payload = b"transaction-data-payload";

    let frame = encode_produce_request(topic, partition, payload).expect("encoding succeeds");

    assert_eq!(&frame[0..2], &[0xAE, 0x01]);
    assert_eq!(frame[2], 1); // Command::Produce

    let body = &frame[7..];
    let topic_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    assert_eq!(topic_len, topic.len());
    assert_eq!(&body[2..2 + topic_len], topic.as_bytes());

    let parsed_part = u32::from_be_bytes(body[2 + topic_len..6 + topic_len].try_into().unwrap());
    assert_eq!(parsed_part, partition);

    let payload_len = u32::from_be_bytes(body[6 + topic_len..10 + topic_len].try_into().unwrap()) as usize;
    assert_eq!(payload_len, payload.len());
    assert_eq!(&body[10 + topic_len..], payload);
}

#[test]
fn test_produce_response_decoding() {
    let status_prefix = [0xAE, 0x01, STATUS_OK];
    let status = decode_status_prefix(&status_prefix).unwrap();
    assert_eq!(status, STATUS_OK);

    let offset_bytes = 1024u64.to_be_bytes();
    let offset = decode_produce_response(status, Some(&offset_bytes)).unwrap();
    assert_eq!(offset, 1024);

    // Test Out of order
    let err = decode_produce_response(STATUS_OUT_OF_ORDER, None).unwrap_err();
    assert!(matches!(err, AeroError::OutOfOrder));

    // Test Auth failure
    let err = decode_produce_response(STATUS_AUTH_FAILED, None).unwrap_err();
    assert!(matches!(err, AeroError::AuthFailed(_)));
}

#[test]
fn test_fetch_request_encoding() {
    let topic = "metrics";
    let partition = 1u32;
    let start_offset = 500u64;
    let max_bytes = 65536u32;

    let frame = encode_fetch_request(topic, partition, start_offset, max_bytes).unwrap();
    assert_eq!(&frame[0..2], &[0xAE, 0x01]);
    assert_eq!(frame[2], 2); // Command::Fetch

    let body = &frame[7..];
    let topic_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    assert_eq!(topic_len, topic.len());
    assert_eq!(&body[2..2 + topic_len], topic.as_bytes());

    let mut cursor = 2 + topic_len;
    let part = u32::from_be_bytes(body[cursor..cursor + 4].try_into().unwrap());
    cursor += 4;
    assert_eq!(part, partition);

    let off = u64::from_be_bytes(body[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    assert_eq!(off, start_offset);

    let bytes = u32::from_be_bytes(body[cursor..cursor + 4].try_into().unwrap());
    assert_eq!(bytes, max_bytes);
}

#[test]
fn test_fetch_multi_request_and_response() {
    let topic = "sensor-events";
    let partition = 0u32;
    let start_offset = 120u64;
    let max_bytes = 1048576u32;
    let max_wait_ms = 250u32;

    let frame = encode_fetch_multi_request(topic, partition, start_offset, max_bytes, max_wait_ms).unwrap();
    assert_eq!(&frame[0..2], &[0xAE, 0x01]);
    assert_eq!(frame[2], 4); // Command::FetchMulti

    // Mock multi-entry response with 3 records
    let mut index_data = Vec::new();
    let mut payload_data = Vec::new();

    let items: Vec<(u64, &[u8])> = vec![
        (120, b"entry-120"),
        (121, b"entry-121-longer"),
        (122, b"entry-122"),
    ];

    for (off, data) in &items {
        index_data.extend_from_slice(&off.to_be_bytes());
        index_data.extend_from_slice(&(data.len() as u32).to_be_bytes());
        payload_data.extend_from_slice(data);
    }

    let records = decode_multi_entries(
        items.len() as u32,
        &index_data,
        Bytes::from(payload_data),
    ).expect("decoding succeeded");

    assert_eq!(records.len(), 3);
    for (i, (expected_off, expected_data)) in items.iter().enumerate() {
        assert_eq!(records[i].offset, *expected_off);
        assert_eq!(&records[i].payload[..], *expected_data);
    }
}

#[test]
fn test_decode_status_prefix_validation() {
    let valid = [0xAE, 0x01, 0x00];
    assert_eq!(decode_status_prefix(&valid).unwrap(), 0);

    let invalid_magic = [0x00, 0x01, 0x00];
    assert!(decode_status_prefix(&invalid_magic).is_err());
}
