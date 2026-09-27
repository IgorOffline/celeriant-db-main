use celeriant_msg::process_client_responses::{ClientResponse, ClientResponseType};
use celeriant_msg::response::responses::{AggregateConflict, DeleteConflictResponse, WriteConflictResponse};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::network::wire_header::{WIRE_HEADER_SIZE, WireHeader, serialise_heap_versioned};
use futures_lite::future::block_on;
use futures_lite::io::Cursor;

const CEILING: u64 = 64 * 1024 * 1024;
const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../fixtures/client-protocol");

fn fixture(version: u32, name: &str) -> Vec<u8> {
    std::fs::read(format!("{FIXTURES}/v{version}-{name}")).unwrap()
}

fn record(key: (u128, u128, u128), expected: u64, current: u64) -> AggregateConflict {
    AggregateConflict {
        key: AggregateKey::new(key.0, key.1, key.2),
        expected,
        current,
    }
}

fn records(case: &str) -> Vec<AggregateConflict> {
    let keys = match case {
        "single" => vec![(u128::MAX, u128::MAX, u128::MAX)],
        "many" => vec![(1, 1, 255), (1, 1, 256), (1, 256, 0), (255, 0, 0), (256, 0, 0)],
        _ => panic!("unknown fixture case"),
    };
    keys.into_iter()
        .enumerate()
        .map(|(i, key)| record(key, u64::MAX - i as u64, i as u64))
        .collect()
}

fn response(id: u32, conflicts: Vec<AggregateConflict>, correlation_id: Option<u128>) -> ClientResponse {
    match id {
        13 => ClientResponse::WriteConflict(WriteConflictResponse { correlation_id, conflicts }),
        14 => ClientResponse::DeleteConflict(DeleteConflictResponse { correlation_id, conflicts }),
        _ => panic!("unknown response ID"),
    }
}

fn body(response: &ClientResponse, version: u32) -> Vec<u8> {
    match response {
        ClientResponse::WriteConflict(value) => serialise_heap_versioned(value, version).unwrap(),
        ClientResponse::DeleteConflict(value) => serialise_heap_versioned(value, version).unwrap(),
        _ => panic!("expected typed conflict"),
    }
}

fn conflicts(response: &ClientResponse) -> &[AggregateConflict] {
    match response {
        ClientResponse::WriteConflict(value) => &value.conflicts,
        ClientResponse::DeleteConflict(value) => &value.conflicts,
        _ => panic!("expected typed conflict"),
    }
}

fn assert_records(actual: &[AggregateConflict], expected: &[AggregateConflict]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.key, expected.key);
        assert_eq!(actual.expected, expected.expected);
        assert_eq!(actual.current, expected.current);
    }
}

#[test]
fn frozen_binary_bodies_and_frames_round_trip_both_operations() {
    block_on(async {
        let codec = DictCodec::new(&[], 3).unwrap();
        for version in [4, 5] {
            for (id, operation) in [(13, "write"), (14, "delete")] {
                for case in ["single", "many"] {
                    let response = response(id, records(case), Some(u128::MAX));
                    assert_eq!(response.response_type() as u32, id);
                    assert_eq!(ClientResponseType::from_u32(id).unwrap() as u32, id);
                    assert!(!ClientResponse::is_fixed_size_variant(id));
                    let expected_body = fixture(version, &format!("{case}.body.bin"));
                    assert_eq!(body(&response, version), expected_body, "v{version} {operation} {case} body");
                    let mut frame = Vec::new();
                    ClientResponse::write_response(&mut frame, &response, true, &codec, CEILING, version)
                        .await
                        .unwrap();
                    assert_eq!(
                        frame,
                        fixture(version, &format!("{operation}-{case}.frame.bin")),
                        "v{version} {operation} {case} frame"
                    );
                    assert_eq!(frame.len(), WIRE_HEADER_SIZE + expected_body.len());
                    let decoded = ClientResponse::read_response(&mut Cursor::new(frame), CEILING).await.unwrap();
                    assert_eq!(decoded.response_type() as u32, id);
                    assert_eq!(decoded.correlation_id(), Some(u128::MAX));
                    assert_records(conflicts(&decoded), conflicts(&response));
                }
                for correlation_id in [None, Some(0), Some(1 << 127)] {
                    let original = response(id, records("many"), correlation_id);
                    let decoded = ClientResponse::deserialize_body(id, &body(&original, version), version).unwrap();
                    assert_eq!(decoded.correlation_id(), correlation_id);
                    assert_records(conflicts(&decoded), conflicts(&original));
                }
            }
        }
    });
}

#[test]
fn maximal_unsigned_values_fit_the_reservation_at_array_boundaries() {
    for version in [4, 5] {
        for n in [1usize, 15, 16, 255, 256, 65_535, 65_536] {
            let items = (0..n)
                .map(|i| record((u128::MAX, u128::MAX, u128::MAX - (n - 1 - i) as u128), u64::MAX, u64::MAX - 1))
                .collect();
            let original = response(13, items, Some(u128::MAX));
            let bytes = body(&original, version);
            let shared_bound = 25usize.checked_add(74usize.checked_mul(n).unwrap()).unwrap();
            let codec_bound = if version == 4 { 25 + 64 * n } else { 24 + 74 * n };
            assert!(bytes.len() <= codec_bound, "v{version} N={n}: {} > {codec_bound}", bytes.len());
            assert!(bytes.len() <= shared_bound);
            if version == 4 {
                assert_eq!(bytes.len(), codec_bound);
            }
            for id in [13, 14] {
                let decoded = ClientResponse::deserialize_body(id, &bytes, version).unwrap();
                assert_records(conflicts(&decoded), conflicts(&original));
            }
        }
    }
}

#[test]
fn every_truncated_conflict_body_is_rejected() {
    for version in [4, 5] {
        let bytes = fixture(version, "many.body.bin");
        for id in [13, 14] {
            for cut in 0..bytes.len() {
                assert!(
                    ClientResponse::deserialize_body(id, &bytes[..cut], version).is_err(),
                    "v{version} ID={id} accepted prefix {cut}"
                );
            }
        }
    }
}

#[test]
fn receive_and_send_ceilings_are_body_limits_without_partial_frames() {
    block_on(async {
        let codec = DictCodec::new(&[], 3).unwrap();
        for version in [4, 5] {
            for id in [13, 14] {
                let original = response(id, records("many"), Some(u128::MAX));
                let size = body(&original, version).len() as u64;
                let mut frame = Vec::new();
                ClientResponse::write_response(&mut frame, &original, false, &codec, size, version)
                    .await
                    .unwrap();
                assert!(ClientResponse::read_response(&mut Cursor::new(&frame), size).await.is_ok());
                assert!(ClientResponse::read_response(&mut Cursor::new(&frame), size - 1).await.is_err());
                let mut rejected = Vec::new();
                assert!(
                    ClientResponse::write_response(&mut rejected, &original, false, &codec, size - 1, version)
                        .await
                        .is_err()
                );
                assert!(rejected.is_empty(), "oversized body must fail before writing a frame");
            }
        }
    });
}

fn random_word(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn random_u128(state: &mut u64) -> u128 {
    ((random_word(state) as u128) << 64) | random_word(state) as u128
}

#[test]
fn compression_expansion_uses_the_fitting_uncompressed_conflict_body() {
    block_on(async {
        let codec = DictCodec::new(&[], 3).unwrap();
        let mut state = 0xc0ff_ee42_8329_1387;
        let items: Vec<_> = (0..32)
            .map(|_| {
                record(
                    (random_u128(&mut state), random_u128(&mut state), random_u128(&mut state)),
                    random_word(&mut state),
                    random_word(&mut state),
                )
            })
            .collect();
        let original = response(13, items, Some(random_u128(&mut state)));
        let raw = body(&original, 4);
        assert!(raw.len() >= celeriant_msg::RESPONSE_COMPRESSION_THRESHOLD_BYTES);
        assert!(
            codec.compress(&raw).unwrap().len() > raw.len(),
            "fixture must actually cause compression expansion"
        );
        let mut frame = Vec::new();
        ClientResponse::write_response(&mut frame, &original, true, &codec, raw.len() as u64, 4)
            .await
            .unwrap();
        let header = WireHeader::from_reader(&mut Cursor::new(&frame), raw.len() as u64).await.unwrap();
        assert_eq!(header.compression_type, CompressionType::None);
        assert_eq!(&frame[WIRE_HEADER_SIZE..], raw);
        let decoded = ClientResponse::read_response(&mut Cursor::new(frame), raw.len() as u64).await.unwrap();
        assert_records(conflicts(&decoded), conflicts(&original));
    });
}
