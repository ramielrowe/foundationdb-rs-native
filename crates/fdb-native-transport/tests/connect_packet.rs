//! `ConnectPacket` against the vectors FDB's own `PacketWriter` produced
//! (tools/golden-vectors/fixtures/binary-release-*), and property tests.

use std::path::{Path, PathBuf};

use fdb_native_transport::{ConnectPacket, ConnectPacketError};
use fdb_native_wire::ProtocolVersion;
use fdb_native_wire::binary::DecodeError;
use proptest::prelude::*;
use serde_json::Value;

const RELEASES: [&str; 2] = ["release-7.4", "release-8.0"];

fn fixtures_dir(release: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/golden-vectors/fixtures")
        .join(format!("binary-{release}"))
}

fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()))
}

fn from_hex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex"))
        .collect()
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The `connect_packet` records listed under `key`.
fn records(release: &str, key: &str) -> Vec<Value> {
    let dir = fixtures_dir(release);
    read_json(&dir.join("index.json"))[key]
        .as_array()
        .expect("index list")
        .iter()
        .map(|f| read_json(&dir.join(f.as_str().expect("file name"))))
        .filter(|r| r["type"] == "connect_packet")
        .collect()
}

fn packet_of(v: &Value) -> ConnectPacket {
    ConnectPacket {
        protocol_version: ProtocolVersion(v["protocol_version"].as_u64().unwrap()),
        canonical_remote_port: v["canonical_remote_port"]
            .as_u64()
            .unwrap()
            .try_into()
            .unwrap(),
        connection_id: v["connection_id"].as_u64().unwrap(),
        canonical_remote_ip4: v["canonical_remote_ip4"]
            .as_u64()
            .unwrap()
            .try_into()
            .unwrap(),
        flags: v["flags"].as_u64().unwrap().try_into().unwrap(),
        canonical_remote_ip6: from_hex(v["canonical_remote_ip6"].as_str().unwrap())
            .try_into()
            .unwrap(),
    }
}

#[test]
fn vectors_decode_and_encode() {
    for release in RELEASES {
        let records = records(release, "positive");
        assert!(records.len() >= 4, "{release}: {} vectors", records.len());
        let current =
            read_json(&fixtures_dir(release).join("index.json"))["current_protocol_version"]
                .as_u64()
                .unwrap();
        let local = ProtocolVersion(current);
        for record in records {
            let name = record["name"].as_str().unwrap();
            let bytes = from_hex(record["bytes"].as_str().unwrap());
            let v = &record["value"];
            let expected = packet_of(v);
            assert_eq!(v["connect_packet_length"], ConnectPacket::LENGTH);
            assert_eq!(v["total_packet_size"], ConnectPacket::SIZE);

            // A longer buffer is fine: the packet says how long it is.
            let mut longer = bytes.clone();
            longer.extend_from_slice(b"next packet");
            let (decoded, used) = ConnectPacket::decode(&longer).unwrap();
            assert_eq!((decoded, used), (expected, bytes.len()), "{release}/{name}");
            assert_eq!(ConnectPacket::decode(&bytes).unwrap().0, expected);
            assert_eq!(
                to_hex(&expected.encode().unwrap()),
                to_hex(&bytes),
                "{name}"
            );

            // Every packet is what its own release's receiver accepts.
            assert!(expected.protocol_version.has_object_serializer_flag());
            assert_eq!(expected.check(local, None), Ok(()), "{release}/{name}");
            assert_eq!(
                expected.check(local, Some(expected.canonical_remote_port)),
                Ok(())
            );
            match expected.canonical_remote_address() {
                None => {
                    assert!(v["canonical_remote_ip"].is_null());
                    assert_eq!(expected.canonical_remote_port, 0);
                }
                Some(addr) => {
                    assert_eq!(
                        v["canonical_remote_ip"].as_str().unwrap(),
                        addr.ip().to_string()
                    );
                    assert_eq!(addr.port(), expected.canonical_remote_port);
                }
            }
            // The constructors reproduce the vectors that came from fixed inputs.
            match name {
                "connect_packet_client" => assert_eq!(expected, ConnectPacket::client(local)),
                "connect_packet_ipv4" | "connect_packet_ipv6" => {
                    let addr = expected.canonical_remote_address().unwrap();
                    assert_eq!(expected, ConnectPacket::server(local, addr));
                }
                "connect_packet_multi_version_client" => {
                    assert_eq!(
                        expected,
                        ConnectPacket::multi_version_client(local, expected.connection_id)
                    );
                    assert!(expected.is_multi_version());
                }
                other => panic!("unexpected vector {other}"),
            }
        }
    }
}

#[test]
fn a_release_rejects_the_other_releases_packets() {
    let v74 = ConnectPacket::client(ProtocolVersion::V7_4);
    let v80 = ConnectPacket::client(ProtocolVersion::V8_0);
    assert_eq!(v74.check(ProtocolVersion::V7_4, None), Ok(()));
    assert_eq!(v80.check(ProtocolVersion::V8_0, None), Ok(()));
    assert!(matches!(
        v74.check(ProtocolVersion::V8_0, None),
        Err(ConnectPacketError::Incompatible { .. })
    ));
    assert!(matches!(
        v80.check(ProtocolVersion::V7_4, None),
        Err(ConnectPacketError::Incompatible { .. })
    ));
}

#[test]
fn negative_vectors_are_rejected() {
    for release in RELEASES {
        let records = records(release, "negative");
        assert!(records.len() >= 2, "{release}");
        for record in records {
            let name = record["name"].as_str().unwrap();
            let bytes = from_hex(record["bytes"].as_str().unwrap());
            assert_eq!(record["cpp_reason"], "serialization_failed", "{name}");
            let err = ConnectPacket::decode(&bytes).expect_err(name);
            assert!(
                matches!(
                    err,
                    DecodeError::UnexpectedEof { .. } | DecodeError::Invalid(_)
                ),
                "{release}/{name}: {err:?}"
            );
        }
    }
}

proptest! {
    #[test]
    fn round_trips(port in any::<u16>(), id in any::<u64>(), ip4 in any::<u32>(),
                   flags in 0u16..2, ip6 in any::<[u8; 16]>(), version in any::<u64>(),
                   later in any::<u64>()) {
        let packet = ConnectPacket {
            protocol_version: ProtocolVersion(version),
            canonical_remote_port: port,
            connection_id: id,
            canonical_remote_ip4: ip4,
            flags,
            canonical_remote_ip6: ip6,
        };
        let mut bytes = packet.encode().unwrap();
        prop_assert_eq!(bytes.len(), ConnectPacket::SIZE);
        prop_assert_eq!(&bytes[..4], &40u32.to_le_bytes());
        bytes.extend_from_slice(&later.to_le_bytes());
        let (back, used) = ConnectPacket::decode(&bytes).unwrap();
        prop_assert_eq!((back, used), (packet, ConnectPacket::SIZE));
    }

    #[test]
    fn random_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..64),
                                local in any::<u64>(), port in proptest::option::of(any::<u16>())) {
        if let Ok((packet, used)) = ConnectPacket::decode(&bytes) {
            prop_assert_eq!(used, ConnectPacket::SIZE);
            let _ = packet.check(ProtocolVersion(local), port);
            let _ = packet.canonical_remote_address();
        }
    }

    #[test]
    fn check_ignores_flags_and_low_bytes(low in 0u64..0x1_0000, flag in any::<bool>()) {
        let mut packet = ConnectPacket::client(ProtocolVersion::V7_4);
        packet.protocol_version = ProtocolVersion(packet.protocol_version.0 + low);
        let local = if flag {
            ProtocolVersion::V7_4.with_object_serializer_flag()
        } else {
            ProtocolVersion::V7_4
        };
        prop_assert_eq!(packet.check(local, None), Ok(()));
    }
}
