//! The domain layer: round trips through each schema set, the 7.4 tenant default, the
//! throttle-limit clock conversion, single-key compression edge cases and the interfaces'
//! adjusted-endpoint indices.

#[macro_use]
mod support;

use std::collections::BTreeMap;

use fdb_native_schema::domain::{
    ClientDbInfo, ClientVersion, Commit, CommitCostEstimation, CommitId, CommitProxy,
    GetKeyServerLocations, GetReadVersion, GetValue, GetValueReply, GrvProxy, Hostname, KeyRange,
    KeyServerLocations, LATEST_VERSION, Mutation, MutationType, OpenDatabaseCoord, ReadOptions,
    ReadVersion, StorageServer, TagThrottleLimits, VersionHistory,
};
use fdb_native_schema::streams::{GRV_PROXY_STREAMS, STORAGE_SERVER_STREAMS};
use fdb_native_schema::{Request, SchemaVersion, SpanContext, TraceFlags, v7_4, v8_0};
use fdb_native_wire::{
    DecodeError, Endpoint, EnsureTable, ErrorOr, FdbError, IdempotencyId, IpAddress,
    NetworkAddress, NetworkAddressList, ProtocolVersion, Tag, TagSet, Uid, VersionVector, decode,
    encode,
};
use support::*;

fn endpoint(token: Uid, port: u16) -> Endpoint {
    Endpoint::new(
        NetworkAddressList {
            address: NetworkAddress::new(IpAddress::V4(0x0a00_0001), port, true, false),
            secondary_address: Some(NetworkAddress::new(
                IpAddress::V6([0xfd, 0xdb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
                port + 1,
                true,
                true,
            )),
        },
        token,
    )
}

fn span() -> SpanContext {
    SpanContext {
        trace_id: Uid::new(11, 12),
        span_id: 13,
        flags: TraceFlags::SAMPLED,
    }
}

fn tags() -> TagSet {
    let mut tags = TagSet::default();
    tags.push(b"tagA".to_vec());
    tags.push(b"tag-b".to_vec());
    tags
}

fn version_vector() -> VersionVector {
    let mut vv = VersionVector::new(2000);
    vv.insert(Tag::new(0, 1), 1500);
    vv.insert(Tag::new(1, 300), 2000);
    vv
}

fn storage_server(n: u64) -> StorageServer {
    StorageServer {
        id: Uid::new(n, n + 1),
        locality: BTreeMap::from([
            (b"zoneid".to_vec(), Some(b"zone-a".to_vec())),
            (b"dcid".to_vec(), None),
        ]),
        base: endpoint(Uid::new(n << 8, n << 40), 4500),
        tss_pair_id: (n == 3).then(|| Uid::new(1, 1)),
        accepting_requests: n != 2,
    }
}

fn client_db_info(schema: SchemaVersion) -> ClientDbInfo {
    ClientDbInfo {
        id: Uid::new(0xdb, 1),
        grv_proxies: vec![GrvProxy {
            process_id: Some(b"grv-1".to_vec()),
            provisional: false,
            base: endpoint(Uid::new(0x1000, 0x2000), 4500),
        }],
        commit_proxies: vec![
            CommitProxy {
                process_id: Some(b"cp-1".to_vec()),
                provisional: true,
                base: endpoint(Uid::new(0x3000, 0x4000), 4500),
                schema,
            },
            CommitProxy {
                process_id: None,
                provisional: false,
                base: endpoint(Uid::new(0x5000, 0x6000), 4502),
                schema,
            },
        ],
        forward: Some(b"desc:id@10.0.0.9:4500".to_vec()),
        history: vec![VersionHistory {
            version: 77,
            mutations: vec![
                Mutation::new(MutationType::SET_VALUE, "\u{ff}/conf/x", "1"),
                Mutation::new(MutationType::CLEAR_RANGE, b"k".to_vec(), b"k\x00".to_vec()),
            ],
        }],
        cluster_id: Uid::new(9, 9),
        cluster_type: 0,
    }
}

fn commit() -> Commit {
    Commit {
        read_conflict_ranges: vec![KeyRange::new("a", "m"), KeyRange::single_key(b"r")],
        write_conflict_ranges: vec![KeyRange::single_key(b"k1"), KeyRange::single_key(b"")],
        mutations: vec![
            Mutation::new(MutationType::SET_VALUE, "k1", "v1"),
            Mutation::new(MutationType::CLEAR_RANGE, b"c".to_vec(), b"c\x00".to_vec()),
            Mutation::new(MutationType::CLEAR_RANGE, "x", "z"),
            Mutation::new(MutationType::ADD_VALUE, "counter", [1, 0, 0, 0]),
        ],
        read_snapshot: 123_456,
        report_conflicting_keys: true,
        lock_aware: true,
        transaction_span_context: Some(span()),
        reply: Uid::new(5, 6),
        flags: Commit::FLAG_IS_LOCK_AWARE | Commit::FLAG_FIRST_IN_BATCH,
        debug_id: Some(Uid::new(7, 8)),
        commit_cost_estimation: Some(CommitCostEstimation {
            ops_count: 4,
            write_costs: 1000,
            clear_idx_costs: vec![(1, 10), (2, 20)],
            expensive_cost_est_count: 1,
        }),
        tag_set: Some(tags()),
        span_context: span(),
        idempotency_id: Some(IdempotencyId::new([0xab; 16]).unwrap()),
    }
}

fn get_value() -> GetValue {
    GetValue {
        key: b"user/key".to_vec(),
        version: 42,
        tags: Some(tags()),
        reply: Uid::new(1, 2),
        span_context: span(),
        options: Some(ReadOptions {
            read_type: 3,
            cache_result: true,
            debug_id: Some(Uid::new(3, 4)),
            consistency_check_start_version: Some(40),
            lock_aware: true,
        }),
        ss_latest_commit_versions: version_vector(),
    }
}

fn get_read_version(schema: SchemaVersion) -> GetReadVersion {
    GetReadVersion {
        transaction_count: 3,
        flags: GetReadVersion::PRIORITY_DEFAULT | GetReadVersion::FLAG_CAUSAL_READ_RISKY,
        tags: BTreeMap::from([(b"tagA".to_vec(), 2), (b"tagB".to_vec(), 1)]),
        debug_id: Some(Uid::new(1, 1)),
        reply: Uid::new(2, 2),
        span_context: span(),
        max_version: 99,
        // Only 8.0 has it.
        max_grv_queue_delay_ms: (schema == SchemaVersion::V8_0).then_some(250),
    }
}

fn read_version(schema: SchemaVersion) -> ReadVersion {
    ReadVersion {
        process_busy_time: 125_000,
        version: 5_000_000_000_123,
        locked: true,
        metadata_version: Some(b"\x00\x01".to_vec()),
        tag_throttle_info: BTreeMap::from([(
            b"hot".to_vec(),
            TagThrottleLimits {
                tps_rate: 50.0,
                expiration: 1030.5,
            },
        )]),
        mid_shard_size: 1 << 20,
        rk_default_throttled: true,
        rk_batch_throttled: false,
        ss_version_vector_delta: version_vector(),
        proxy_id: Uid::new(4, 4),
        // Only 7.4 has it.
        proxy_tag_throttled_duration: if schema == SchemaVersion::V7_4 {
            0.25
        } else {
            0.0
        },
    }
}

fn open_database() -> OpenDatabaseCoord {
    OpenDatabaseCoord {
        issues: vec![b"incompatible_connection".to_vec()],
        supported_versions: vec![ClientVersion {
            client_version: b"7.4.10".to_vec(),
            source_version: b"abc".to_vec(),
            protocol_version: b"fdb00b074000000".to_vec(),
        }],
        trace_log_group: b"group".to_vec(),
        known_client_info_id: Uid::new(1, 9),
        cluster_key: b"desc:id".to_vec(),
        coordinators: vec![NetworkAddress::new(
            IpAddress::V4(0x7f00_0001),
            4500,
            true,
            false,
        )],
        reply: Uid::new(8, 8),
        hostnames: vec![Hostname {
            host: b"coord.example".to_vec(),
            service: b"4500".to_vec(),
            is_tls: true,
        }],
        internal: false,
    }
}

const NOW: f64 = 1000.0;

/// Request → bytes → request and reply → bytes → reply, through the [`Request`] dispatch.
/// (Replies may borrow from their bytes, so they are compared by their `Debug` form, which
/// shows every field.)
fn round_trip<R>(version: SchemaVersion, request: &R, reply: &ErrorOr<R::Reply<'_>>)
where
    R: Request + PartialEq + std::fmt::Debug,
    for<'a> R::Reply<'a>: std::fmt::Debug,
{
    let bytes = request.encode_request(version).unwrap();
    assert_eq!(
        R::decode_request(version, &bytes).as_ref(),
        Ok(request),
        "{version}"
    );
    let bytes = R::encode_reply(version, reply, NOW).unwrap();
    let back = R::decode_reply(version, &bytes, NOW).unwrap();
    assert_eq!(format!("{back:?}"), format!("{reply:?}"), "{version}");
    // An error reply is the error envelope.
    let error: ErrorOr<R::Reply<'_>> = Err(FdbError::new(1020));
    let bytes = R::encode_reply(version, &error, NOW).unwrap();
    let back = R::decode_reply(version, &bytes, NOW).unwrap();
    assert_eq!(back.err(), Some(FdbError::new(1020)), "{version}");
}

#[test]
fn domain_messages_round_trip_through_each_schema_set() {
    for version in SchemaVersion::ALL {
        round_trip(version, &open_database(), &Ok(client_db_info(version)));
        round_trip(
            version,
            &get_read_version(version),
            &Ok(read_version(version)),
        );
        round_trip(
            version,
            &GetKeyServerLocations {
                begin: b"loc/a".to_vec(),
                end: Some(b"loc/z".to_vec()),
                limit: 100,
                reverse: true,
                reply: Uid::new(3, 3),
                span_context: span(),
                min_tenant_version: LATEST_VERSION,
            },
            &Ok(KeyServerLocations {
                results: vec![
                    (
                        KeyRange::new("", "m"),
                        vec![storage_server(1), storage_server(2)],
                    ),
                    (KeyRange::single_key(b"m"), vec![storage_server(3)]),
                ],
                tss_mapping: vec![(Uid::new(3, 4), storage_server(4))],
                tag_mapping: vec![
                    (Uid::new(1, 2), Tag::new(0, 1)),
                    (Uid::new(2, 3), Tag::new(-2, 300)),
                ],
            }),
        );
        round_trip(
            version,
            &get_value(),
            &Ok(GetValueReply {
                penalty: 1.5,
                error: None,
                value: Some(b"value"),
                cached: true,
            }),
        );
        round_trip(
            version,
            &get_value(),
            &Ok(GetValueReply {
                error: Some(FdbError::new(1037)),
                ..GetValueReply::default()
            }),
        );
        round_trip(
            version,
            &commit(),
            &Ok(CommitId {
                version: 1234,
                txn_batch_id: 7,
                metadata_version: Some(b"mv".to_vec()),
                conflicting_kr_indices: Some(vec![0, 3]),
            }),
        );
        round_trip(version, &Commit::default(), &Ok(CommitId::default()));
    }
}

/// On 7.4 every request with a `TenantInfo` carries the tenant-less one, `tenantId = -1`
/// with no token; 8.0's requests have none.
#[test]
fn a_7_4_request_carries_the_default_tenant() {
    let v = SchemaVersion::V7_4;
    let bytes = get_value().encode_request(v).unwrap();
    let wire: v7_4::GetValueRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(wire.tenant_info.tenant_id, -1);
    assert_eq!(wire.tenant_info.token, None);

    let bytes = commit().encode_request(v).unwrap();
    let wire: v7_4::CommitTransactionRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant_info, v7_4::TenantInfo::NONE);

    let request = GetKeyServerLocations {
        begin: b"a".to_vec(),
        end: None,
        limit: 1,
        reverse: false,
        reply: Uid::new(1, 1),
        span_context: SpanContext::default(),
        min_tenant_version: LATEST_VERSION,
    };
    let bytes = request.encode_request(v).unwrap();
    let wire: v7_4::GetKeyServerLocationsRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant, v7_4::TenantInfo::NONE);
    assert_eq!(wire.min_tenant_version, LATEST_VERSION);

    // An absent TenantInfo would read as tenant 0, which is why it is always written.
    assert_eq!(
        <v7_4::TenantInfo as fdb_native_wire::Decode>::absent().tenant_id,
        0
    );
}

/// The 7.4 fixture's request has a tenant with a token; the domain drops it, and encoding
/// the domain request again writes the default tenant.
#[test]
fn decoding_a_7_4_request_drops_its_tenant() {
    let bytes = record_bytes(&fixture("release-7.4", "get_value_request"));
    let wire: v7_4::GetValueRequest = decode(&bytes).unwrap();
    assert_ne!(wire.tenant_info.tenant_id, -1);
    assert!(wire.tenant_info.token.is_some());
    let request = GetValue::decode_request(SchemaVersion::V7_4, &bytes).unwrap();
    let encoded = request.encode_request(SchemaVersion::V7_4).unwrap();
    let again: v7_4::GetValueRequest = decode(&encoded).unwrap();
    assert_eq!(again.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(again.key, wire.key);
    assert_eq!(
        again.options.map(|o| o.read_type),
        wire.options.map(|o| o.read_type)
    );
}

/// `ClientTagThrottleLimits` travels as a duration; the domain has an expiration on the local
/// clock, `now + duration` when received and `expiration - now` when sent.
#[test]
fn throttle_limits_convert_with_the_given_clock() {
    for (release, version) in [
        ("release-7.4", SchemaVersion::V7_4),
        ("release-8.0", SchemaVersion::V8_0),
    ] {
        let bytes = record_bytes(&fixture(release, "get_read_version_reply_envelope"));
        let at_1000 = GetReadVersion::decode_reply(version, &bytes, 1000.0)
            .unwrap()
            .unwrap();
        let at_5000 = GetReadVersion::decode_reply(version, &bytes, 5000.0)
            .unwrap()
            .unwrap();
        // The fixture's duration is exactly 30 (the generator's clock does not advance).
        let (tag, limits) = at_1000.tag_throttle_info.iter().next().unwrap();
        assert_eq!(limits.expiration, 1030.0, "{release}");
        assert_eq!(
            at_5000.tag_throttle_info[tag].expiration, 5030.0,
            "{release}"
        );
        assert_eq!(at_5000.tag_throttle_info[tag].tps_rate, limits.tps_rate);

        // Sent at 1010, 20 seconds remain.
        let bytes = GetReadVersion::encode_reply(version, &Ok(at_1000), 1010.0).unwrap();
        let durations: Vec<f64> = match version {
            SchemaVersion::V7_4 => {
                let r: ErrorOr<EnsureTable<v7_4::GetReadVersionReply>> = decode(&bytes).unwrap();
                r.unwrap()
                    .0
                    .tag_throttle_info
                    .values()
                    .map(|l| l.duration)
                    .collect()
            }
            SchemaVersion::V8_0 => {
                let r: ErrorOr<EnsureTable<v8_0::GetReadVersionReply>> = decode(&bytes).unwrap();
                r.unwrap()
                    .0
                    .tag_throttle_info
                    .values()
                    .map(|l| l.duration)
                    .collect()
            }
        };
        assert_eq!(durations, [20.0], "{release}");
    }
}

#[test]
fn version_specific_fields() {
    // 8.0's maxGrvQueueDelayMS is not sent to 7.4.
    let request = get_read_version(SchemaVersion::V8_0);
    let back = GetReadVersion::decode_request(
        SchemaVersion::V7_4,
        &request.encode_request(SchemaVersion::V7_4).unwrap(),
    )
    .unwrap();
    assert_eq!(back.max_grv_queue_delay_ms, None);
    // 7.4's proxyTagThrottledDuration is not sent to 8.0.
    let reply = read_version(SchemaVersion::V7_4);
    let bytes = GetReadVersion::encode_reply(SchemaVersion::V8_0, &Ok(reply), NOW).unwrap();
    let back = GetReadVersion::decode_reply(SchemaVersion::V8_0, &bytes, NOW)
        .unwrap()
        .unwrap();
    assert_eq!(back.proxy_tag_throttled_duration, 0.0);
    // ReadOptions::type is i32 on 7.4 and u32 on 8.0: the bits are kept.
    let mut request = get_value();
    request.options.as_mut().unwrap().read_type = u32::MAX;
    for v in SchemaVersion::ALL {
        let back = GetValue::decode_request(v, &request.encode_request(v).unwrap()).unwrap();
        assert_eq!(back, request, "{v}");
    }
}

#[test]
fn schema_version_from_protocol_version() {
    use SchemaVersion::*;
    let cases = [
        (ProtocolVersion::V7_4, Some(V7_4)),
        (
            ProtocolVersion::V7_4.with_object_serializer_flag(),
            Some(V7_4),
        ),
        (ProtocolVersion(0x0FDB_00B0_7400_FFFF), Some(V7_4)),
        (ProtocolVersion::V8_0, Some(V8_0)),
        (
            ProtocolVersion(0x0FDB_00B0_8000_0001).with_object_serializer_flag(),
            Some(V8_0),
        ),
        (ProtocolVersion(0x0FDB_00B0_7300_0000), None),
        (ProtocolVersion(0x0FDB_00B0_7401_0000), None),
        (ProtocolVersion(0x0FDB_00B0_8100_0000), None),
    ];
    for (pv, want) in cases {
        assert_eq!(SchemaVersion::for_protocol_version(pv), want, "{pv}");
    }
    for v in SchemaVersion::ALL {
        assert_eq!(
            SchemaVersion::for_protocol_version(v.protocol_version()),
            Some(v)
        );
    }
}

// ---------------------------------------------------------------------------------------
// Single-key compression.

/// `(begin, end)` as the encoder writes them.
fn written_range(begin: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let (b, e) = v7_4::KeyRangeRef::new(begin, end).wire_form();
    (b.to_vec(), e.to_vec())
}

/// The raw strings of a `KeyRangeRef` table, read with a type that does not expand.
#[derive(fdb_native_wire::Wire, Debug, PartialEq)]
#[fdb(file_identifier = 1)]
struct RawPair<'a> {
    first: &'a [u8],
    second: &'a [u8],
}

#[test]
fn the_encoder_writes_the_compressed_form() {
    let ranges = vec![
        v7_4::KeyRangeRef::new(b"k", b"k\x00"),
        v7_4::KeyRangeRef::new(b"a", b"b"),
    ];
    let bytes = fdb_native_wire::encode_with_identifier(&ranges, 1).unwrap();
    let reader = fdb_native_wire::Reader::new(&bytes).unwrap();
    let raw: Vec<RawPair> = reader.decode_root().unwrap();
    assert_eq!(
        raw,
        [
            RawPair {
                first: b"k\x00",
                second: b""
            },
            RawPair {
                first: b"a",
                second: b"b"
            },
        ]
    );
}

#[test]
fn key_range_single_key_compression() {
    // [k, k\0) is written (k\0, "") and read back.
    assert_eq!(written_range(b"k", b"k\x00"), (b"k\x00".to_vec(), vec![]));
    // The empty key: ["", "\0") is written ("\0", "").
    assert_eq!(written_range(b"", b"\x00"), (b"\x00".to_vec(), vec![]));
    // Not single-key ranges: written as they are.
    for (b, e) in [
        (&b"k"[..], &b"k\x00\x00"[..]),
        (b"k", b"l"),
        (b"k", b"k\x01"),
        (b"k\x00", b"k\x00"),
        (b"", b""),
        (b"a", b""),
    ] {
        assert_eq!(written_range(b, e), (b.to_vec(), e.to_vec()));
    }

    // Through a message: every range survives encode and decode.
    let ranges = [
        KeyRange::single_key(b"k"),
        KeyRange::single_key(b""),
        KeyRange::single_key(b"\x00"),
        KeyRange::new("k", b"k\x00\x00".to_vec()),
        KeyRange::new("", ""),
        KeyRange::new("a", "b"),
    ];
    let commit = Commit {
        read_conflict_ranges: ranges.to_vec(),
        ..Commit::default()
    };
    for v in SchemaVersion::ALL {
        let back = Commit::decode_request(v, &commit.encode_request(v).unwrap()).unwrap();
        assert_eq!(back.read_conflict_ranges, ranges, "{v}");
    }

    // The reader's rules.
    let read = v7_4::KeyRangeRef::from_wire_form;
    assert_eq!(
        read(b"k\x00", b""),
        Ok(v7_4::KeyRangeRef::new(b"k", b"k\x00"))
    );
    assert_eq!(read(b"\x00", b""), Ok(v7_4::KeyRangeRef::new(b"", b"\x00")));
    assert_eq!(read(b"", b""), Ok(v7_4::KeyRangeRef::new(b"", b"")));
    // A compressed begin must end in \0 (C++ ASSERTs).
    assert!(matches!(read(b"k", b""), Err(DecodeError::Invalid(_))));
    // An inverted range is rejected (C++ throws inverted_range).
    assert!(matches!(read(b"b", b"a"), Err(DecodeError::Invalid(_))));
}

#[test]
fn mutation_single_key_compression() {
    let written = |kind: u8, p1: &[u8], p2: &[u8]| {
        let (a, b) = v7_4::MutationRef::new(kind, p1, p2).wire_form();
        (a.to_vec(), b.to_vec())
    };
    // Only a ClearRange of one key is compressed.
    assert_eq!(written(1, b"k", b"k\x00"), (b"k\x00".to_vec(), vec![]));
    assert_eq!(written(1, b"", b"\x00"), (b"\x00".to_vec(), vec![]));
    assert_eq!(
        written(0, b"k", b"k\x00"),
        (b"k".to_vec(), b"k\x00".to_vec())
    );
    assert_eq!(
        written(2, b"k", b"k\x00"),
        (b"k".to_vec(), b"k\x00".to_vec())
    );
    assert_eq!(written(1, b"k", b"l"), (b"k".to_vec(), b"l".to_vec()));

    let read = v7_4::MutationRef::from_wire_form;
    assert_eq!(
        read(1, b"k\x00", b""),
        Ok(v7_4::MutationRef::new(1, b"k", b"k\x00"))
    );
    // A SetValue with an empty value is not a compressed clear.
    assert_eq!(
        read(0, b"k\x00", b""),
        Ok(v7_4::MutationRef::new(0, b"k\x00", b""))
    );
    // An empty clear stays empty.
    assert_eq!(read(1, b"", b""), Ok(v7_4::MutationRef::new(1, b"", b"")));
    // A compressed param1 must end in \0.
    assert!(matches!(read(1, b"k", b""), Err(DecodeError::Invalid(_))));
    // A checksummed type (bit 7) is not interpreted: no expansion, kept as received.
    assert_eq!(
        read(0x81, b"k\x00", b"\x01\x02\x03\x04"),
        Ok(v7_4::MutationRef::new(0x81, b"k\x00", b"\x01\x02\x03\x04"))
    );

    // Through a message.
    let mutations = vec![
        Mutation::new(MutationType::CLEAR_RANGE, b"k".to_vec(), b"k\x00".to_vec()),
        Mutation::new(MutationType::CLEAR_RANGE, b"".to_vec(), b"\x00".to_vec()),
        Mutation::new(MutationType::CLEAR_RANGE, "", ""),
        Mutation::new(MutationType::SET_VALUE, b"k\x00".to_vec(), b"".to_vec()),
        Mutation::new(MutationType::SET_VALUE, b"k".to_vec(), b"k\x00".to_vec()),
    ];
    let commit = Commit {
        mutations: mutations.clone(),
        ..Commit::default()
    };
    for v in SchemaVersion::ALL {
        let back = Commit::decode_request(v, &commit.encode_request(v).unwrap()).unwrap();
        assert_eq!(back.mutations, mutations, "{v}");
    }
}

/// The C++ writer's single-key forms in the fixtures decode to expanded values.
#[test]
fn fixtures_single_key_forms_expand() {
    for (release, v) in [
        ("release-7.4", SchemaVersion::V7_4),
        ("release-8.0", SchemaVersion::V8_0),
    ] {
        let bytes = record_bytes(&fixture(
            release,
            "commit_transaction_request_single_key_clear",
        ));
        let commit = Commit::decode_request(v, &bytes).unwrap();
        assert_eq!(
            commit.write_conflict_ranges,
            [KeyRange::single_key(b"single")]
        );
        assert_eq!(
            commit.mutations,
            [Mutation::new(
                MutationType::CLEAR_RANGE,
                b"single".to_vec(),
                b"single\x00".to_vec()
            )]
        );
    }
}

// ---------------------------------------------------------------------------------------
// Endpoints.

/// `CommitProxyInterface::serialize` on each branch: the `getAdjustedEndpoint(i)` of every
/// stream, by C++ name.
#[test]
fn commit_proxy_indices_match_the_headers() {
    let v74 = v7_4::COMMIT_PROXY_STREAMS;
    let v80 = v8_0::COMMIT_PROXY_STREAMS;
    let table: [(&str, Option<u32>, Option<u32>); 14] = [
        ("commit", Some(v74.commit), Some(v80.commit)),
        (
            "getConsistentReadVersion",
            Some(v74.get_consistent_read_version),
            Some(v80.get_consistent_read_version),
        ),
        (
            "getKeyServersLocations",
            Some(v74.get_key_servers_locations),
            Some(v80.get_key_servers_locations),
        ),
        (
            "getStorageServerRejoinInfo",
            Some(v74.get_storage_server_rejoin_info),
            Some(v80.get_storage_server_rejoin_info),
        ),
        (
            "waitFailure",
            Some(v74.wait_failure),
            Some(v80.wait_failure),
        ),
        ("txnState", Some(v74.txn_state), Some(v80.txn_state)),
        (
            "getHealthMetrics",
            v74.get_health_metrics,
            v80.get_health_metrics,
        ),
        (
            "proxySnapReq",
            Some(v74.proxy_snap_req),
            Some(v80.proxy_snap_req),
        ),
        (
            "exclusionSafetyCheckReq",
            Some(v74.exclusion_safety_check_req),
            Some(v80.exclusion_safety_check_req),
        ),
        (
            "getDDMetrics",
            Some(v74.get_dd_metrics),
            Some(v80.get_dd_metrics),
        ),
        (
            "expireIdempotencyId",
            Some(v74.expire_idempotency_id),
            Some(v80.expire_idempotency_id),
        ),
        ("getTenantId", v74.get_tenant_id, v80.get_tenant_id),
        (
            "getBlobGranuleLocations",
            v74.get_blob_granule_locations,
            v80.get_blob_granule_locations,
        ),
        (
            "setThrottledShard",
            Some(v74.set_throttled_shard),
            Some(v80.set_throttled_shard),
        ),
    ];
    // From release-7.4 and release-8.0 fdbclient/include/fdbclient/CommitProxyInterface.h.
    let headers: [(&str, Option<u32>, Option<u32>); 14] = [
        ("commit", Some(0), Some(0)),
        ("getConsistentReadVersion", Some(1), Some(1)),
        ("getKeyServersLocations", Some(2), Some(2)),
        ("getStorageServerRejoinInfo", Some(3), Some(3)),
        ("waitFailure", Some(4), Some(4)),
        ("txnState", Some(5), Some(5)),
        ("getHealthMetrics", Some(6), None),
        ("proxySnapReq", Some(7), Some(6)),
        ("exclusionSafetyCheckReq", Some(8), Some(7)),
        ("getDDMetrics", Some(9), Some(8)),
        ("expireIdempotencyId", Some(10), Some(9)),
        ("getTenantId", Some(11), None),
        ("getBlobGranuleLocations", Some(12), None),
        ("setThrottledShard", Some(13), Some(10)),
    ];
    assert_eq!(table, headers);
    // kNumAdjustedEndpoints: 13 on 7.4, 10 on 8.0.
    assert_eq!(table.iter().filter_map(|r| r.1).max(), Some(13));
    assert_eq!(table.iter().filter_map(|r| r.2).max(), Some(10));
    assert_eq!(SchemaVersion::V7_4.commit_proxy_streams(), v74);
    assert_eq!(SchemaVersion::V8_0.commit_proxy_streams(), v80);
}

#[test]
fn grv_proxy_and_storage_server_indices_match_the_headers() {
    // GrvProxyInterface.h, identical on both branches (kNumAdjustedEndpoints = 3).
    let g = GRV_PROXY_STREAMS;
    assert_eq!(
        [
            g.get_consistent_read_version,
            g.wait_failure,
            g.get_health_metrics,
            g.refresh_global_config
        ],
        [0, 1, 2, 3]
    );
    // StorageServerInterface.h (7.4) / StorageServerInterface.cpp (8.0), identical
    // (kNumAdjustedEndpoints = 26).
    let s = STORAGE_SERVER_STREAMS;
    let all = [
        s.get_value,
        s.get_key,
        s.get_key_values,
        s.get_shard_state,
        s.wait_metrics,
        s.split_metrics,
        s.get_storage_metrics,
        s.wait_failure,
        s.get_queuing_metrics,
        s.get_key_value_store_type,
        s.watch_value,
        s.get_read_hot_ranges,
        s.get_range_split_points,
        s.get_key_values_stream,
        s.get_mapped_key_values,
        s.change_feed_stream,
        s.overlapping_change_feeds,
        s.change_feed_pop,
        s.change_feed_version_update,
        s.checkpoint,
        s.fetch_checkpoint,
        s.fetch_checkpoint_key_values,
        s.update_commit_cost_request,
        s.audit_storage,
        s.get_hot_shards,
        s.get_check_sum,
        s.bulkdump,
    ];
    assert_eq!(all, core::array::from_fn::<u32, 27, _>(|i| i as u32));
    assert_eq!((s.get_key, s.get_key_values, s.watch_value), (1, 2, 10));
}

/// `getAdjustedEndpoint(i)`: token (first + (i << 32), (second & 0xffffffff00000000) |
/// u32(second + i)), same addresses.
fn adjusted_token(base: Uid, i: u32) -> Uid {
    Uid::new(
        base.first.wrapping_add(u64::from(i) << 32),
        (base.second & 0xffff_ffff_0000_0000) | u64::from((base.second as u32).wrapping_add(i)),
    )
}

#[test]
fn domain_interfaces_compute_adjusted_endpoints() {
    for (release, v) in [
        ("release-7.4", SchemaVersion::V7_4),
        ("release-8.0", SchemaVersion::V8_0),
    ] {
        let bytes = record_bytes(&fixture(release, "client_db_info_envelope"));
        let info = OpenDatabaseCoord::decode_reply(v, &bytes, NOW)
            .unwrap()
            .unwrap();
        assert_eq!(info.commit_proxies.len(), 2);
        for proxy in &info.commit_proxies {
            assert_eq!(proxy.schema, v);
            let e = proxy.endpoints();
            assert_eq!(e.commit, proxy.base);
            assert_eq!(
                e.get_key_servers_locations.token,
                adjusted_token(proxy.base.token, 2)
            );
            assert_eq!(e.get_key_servers_locations.addresses, proxy.base.addresses);
            let expire = if v == SchemaVersion::V7_4 { 10 } else { 9 };
            assert_eq!(
                e.expire_idempotency_id.token,
                adjusted_token(proxy.base.token, expire)
            );
            assert_eq!(e.get_tenant_id.is_some(), v == SchemaVersion::V7_4);
            assert_eq!(proxy.id(), proxy.base.token);
        }
        for proxy in &info.grv_proxies {
            let e = proxy.endpoints();
            assert_eq!(e.get_consistent_read_version, proxy.base);
            assert_eq!(
                e.get_health_metrics.token,
                adjusted_token(proxy.base.token, 2)
            );
        }

        let bytes = record_bytes(&fixture(release, "get_key_server_locations_reply_envelope"));
        let locations = GetKeyServerLocations::decode_reply(v, &bytes, NOW)
            .unwrap()
            .unwrap();
        assert!(!locations.results.is_empty());
        for (_, servers) in &locations.results {
            for server in servers {
                let e = server.endpoints();
                assert_eq!(e.get_value, server.base);
                assert_eq!(e.get_key.token, adjusted_token(server.base.token, 1));
                assert_eq!(e.get_key_values.token, adjusted_token(server.base.token, 2));
                assert_eq!(e.watch_value.token, adjusted_token(server.base.token, 10));
            }
        }
    }
    // Wrapping of the low 32 bits of `second`.
    let server = StorageServer {
        base: endpoint(Uid::new(1, 0xaaaa_bbbb_ffff_fffe), 1),
        ..storage_server(1)
    };
    assert_eq!(
        server.endpoints().get_key_values.token,
        Uid::new(1 + (2 << 32), 0xaaaa_bbbb_0000_0000)
    );
}

/// A decoded `GetKeyServerLocationsReply` reads its shards lazily; a shard that does not
/// decode is an error of the conversion.
#[test]
fn key_server_locations_reply_is_read_through_a_view() {
    let bytes = record_bytes(&fixture("release-8.0", "get_key_server_locations_reply"));
    let reply: v8_0::GetKeyServerLocationsReply = decode(&bytes).unwrap();
    assert_eq!(reply.results.len(), reply.results.iter().count());
    let locations = KeyServerLocations::try_from(reply.clone()).unwrap();
    assert_eq!(locations.results.len(), reply.results.len());
    // Re-encoding the view writes the same shards.
    let encoded = encode(&reply).unwrap();
    let again: v8_0::GetKeyServerLocationsReply = decode(&encoded).unwrap();
    assert_eq!(KeyServerLocations::try_from(again).unwrap(), locations);
}
