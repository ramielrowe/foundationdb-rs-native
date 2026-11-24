//! The domain layer: round trips through each schema set, reply tokens, the 7.4 tenant
//! default, throttle-limit durations, single-key compression and inverted ranges, mutation
//! checksums, and the interfaces' adjusted-endpoint indices.

#[macro_use]
mod support;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use fdb_native_schema::domain::{
    ClientDbInfo, ClientVersion, Commit, CommitCostEstimation, CommitId, CommitProxy, GetKey,
    GetKeyReply, GetKeyServerLocations, GetKeyValues, GetKeyValuesReply, GetReadVersion, GetValue,
    GetValueReply, GrvProxy, Hostname, INVALID_VERSION, InvalidValue, KeyRange, KeySelector,
    KeyServerLocations, LATEST_VERSION, Mutation, MutationType, OpenDatabaseCoord, ReadOptions,
    ReadVersion, SplitRange, SplitRangeReply, StorageMetrics, StorageServer, TagThrottleLimits,
    TenantMode, VersionHistory, WaitMetrics, WatchValue, WatchValueReply,
};
use fdb_native_schema::streams::{GRV_PROXY_STREAMS, STORAGE_SERVER_STREAMS};
use fdb_native_schema::{
    Request, SchemaVersion, SpanContext, TraceFlags, encode_request, encode_request_into, v7_4,
    v8_0,
};
use fdb_native_wire::{
    Decode, DecodeError, EncodeError, Endpoint, EnsureTable, ErrorOr, FdbError, IdempotencyId,
    IpAddress, KeyValueRef, KeyValueVector, KeyValueVectorRef, NetworkAddress, NetworkAddressList,
    ProtocolVersion, Tag, TagSet, Uid, VersionVector, decode, encode,
};
use support::*;

const TOKEN: Uid = Uid::new(0x7070, 0x8080);

fn kr(begin: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> KeyRange {
    KeyRange::new(begin, end).unwrap()
}

fn mu(kind: MutationType, p1: impl Into<Vec<u8>>, p2: impl Into<Vec<u8>>) -> Mutation {
    Mutation::new(kind, p1, p2).unwrap()
}

/// Encodes `request` with [`TOKEN`] and decodes it back, checking the token.
fn request_round_trip<R: Request + std::fmt::Debug>(version: SchemaVersion, request: &R) -> R {
    let bytes = request.encode_request(version, TOKEN).unwrap();
    let (back, token) = R::decode_request(version, &bytes).unwrap();
    assert_eq!(token, TOKEN);
    back
}

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
                mu(MutationType::SET_VALUE, "\u{ff}/conf/x", "1"),
                mu(MutationType::CLEAR_RANGE, b"k".to_vec(), b"k\x00".to_vec()),
            ],
        }],
        cluster_id: Uid::new(9, 9),
        cluster_type: 0,
        // Only 7.4 has it.
        tenant_mode: (schema == SchemaVersion::V7_4).then_some(TenantMode::REQUIRED),
    }
}

fn commit() -> Commit {
    Commit {
        read_conflict_ranges: vec![kr("a", "m"), KeyRange::single_key(b"r")],
        write_conflict_ranges: vec![KeyRange::single_key(b"k1"), KeyRange::single_key(b"")],
        mutations: vec![
            mu(MutationType::SET_VALUE, "k1", "v1"),
            mu(MutationType::CLEAR_RANGE, b"c".to_vec(), b"c\x00".to_vec()),
            mu(MutationType::CLEAR_RANGE, "x", "z"),
            mu(MutationType::ADD_VALUE, "counter", [1, 0, 0, 0]),
        ],
        read_snapshot: 123_456,
        report_conflicting_keys: true,
        lock_aware: true,
        transaction_span_context: Some(span()),
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

fn read_options() -> ReadOptions {
    ReadOptions {
        read_type: 3,
        cache_result: true,
        debug_id: Some(Uid::new(3, 4)),
        consistency_check_start_version: Some(40),
        lock_aware: true,
    }
}

fn get_key() -> GetKey {
    GetKey {
        sel: KeySelector::new(b"sel/key".to_vec(), true, -4),
        version: 42,
        tags: Some(tags()),
        span_context: span(),
        options: Some(read_options()),
        ss_latest_commit_versions: version_vector(),
    }
}

fn get_key_values(schema: SchemaVersion) -> GetKeyValues {
    GetKeyValues {
        begin: KeySelector::first_greater_or_equal(b"range/a".to_vec()),
        end: KeySelector::last_less_or_equal(b"range/z".to_vec()),
        version: 42,
        limit: -100,
        limit_bytes: 80_000,
        tags: Some(tags()),
        span_context: span(),
        options: Some(read_options()),
        ss_latest_commit_versions: version_vector(),
        // Only 8.0 has it.
        task_id: (schema == SchemaVersion::V8_0).then_some(2500),
    }
}

fn watch_value() -> WatchValue {
    WatchValue {
        key: b"watched".to_vec(),
        value: Some(b"old".to_vec()),
        version: 42,
        tags: Some(tags()),
        debug_id: Some(Uid::new(5, 6)),
        span_context: span(),
    }
}

/// Rows of a `GetKeyValuesReply`: empty keys and values, binary bytes, and a long value.
fn kv_vector() -> KeyValueVector {
    KeyValueVector::from_pairs([
        KeyValueRef::new(b"range/b", b"value-b"),
        KeyValueRef::new(b"range/c", b""),
        KeyValueRef::new(b"", b"empty key"),
        KeyValueRef::new(b"range/d\xff", &[0, 1, 0xff]),
        KeyValueRef::new(b"range/e", &[7; 300]),
    ])
    .unwrap()
}

fn get_key_server_locations() -> GetKeyServerLocations {
    GetKeyServerLocations {
        begin: b"loc/a".to_vec(),
        end: Some(b"loc/z".to_vec()),
        limit: 100,
        reverse: true,
        span_context: span(),
        min_tenant_version: LATEST_VERSION,
    }
}

fn get_read_version(schema: SchemaVersion) -> GetReadVersion {
    GetReadVersion {
        transaction_count: 3,
        flags: GetReadVersion::PRIORITY_DEFAULT | GetReadVersion::FLAG_CAUSAL_READ_RISKY,
        tags: BTreeMap::from([(b"tagA".to_vec(), 2), (b"tagB".to_vec(), 1)]),
        debug_id: Some(Uid::new(1, 1)),
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
        tag_throttle_info: BTreeMap::from([
            (
                b"hot".to_vec(),
                TagThrottleLimits {
                    tps_rate: 50.0,
                    duration: 30.5,
                },
            ),
            (
                b"forever".to_vec(),
                TagThrottleLimits {
                    tps_rate: 1.0,
                    duration: f64::MAX,
                },
            ),
        ]),
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
        hostnames: vec![Hostname {
            host: b"coord.example".to_vec(),
            service: b"4500".to_vec(),
            is_tls: true,
        }],
        internal: false,
    }
}

/// Request → bytes → request (with its token) and reply → bytes → reply, through the
/// [`Request`] dispatch. (Replies may borrow from their bytes, so they are compared by their
/// `Debug` form, which shows every field.)
fn round_trip<R>(version: SchemaVersion, request: &R, reply: &ErrorOr<R::Reply<'_>>)
where
    R: Request + PartialEq + std::fmt::Debug,
    for<'a> R::Reply<'a>: std::fmt::Debug,
{
    assert_eq!(&request_round_trip(version, request), request, "{version}");
    let bytes = R::encode_reply(version, reply).unwrap();
    let back = R::decode_reply(version, &bytes).unwrap();
    assert_eq!(format!("{back:?}"), format!("{reply:?}"), "{version}");
    // An error reply is the error envelope.
    let error: ErrorOr<R::Reply<'_>> = Err(FdbError::new(1020));
    let bytes = R::encode_reply(version, &error).unwrap();
    let back = R::decode_reply(version, &bytes).unwrap();
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
            &get_key_server_locations(),
            &Ok(KeyServerLocations {
                results: vec![
                    (kr("", "m"), vec![storage_server(1), storage_server(2)]),
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
            &get_key(),
            &Ok(GetKeyReply {
                penalty: 2.5,
                error: None,
                sel: KeySelector::new(b"sel/result".to_vec(), false, 1).borrowed(),
                cached: true,
            }),
        );
        round_trip(
            version,
            &get_key(),
            &Ok(GetKeyReply {
                error: Some(FdbError::new(1037)),
                ..GetKeyReply::default()
            }),
        );
        let rows = kv_vector();
        round_trip(
            version,
            &get_key_values(version),
            &Ok(GetKeyValuesReply {
                penalty: 1.25,
                error: None,
                data: rows.borrowed(),
                version: 1234,
                more: true,
                cached: true,
            }),
        );
        round_trip(
            version,
            &get_key_values(version),
            &Ok(GetKeyValuesReply::default()),
        );
        round_trip(
            version,
            &watch_value(),
            &Ok(WatchValueReply {
                version: 5678,
                cached: true,
            }),
        );
        round_trip(
            version,
            &WatchValue {
                value: None,
                tags: None,
                debug_id: None,
                ..watch_value()
            },
            &Ok(WatchValueReply::default()),
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

/// The reply token is a parameter: the same request is encoded with each token it is sent
/// with, and `encode_request_into` appends exactly what `encode_request` returns.
#[test]
fn reply_tokens_and_encode_into() {
    let request = commit();
    for v in SchemaVersion::ALL {
        let a = request.encode_request(v, Uid::new(1, 1)).unwrap();
        let b = encode_request(v, &request, Uid::new(2, 2)).unwrap();
        assert_ne!(a, b);
        assert_eq!(Commit::decode_request(v, &a).unwrap().1, Uid::new(1, 1));
        assert_eq!(
            Commit::decode_request(v, &b).unwrap(),
            (request.clone(), Uid::new(2, 2))
        );

        let mut frame = b"header".to_vec();
        encode_request_into(v, &request, Uid::new(1, 1), &mut frame).unwrap();
        assert_eq!(&frame[..6], b"header");
        assert_eq!(&frame[6..], &a[..]);

        // On an error the buffer is left as it was.
        let mut bad = request.clone();
        bad.read_conflict_ranges.push(KeyRange {
            begin: b"b".to_vec(),
            end: b"a".to_vec(),
        });
        let mut frame = b"header".to_vec();
        assert!(bad.encode_request_into(v, TOKEN, &mut frame).is_err());
        assert_eq!(frame, b"header");
    }
}

/// On 7.4 every request with a `TenantInfo` carries the tenant-less one, `tenantId = -1`
/// with no token; 8.0's requests have none.
#[test]
fn a_7_4_request_carries_the_default_tenant() {
    let v = SchemaVersion::V7_4;
    let bytes = get_value().encode_request(v, TOKEN).unwrap();
    let wire: v7_4::GetValueRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(wire.tenant_info.tenant_id, -1);
    assert_eq!(wire.tenant_info.token, None);

    let bytes = commit().encode_request(v, TOKEN).unwrap();
    let wire: v7_4::CommitTransactionRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant_info, v7_4::TenantInfo::NONE);

    let bytes = get_key_server_locations().encode_request(v, TOKEN).unwrap();
    let wire: v7_4::GetKeyServerLocationsRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant, v7_4::TenantInfo::NONE);
    assert_eq!(wire.min_tenant_version, LATEST_VERSION);

    let bytes = get_key().encode_request(v, TOKEN).unwrap();
    let wire: v7_4::GetKeyRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant_info, v7_4::TenantInfo::NONE);

    let bytes = get_key_values(v).encode_request(v, TOKEN).unwrap();
    let wire: v7_4::GetKeyValuesRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant_info, v7_4::TenantInfo::NONE);

    let bytes = watch_value().encode_request(v, TOKEN).unwrap();
    let wire: v7_4::WatchValueRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant_info, v7_4::TenantInfo::NONE);
}

/// C++'s `LoadMember` default-constructs an absent table member, and `TenantInfo()` has
/// `tenantId = -1`; a present table with an absent `tenantId` slot reads 0.
#[test]
fn an_absent_tenant_info_is_the_default_tenant() {
    assert_eq!(
        <v7_4::TenantInfo as Decode>::absent(),
        v7_4::TenantInfo::NONE
    );
    assert_eq!(v7_4::TenantInfo::default(), v7_4::TenantInfo::NONE);

    // A 7.4 GetValueRequest whose `tenantInfo` slot is absent.
    #[allow(dead_code)]
    #[derive(fdb_native_wire::Wire)]
    #[fdb(file_identifier = 8454530)]
    struct NoTenant<'a> {
        key: &'a [u8],
        version: i64,
        #[fdb(skip_slots = 2)]
        tags: (),
        reply: fdb_native_wire::ReplyPromise<()>,
        span_context: SpanContext,
        #[fdb(skip_slots = 1)]
        tenant_info: (),
    }
    let bytes = encode(&NoTenant {
        key: b"k",
        version: 1,
        tags: (),
        reply: fdb_native_wire::ReplyPromise::new(TOKEN),
        span_context: span(),
        tenant_info: (),
    })
    .unwrap();
    let wire: v7_4::GetValueRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant_info, v7_4::TenantInfo::NONE);
}

/// The 7.4 fixture's request has a tenant with a token; the domain drops it, and encoding
/// the domain request again writes the default tenant.
#[test]
fn decoding_a_7_4_request_drops_its_tenant() {
    let bytes = record_bytes(&fixture("release-7.4", "get_value_request"));
    let wire: v7_4::GetValueRequest = decode(&bytes).unwrap();
    assert_ne!(wire.tenant_info.tenant_id, -1);
    assert!(wire.tenant_info.token.is_some());
    let (request, token) = GetValue::decode_request(SchemaVersion::V7_4, &bytes).unwrap();
    assert_eq!(token, wire.reply.token());
    let encoded = request.encode_request(SchemaVersion::V7_4, token).unwrap();
    let again: v7_4::GetValueRequest = decode(&encoded).unwrap();
    assert_eq!(again.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(again.key, wire.key);
    assert_eq!(
        again.options.map(|o| o.read_type),
        wire.options.map(|o| o.read_type)
    );
}

/// `ClientTagThrottleLimits` travels as a duration relative to the sender's clock; the
/// domain keeps it and turns it into a deadline from the receive time.
#[test]
fn throttle_limits_keep_their_duration() {
    for (release, version) in [
        ("release-7.4", SchemaVersion::V7_4),
        ("release-8.0", SchemaVersion::V8_0),
    ] {
        let bytes = record_bytes(&fixture(release, "get_read_version_reply_envelope"));
        let reply = GetReadVersion::decode_reply(version, &bytes)
            .unwrap()
            .unwrap();
        // The fixture's duration is exactly 30 (the generator's clock does not advance).
        let limits = *reply.tag_throttle_info.values().next().unwrap();
        assert_eq!(limits.duration, 30.0, "{release}");
        assert_eq!(limits.expires_after(), Some(Duration::from_secs(30)));
        let received = Instant::now();
        assert_eq!(
            limits.expires_at(received),
            Some(received + Duration::from_secs(30))
        );
        // Encoding writes the duration back.
        let bytes = GetReadVersion::encode_reply(version, &Ok(reply)).unwrap();
        let durations: Vec<f64> = match version {
            SchemaVersion::V7_4 => {
                let r: ErrorOr<EnsureTable<v7_4::GetReadVersionReply>> = decode(&bytes).unwrap();
                let r = r.unwrap().0;
                r.tag_throttle_info.values().map(|l| l.duration).collect()
            }
            SchemaVersion::V8_0 => {
                let r: ErrorOr<EnsureTable<v8_0::GetReadVersionReply>> = decode(&bytes).unwrap();
                let r = r.unwrap().0;
                r.tag_throttle_info.values().map(|l| l.duration).collect()
            }
        };
        assert_eq!(durations, [30.0], "{release}");
    }

    let limits = |duration| TagThrottleLimits {
        tps_rate: 1.0,
        duration,
    };
    // NO_EXPIRATION (f64::MAX), and anything else that does not fit a Duration: never.
    assert_eq!(limits(f64::MAX).expires_after(), None);
    assert_eq!(limits(f64::MAX - 1000.0).expires_after(), None);
    assert_eq!(limits(f64::INFINITY).expires_after(), None);
    assert_eq!(limits(f64::MAX).expires_at(Instant::now()), None);
    // Already expired.
    assert_eq!(limits(-5.0).expires_after(), Some(Duration::ZERO));
    assert_eq!(
        limits(f64::NEG_INFINITY).expires_after(),
        Some(Duration::ZERO)
    );
    assert_eq!(limits(f64::NAN).expires_after(), Some(Duration::ZERO));
    assert_eq!(
        limits(0.25).expires_after(),
        Some(Duration::from_millis(250))
    );
}

#[test]
fn version_specific_fields() {
    // 8.0's maxGrvQueueDelayMS is not sent to 7.4.
    let back = request_round_trip(SchemaVersion::V7_4, &get_read_version(SchemaVersion::V8_0));
    assert_eq!(back.max_grv_queue_delay_ms, None);
    // 7.4's proxyTagThrottledDuration is not sent to 8.0.
    let reply = read_version(SchemaVersion::V7_4);
    let bytes = GetReadVersion::encode_reply(SchemaVersion::V8_0, &Ok(reply)).unwrap();
    let back = GetReadVersion::decode_reply(SchemaVersion::V8_0, &bytes)
        .unwrap()
        .unwrap();
    assert_eq!(back.proxy_tag_throttled_duration, 0.0);
    // 7.4's tenantMode is not sent to 8.0, and is DISABLED on 7.4 when the domain has none.
    let info = client_db_info(SchemaVersion::V7_4);
    let bytes = OpenDatabaseCoord::encode_reply(SchemaVersion::V8_0, &Ok(info.clone())).unwrap();
    let back = OpenDatabaseCoord::decode_reply(SchemaVersion::V8_0, &bytes)
        .unwrap()
        .unwrap();
    assert_eq!(back.tenant_mode, None);
    let info = ClientDbInfo {
        tenant_mode: None,
        ..info
    };
    let bytes = OpenDatabaseCoord::encode_reply(SchemaVersion::V7_4, &Ok(info)).unwrap();
    let back = OpenDatabaseCoord::decode_reply(SchemaVersion::V7_4, &bytes)
        .unwrap()
        .unwrap();
    assert_eq!(back.tenant_mode, Some(TenantMode::DISABLED));
    // 8.0's taskID on GetKeyValues is not sent to 7.4.
    let request = get_key_values(SchemaVersion::V8_0);
    assert_eq!(request.task_id, Some(2500));
    assert_eq!(request_round_trip(SchemaVersion::V8_0, &request), request);
    let back = request_round_trip(SchemaVersion::V7_4, &request);
    assert_eq!(back.task_id, None);
    assert_eq!(
        back,
        GetKeyValues {
            task_id: None,
            ..request
        }
    );
    // ReadOptions::type is i32 on 7.4 and u32 on 8.0: the bits are kept.
    let mut request = get_value();
    request.options.as_mut().unwrap().read_type = u32::MAX;
    for v in SchemaVersion::ALL {
        assert_eq!(request_round_trip(v, &request), request, "{v}");
    }
}

/// The fixtures' 7.4 `ClientDBInfo` has a tenant mode; the domain carries it.
#[test]
fn tenant_mode_from_the_fixtures() {
    let bytes = record_bytes(&fixture("release-7.4", "client_db_info_envelope"));
    let wire: ErrorOr<EnsureTable<v7_4::ClientDBInfo>> = decode(&bytes).unwrap();
    let mode = wire.unwrap().0.tenant_mode.mode;
    let info = OpenDatabaseCoord::decode_reply(SchemaVersion::V7_4, &bytes)
        .unwrap()
        .unwrap();
    assert_eq!(info.tenant_mode, Some(TenantMode(mode)));
    let bytes = record_bytes(&fixture("release-8.0", "client_db_info_envelope"));
    let info = OpenDatabaseCoord::decode_reply(SchemaVersion::V8_0, &bytes)
        .unwrap()
        .unwrap();
    assert_eq!(info.tenant_mode, None);
}

#[test]
fn defaults_match_cpp() {
    // LoadBalancedReply() has penalty 1.0.
    assert_eq!(GetValueReply::default().penalty, 1.0);
    assert_eq!(v7_4::GetValueReply::default().penalty, 1.0);
    assert_eq!(GetKeyReply::default().penalty, 1.0);
    assert_eq!(v7_4::GetKeyReply::default().penalty, 1.0);
    assert_eq!(GetKeyValuesReply::default().penalty, 1.0);
    assert_eq!(v7_4::GetKeyValuesReply::default().penalty, 1.0);
    // GetKeyValuesReply() : version(invalidVersion)
    assert_eq!(GetKeyValuesReply::default().version, INVALID_VERSION);
    assert_eq!(v8_0::GetKeyValuesReply::default().version, INVALID_VERSION);
    assert!(GetKeyValuesReply::default().data.is_empty());
    // WatchValueReply() = default: zero.
    assert_eq!(
        WatchValueReply::default(),
        WatchValueReply {
            version: 0,
            cached: false
        }
    );
    // KeySelector(): the empty key, orEqual false, offset 0.
    assert_eq!(
        KeySelector::default(),
        KeySelector::new(b"".to_vec(), false, 0)
    );
}

/// The four standard selectors are `(key, orEqual, offset)` as C++'s `lastLessThan` and so on
/// build them (`FDBTypes.h`).
#[test]
fn key_selector_constructors() {
    let k = || b"k".to_vec();
    assert_eq!(
        KeySelector::last_less_than(k()),
        KeySelector::new(k(), false, 0)
    );
    assert_eq!(
        KeySelector::last_less_or_equal(k()),
        KeySelector::new(k(), true, 0)
    );
    assert_eq!(
        KeySelector::first_greater_than(k()),
        KeySelector::new(k(), true, 1)
    );
    assert_eq!(
        KeySelector::first_greater_or_equal(k()),
        KeySelector::new(k(), false, 1)
    );
    let sel = KeySelector::new(k(), true, -2);
    assert_eq!(sel.borrowed().into_owned(), sel);
}

/// A `GetKeyValuesReply` is read in place: the rows are a view of the received message's
/// blob, with no copy, and `more` and `version` come with it.
#[test]
fn get_key_values_reply_is_read_in_place() {
    for (release, schema) in [
        ("release-7.4", SchemaVersion::V7_4),
        ("release-8.0", SchemaVersion::V8_0),
    ] {
        let bytes = record_bytes(&fixture(release, "get_key_values_reply_envelope"));
        let reply = GetKeyValues::decode_reply(schema, &bytes).unwrap().unwrap();
        assert_eq!(reply.penalty, 2.0);
        assert_eq!(reply.error, None);
        assert_eq!(reply.version, 2_000_000_000_001);
        assert!(reply.more);
        assert!(!reply.cached);
        assert_eq!(reply.data.len(), 3);
        let rows: Vec<_> = reply.data.iter().collect();
        assert_eq!(
            rows,
            [
                KeyValueRef::new(b"range/b", b"value-b"),
                KeyValueRef::new(b"range/c", b""),
                KeyValueRef::new(b"range/d\xff", &[0, 1]),
            ]
        );
        // Zero-copy: the payload and the rows lie inside the message buffer.
        let message = bytes.as_ptr_range();
        for slice in [
            reply.data.payload(),
            rows[0].key,
            rows[0].value,
            rows[2].key,
        ] {
            let range = slice.as_ptr_range();
            assert!(
                message.start <= range.start && range.end <= message.end,
                "{release}"
            );
        }
        // The empty reply is a four-byte blob holding a zero count.
        let bytes = record_bytes(&fixture(release, "get_key_values_reply_empty"));
        let reply = GetKeyValues::decode_reply(schema, &bytes).unwrap().unwrap();
        assert!(reply.data.is_empty());
        assert_eq!(reply.data, KeyValueVectorRef::EMPTY);
        assert_eq!(reply.version, 5);
        assert!(!reply.more);
    }
}

/// A `GetKeyValuesReply` whose blob does not add up is a decode error, not a panic.
#[test]
fn a_malformed_get_key_values_blob_is_an_error() {
    let bytes = record_bytes(&fixture("release-8.0", "get_key_values_reply_envelope"));
    let reply = GetKeyValues::decode_reply(SchemaVersion::V8_0, &bytes)
        .unwrap()
        .unwrap();
    // Corrupt the first item's key length (the payload is the count, then that length).
    let at = reply.data.payload().as_ptr() as usize - bytes.as_ptr() as usize;
    let mut bad = bytes.clone();
    bad[at + 4..at + 8].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(GetKeyValues::decode_reply(SchemaVersion::V8_0, &bad).is_err());
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
// Single-key compression and inverted ranges.

/// `(begin, end)` as the encoder writes them.
fn written_range(begin: &[u8], end: &[u8]) -> Result<(Vec<u8>, Vec<u8>), EncodeError> {
    let (b, e) = v7_4::KeyRangeRef::new(begin, end).wire_form()?;
    Ok((b.to_vec(), e.to_vec()))
}

/// The raw fields of a `KeyRangeRef` or `MutationRef` table, with no compression or
/// checksum handling.
#[derive(fdb_native_wire::Wire, Debug, PartialEq)]
#[fdb(file_identifier = 1)]
struct RawPair<'a> {
    first: &'a [u8],
    second: &'a [u8],
}

#[derive(fdb_native_wire::Wire, Debug, PartialEq)]
#[fdb(file_identifier = 1)]
struct RawMutation<'a> {
    kind: u8,
    param1: &'a [u8],
    param2: &'a [u8],
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
    assert_eq!(
        written_range(b"k", b"k\x00"),
        Ok((b"k\x00".to_vec(), vec![]))
    );
    // The empty key: ["", "\0") is written ("\0", "").
    assert_eq!(written_range(b"", b"\x00"), Ok((b"\x00".to_vec(), vec![])));
    // Not single-key ranges: written as they are.
    for (b, e) in [
        (&b"k"[..], &b"k\x00\x00"[..]),
        (b"k", b"l"),
        (b"k", b"k\x01"),
        (b"k\x00", b"k\x00"),
        (b"", b""),
    ] {
        assert_eq!(written_range(b, e), Ok((b.to_vec(), e.to_vec())));
    }

    // Through a message: every range survives encode and decode.
    let ranges = [
        KeyRange::single_key(b"k"),
        KeyRange::single_key(b""),
        KeyRange::single_key(b"\x00"),
        kr("k", b"k\x00\x00".to_vec()),
        kr("", ""),
        kr("a", "b"),
    ];
    let commit = Commit {
        read_conflict_ranges: ranges.to_vec(),
        ..Commit::default()
    };
    for v in SchemaVersion::ALL {
        assert_eq!(
            request_round_trip(v, &commit).read_conflict_ranges,
            ranges,
            "{v}"
        );
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

/// C++ `KeyRangeRef::serialize` throws `inverted_range` when writing an inverted range, and a
/// server that cannot deserialize a request exits; `(k\0, "")` would also be read back as the
/// compressed `[k, k\0)`. Encoding refuses them, and the domain constructors do too.
#[test]
fn inverted_ranges_are_not_encoded() {
    let invalid = |r: Result<(Vec<u8>, Vec<u8>), EncodeError>| {
        assert!(matches!(r, Err(EncodeError::Invalid(_))), "{r:?}");
    };
    invalid(written_range(b"b", b"a"));
    invalid(written_range(b"k\x00", b""));
    invalid(written_range(b"a", b""));

    assert!(KeyRange::new("b", "a").is_err());
    assert!(KeyRange::new(b"k\x00".to_vec(), "").is_err());
    assert_eq!(kr("a", "a"), KeyRange::new("a", "a").unwrap());

    // Through a request: the reviewer's three probes, and a write conflict range.
    let inverted = KeyRange {
        begin: b"b".to_vec(),
        end: b"a".to_vec(),
    };
    let open_end = KeyRange {
        begin: b"k\x00".to_vec(),
        end: vec![],
    };
    let clear_open = Mutation {
        kind: MutationType::CLEAR_RANGE,
        param1: b"k\x00".to_vec(),
        param2: vec![],
    };
    for v in SchemaVersion::ALL {
        for commit in [
            Commit {
                read_conflict_ranges: vec![inverted.clone()],
                ..Commit::default()
            },
            Commit {
                write_conflict_ranges: vec![open_end.clone()],
                ..Commit::default()
            },
            Commit {
                mutations: vec![clear_open.clone()],
                ..Commit::default()
            },
        ] {
            assert!(
                matches!(
                    commit.encode_request(v, TOKEN),
                    Err(EncodeError::Invalid(_))
                ),
                "{v}: {commit:?}"
            );
        }
        let reply = KeyServerLocations {
            results: vec![(inverted.clone(), vec![])],
            ..KeyServerLocations::default()
        };
        assert!(GetKeyServerLocations::encode_reply(v, &Ok(reply)).is_err());
    }
}

#[test]
fn mutation_single_key_compression() {
    let written = |kind: u8, p1: &[u8], p2: &[u8]| {
        v7_4::MutationRef::new(kind, p1, p2)
            .wire_form()
            .map(|(a, b)| (a.to_vec(), b.to_vec()))
    };
    // Only a ClearRange of one key is compressed.
    assert_eq!(written(1, b"k", b"k\x00"), Ok((b"k\x00".to_vec(), vec![])));
    assert_eq!(written(1, b"", b"\x00"), Ok((b"\x00".to_vec(), vec![])));
    assert_eq!(
        written(0, b"k", b"k\x00"),
        Ok((b"k".to_vec(), b"k\x00".to_vec()))
    );
    assert_eq!(
        written(2, b"k", b"k\x00"),
        Ok((b"k".to_vec(), b"k\x00".to_vec()))
    );
    assert_eq!(written(1, b"k", b"l"), Ok((b"k".to_vec(), b"l".to_vec())));
    // A SetValue to an empty value is not ambiguous: only clears are expanded.
    assert_eq!(written(0, b"k\x00", b""), Ok((b"k\x00".to_vec(), vec![])));
    // An inverted clear, including the ambiguous (k\0, ""), is refused.
    assert!(matches!(
        written(1, b"k\x00", b""),
        Err(EncodeError::Invalid(_))
    ));
    assert!(matches!(
        written(1, b"b", b"a"),
        Err(EncodeError::Invalid(_))
    ));
    // So is a type with a checksum flag bit.
    assert!(matches!(
        written(0x80, b"k", b"v"),
        Err(EncodeError::Invalid(_))
    ));
    assert!(matches!(
        written(0x40, b"k", b"v"),
        Err(EncodeError::Invalid(_))
    ));

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

    // Through a message.
    let mutations = vec![
        mu(MutationType::CLEAR_RANGE, b"k".to_vec(), b"k\x00".to_vec()),
        mu(MutationType::CLEAR_RANGE, b"".to_vec(), b"\x00".to_vec()),
        mu(MutationType::CLEAR_RANGE, "", ""),
        mu(MutationType::SET_VALUE, b"k\x00".to_vec(), b"".to_vec()),
        mu(MutationType::SET_VALUE, b"k".to_vec(), b"k\x00".to_vec()),
    ];
    let commit = Commit {
        mutations: mutations.clone(),
        ..Commit::default()
    };
    for v in SchemaVersion::ALL {
        assert_eq!(request_round_trip(v, &commit).mutations, mutations, "{v}");
    }

    // The domain constructor.
    assert!(Mutation::new(MutationType::CLEAR_RANGE, "b", "a").is_err());
    assert!(Mutation::new(MutationType::CLEAR_RANGE, b"k\x00".to_vec(), "").is_err());
    assert!(Mutation::new(MutationType(0x81), "k", "v").is_err());
    assert!(Mutation::new(MutationType(0x41), "k", "v").is_err());
    assert_eq!(
        Mutation::new(MutationType::SET_VALUE, "b", "a"),
        Ok(Mutation {
            kind: MutationType::SET_VALUE,
            param1: b"b".to_vec(),
            param2: b"a".to_vec(),
        })
    );
    let _: InvalidValue = KeyRange::new("b", "a").unwrap_err();
}

/// Mutations written with `ENABLE_MUTATION_CHECKSUM` (and `ENABLE_ACCUMULATIVE_CHECKSUM`):
/// the reader removes the flag bits and the param2 suffixes before the single-key expansion,
/// as C++'s `offloadAccumulativeChecksumIndex` / `offloadChecksum` do.
#[test]
fn checksummed_mutations_are_offloaded() {
    let decode_raw = |kind: u8, p1: &[u8], p2: &[u8]| {
        let raw = RawMutation {
            kind,
            param1: p1,
            param2: p2,
        };
        let bytes = encode(&raw).unwrap();
        let reader = fdb_native_wire::Reader::new(&bytes).unwrap();
        reader.decode_root::<v7_4::MutationRef>().map(|m| {
            (
                m.kind,
                m.param1.to_vec(),
                m.param2.to_vec(),
                m.checksum,
                m.accumulative_checksum_index,
            )
        })
    };
    let crc = 0x1234_5678u32.to_le_bytes();
    let acs = 0xabcdu16.to_le_bytes();

    // SetValue with a checksum.
    assert_eq!(
        decode_raw(0x80, b"k", &[&b"v"[..], &crc].concat()),
        Ok((0, b"k".to_vec(), b"v".to_vec(), Some(0x1234_5678), None))
    );
    // With a checksum and an accumulative index.
    assert_eq!(
        decode_raw(0xc0 | 2, b"k", &[&b"\x01"[..], &crc, &acs].concat()),
        Ok((
            2,
            b"k".to_vec(),
            b"\x01".to_vec(),
            Some(0x1234_5678),
            Some(0xabcd)
        ))
    );
    // A single-key clear: the suffixes are on the empty param2, then it expands.
    assert_eq!(
        decode_raw(0x81, b"k\x00", &crc),
        Ok((1, b"k".to_vec(), b"k\x00".to_vec(), Some(0x1234_5678), None))
    );
    assert_eq!(
        decode_raw(0xc1, b"k\x00", &[&crc[..], &acs].concat()),
        Ok((
            1,
            b"k".to_vec(),
            b"k\x00".to_vec(),
            Some(0x1234_5678),
            Some(0xabcd)
        ))
    );
    // A param2 too short for its flags (C++'s validateParam2) is an error.
    assert!(decode_raw(0x80, b"k", b"abc").is_err());
    assert!(decode_raw(0xc0, b"k", b"abcde").is_err());
    // An index flag without the checksum flag is never written, and is an error.
    assert!(decode_raw(0x40, b"k", b"ab").is_err());

    // The domain gets the plain mutation, and re-encoding writes it without checksums.
    let m: Mutation = v7_4::MutationRef::from_wire_form(0x81, b"k\x00", &crc)
        .unwrap()
        .into();
    assert_eq!(m, mu(MutationType::CLEAR_RANGE, "k", b"k\x00".to_vec()));
    let param2 = [&b"v"[..], &crc].concat();
    let raw = v7_4::MutationRef::from_wire_form(0x80, b"k", &param2).unwrap();
    let bytes = fdb_native_wire::encode_with_identifier(&raw, 1).unwrap();
    let reader = fdb_native_wire::Reader::new(&bytes).unwrap();
    assert_eq!(
        reader.decode_root::<RawMutation>().unwrap(),
        RawMutation {
            kind: 0,
            param1: b"k",
            param2: b"v"
        }
    );
    // A domain mutation with a flag bit (built without the constructor) does not encode.
    let commit = Commit {
        mutations: vec![Mutation {
            kind: MutationType(0x80),
            param1: b"k".to_vec(),
            param2: b"v".to_vec(),
        }],
        ..Commit::default()
    };
    assert!(matches!(
        commit.encode_request(SchemaVersion::V8_0, TOKEN),
        Err(EncodeError::Invalid(_))
    ));
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
        let (commit, _) = Commit::decode_request(v, &bytes).unwrap();
        assert_eq!(
            commit.write_conflict_ranges,
            [KeyRange::single_key(b"single")]
        );
        assert_eq!(
            commit.mutations,
            [mu(
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
        let info = OpenDatabaseCoord::decode_reply(v, &bytes).unwrap().unwrap();
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
        let locations = GetKeyServerLocations::decode_reply(v, &bytes)
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

/// The storage metrics requests: both round trip on both versions; 8.0's `SplitRangeRequest`
/// carries `limit`, 7.4's has no such field (it decodes as -1) and a tenant-less `TenantInfo`.
#[test]
fn storage_metrics_requests_round_trip() {
    let wait = WaitMetrics {
        keys: KeyRange::new(b"a".to_vec(), b"m".to_vec()).unwrap(),
        min: StorageMetrics {
            bytes: 1,
            bytes_written_per_ksecond: 2,
            ios_per_ksecond: 3,
            bytes_read_per_ksecond: 4,
            ops_read_per_ksecond: 5,
        },
        max: StorageMetrics {
            bytes: -1,
            ..StorageMetrics::default()
        },
        version: 77,
    };
    let split = SplitRange {
        keys: KeyRange::single_key(b"k"),
        chunk_size: 1 << 20,
        limit: 5,
    };
    for v in SchemaVersion::ALL {
        let bytes = wait.encode_request(v, TOKEN).unwrap();
        assert_eq!(
            WaitMetrics::decode_request(v, &bytes).unwrap(),
            (wait.clone(), TOKEN)
        );
        let reply = StorageMetrics {
            bytes: 12345,
            ..StorageMetrics::default()
        };
        let bytes = WaitMetrics::encode_reply(v, &Ok(reply)).unwrap();
        assert_eq!(WaitMetrics::decode_reply(v, &bytes).unwrap(), Ok(reply));
        let bytes = WaitMetrics::encode_reply(v, &Err(FdbError::new(1001))).unwrap();
        assert_eq!(
            WaitMetrics::decode_reply(v, &bytes).unwrap(),
            Err(FdbError::new(1001))
        );

        let bytes = split.encode_request(v, TOKEN).unwrap();
        let (decoded, token) = SplitRange::decode_request(v, &bytes).unwrap();
        assert_eq!(token, TOKEN);
        let limit = match v {
            SchemaVersion::V7_4 => -1,
            SchemaVersion::V8_0 => 5,
        };
        assert_eq!(
            decoded,
            SplitRange {
                limit,
                ..split.clone()
            }
        );
        let reply = SplitRangeReply {
            split_points: vec![b"b".to_vec(), b"c\x00".to_vec()],
        };
        let bytes = SplitRange::encode_reply(v, &Ok(reply.clone())).unwrap();
        assert_eq!(SplitRange::decode_reply(v, &bytes).unwrap(), Ok(reply));
    }
    let bytes = split.encode_request(SchemaVersion::V7_4, TOKEN).unwrap();
    let wire: v7_4::SplitRangeRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant_info, v7_4::TenantInfo::NONE);
    let bytes = wait.encode_request(SchemaVersion::V7_4, TOKEN).unwrap();
    let wire: v7_4::WaitMetricsRequest = decode(&bytes).unwrap();
    assert_eq!(wire.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(wire.min_version, 77);
    // An inverted range is refused, as for every request.
    let inverted = SplitRange {
        keys: KeyRange {
            begin: b"z".to_vec(),
            end: b"a".to_vec(),
        },
        ..split
    };
    assert!(inverted.encode_request(SchemaVersion::V8_0, TOKEN).is_err());
}
