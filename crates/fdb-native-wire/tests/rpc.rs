//! `ReplyPromise`, `RequestStream` (with `Endpoint`) and `VectorRef`, against the engine
//! fixtures (file-identifier composition, `request_like`) and the release fixtures
//! (`tools/golden-vectors/fixtures/release-7.4` and `release-8.0`) whose messages contain
//! reply promises and request streams. Expected values come from the fixtures' JSON dumps
//! of FDB's own messages, never from the Rust types.

#![allow(unreachable_pub)]

mod support;

use std::path::Path;

use fdb_native_wire::{
    DecodeError, Endpoint, FileIdentified, IpAddress, NetworkAddress, NetworkAddressList, Reader,
    ReplyPromise, RequestStream, Uid, VectorRef, Wire, Wrapper, decode, encode,
};
use serde_json::Value;
use support::{Inner, RequestLike, hex_decode, read_json, record_bytes, record_fid};

// ---------------------------------------------------------------------------------------
// Fixture access.

fn engine(name: &str) -> Value {
    read_json(&support::fixtures_dir().join(format!("{name}.json")))
}

fn release(branch: &str, name: &str) -> Value {
    read_json(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tools/golden-vectors/fixtures")
            .join(branch)
            .join(format!("{name}.json")),
    )
}

const BRANCHES: [&str; 2] = ["release-7.4", "release-8.0"];

/// The member of a dumped table named `field`.
fn member<'a>(table: &'a Value, field: &str) -> &'a Value {
    table["value"]
        .as_array()
        .unwrap_or_else(|| panic!("{table} is not a table"))
        .iter()
        .find(|m| m["field"] == field)
        .unwrap_or_else(|| panic!("no member {field} in {table}"))
}

fn uid(node: &Value) -> Uid {
    assert_eq!(node["type"], "uid");
    Uid::new(
        node["value"]["first"].as_u64().expect("first"),
        node["value"]["second"].as_u64().expect("second"),
    )
}

fn ip_address(node: &Value) -> IpAddress {
    assert_eq!(node["type"], "table<IPAddress>");
    let addr = member(node, "addr");
    let value = &addr["value"];
    match addr["tag"].as_u64().expect("tag") {
        1 => IpAddress::V4(u32::try_from(value["value"].as_u64().expect("u32")).expect("u32")),
        2 => {
            let bytes: Vec<u8> = value["value"]
                .as_array()
                .expect("array")
                .iter()
                .map(|b| u8::try_from(b["value"].as_u64().expect("u8")).expect("u8"))
                .collect();
            IpAddress::V6(bytes.try_into().expect("16 bytes"))
        }
        tag => panic!("variant tag {tag}"),
    }
}

fn network_address(node: &Value) -> NetworkAddress {
    assert_eq!(node["type"], "table<NetworkAddress>");
    NetworkAddress {
        ip: ip_address(member(node, "ip")),
        port: u16::try_from(member(node, "port")["value"].as_u64().expect("port")).expect("u16"),
        flags: u16::try_from(member(node, "flags")["value"].as_u64().expect("flags")).expect("u16"),
        from_hostname: member(node, "fromHostname")["value"]
            .as_bool()
            .expect("bool"),
    }
}

/// The `Endpoint` held by a dumped `table<request_stream>`.
fn request_stream<T>(node: &Value) -> RequestStream<T> {
    assert_eq!(node["type"], "table<request_stream>");
    let endpoint = member(node, "endpoint");
    assert_eq!(endpoint["type"], "table<Endpoint>");
    let addresses = member(endpoint, "addresses");
    let secondary = member(addresses, "secondaryAddress");
    RequestStream::new(Endpoint {
        addresses: NetworkAddressList {
            address: network_address(member(addresses, "address")),
            secondary_address: (secondary["tag"] == 1)
                .then(|| network_address(&secondary["value"])),
        },
        token: uid(member(endpoint, "token")),
    })
}

fn reply_promise<T>(node: &Value) -> ReplyPromise<T> {
    assert_eq!(node["type"], "table<reply_promise>");
    ReplyPromise::new(uid(member(node, "token")))
}

/// The table inside the root.
fn message(record: &Value) -> &Value {
    &record["root"]["value"][0]
}

fn vector(node: &Value) -> &[Value] {
    node["value"].as_array().expect("vector")
}

// ---------------------------------------------------------------------------------------
// File identifiers.

#[test]
fn identifiers_match_engine_fixtures() {
    for (name, composed) in [
        ("fid_reply_promise", <ReplyPromise<Inner>>::FILE_IDENTIFIER),
        ("fid_vectorref", <VectorRef<Inner>>::FILE_IDENTIFIER),
        ("fid_std_vector", <Vec<Inner>>::FILE_IDENTIFIER),
    ] {
        assert_eq!(composed.value(), record_fid(&engine(name)), "{name}");
        assert_eq!(composed.composition_depth(), 1, "{name}");
    }
    // The C++ formulas: (code << 24) | id for a plain type.
    let inner = Inner::FILE_IDENTIFIER.value();
    assert_eq!(inner, 0x000f_db0a);
    assert_eq!(Wrapper::ReplyPromise.code(), 1);
    assert_eq!(Wrapper::VectorRef.code(), 3);
    assert_eq!(Wrapper::Vector.code(), 5);
    assert_eq!(
        <ReplyPromise<Inner>>::FILE_IDENTIFIER.value(),
        (1 << 24) | inner
    );
    assert_eq!(
        <VectorRef<Inner>>::FILE_IDENTIFIER.value(),
        (3 << 24) | inner
    );
    assert_eq!(<Vec<Inner>>::FILE_IDENTIFIER.value(), (5 << 24) | inner);
    // Two levels: the inner wrapper keeps its code in bits 24..28, the outer goes to 28..32.
    assert_eq!(
        <ReplyPromise<VectorRef<Inner>>>::FILE_IDENTIFIER.value(),
        (1 << 28) | (3 << 24) | inner
    );
    assert_eq!(
        <VectorRef<ReplyPromise<Inner>>>::FILE_IDENTIFIER.value(),
        (3 << 28) | (1 << 24) | inner
    );
}

#[test]
fn reply_promise_decodes_engine_fixtures() {
    let record = engine("fid_reply_promise");
    let reply: ReplyPromise<Inner> = decode(&record_bytes(&record)).expect("decode");
    assert_eq!(reply, reply_promise(message(&record)));
    assert_eq!(
        reply.token(),
        Uid::new(1229782938247303441, 2459565876494606882)
    );
    assert_eq!(
        encode(&reply).expect("encode")[4..8],
        record_fid(&record).to_le_bytes()
    );

    let record = engine("request_like");
    let bytes = record_bytes(&record);
    let request: RequestLike = decode(&bytes).expect("decode");
    assert_eq!(
        request.reply,
        reply_promise(member(message(&record), "reply"))
    );
    assert_eq!(
        request.reply.token(),
        Uid::new(6148914691236517205, 7378697629483820646)
    );
}

#[test]
fn vector_ref_and_vec_differ_only_in_identifier() {
    let vector_ref = engine("fid_vectorref");
    let std_vector = engine("fid_std_vector");
    let (a, b) = (record_bytes(&vector_ref), record_bytes(&std_vector));
    assert_eq!(a[..4], b[..4]);
    assert_eq!(a[8..], b[8..]);

    let want = VectorRef(vec![Inner {
        a: 1,
        b: support::Bytes(b"x".to_vec()),
    }]);
    assert_eq!(decode::<VectorRef<Inner>>(&a).expect("VectorRef"), want);
    assert_eq!(decode::<Vec<Inner>>(&b).expect("vector"), want.0);
    // The composed identifier is checked: neither type decodes the other's message.
    assert!(matches!(
        decode::<Vec<Inner>>(&a),
        Err(DecodeError::FileIdentifierMismatch {
            expected: 0x050f_db0a,
            found: 0x030f_db0a
        })
    ));
    assert!(matches!(
        decode::<VectorRef<Inner>>(&b),
        Err(DecodeError::FileIdentifierMismatch {
            expected: 0x030f_db0a,
            found: 0x050f_db0a
        })
    ));
    // Same bytes, identifier aside, whichever type wrote them.
    assert_eq!(encode(&want).expect("encode")[8..], a[8..]);
    // The root's fields decode without the identifier check.
    let reader = Reader::new(&a).expect("reader");
    assert_eq!(reader.decode_root::<Vec<Inner>>().expect("root"), want.0);
}

#[test]
fn shapes() {
    assert_eq!(<ReplyPromise<Inner>>::SHAPE, fdb_native_wire::Shape::Table);
    assert_eq!(<RequestStream<Inner>>::SHAPE, fdb_native_wire::Shape::Table);
    assert_eq!(<VectorRef<Inner>>::SHAPE, <Vec<Inner>>::SHAPE);
}

// ---------------------------------------------------------------------------------------
// Release fixtures: ReplyPromise and RequestStream fields of FDB 7.4 and 8.0 messages.
//
// Each type below models the leading slots of a message and stops: the reader ignores
// trailing slots, so fields the test does not look at need no schema.

mod schema {
    use fdb_native_wire::{ReplyPromise, RequestStream, Uid, Wire};

    // Request types are markers: they are not on the wire.
    pub struct GetValueRequest;
    pub struct GetReadVersionRequest;
    pub struct CommitTransactionRequest;
    pub struct GetReplyUnit;

    /// `serializer(ar, key, version, tags, reply, ...)` of `GetValueRequest`; the same
    /// leading slots on both branches (8.0 removed `tenantInfo` further down).
    #[derive(Wire, Debug, PartialEq)]
    #[fdb(file_identifier = 0x00810182)]
    pub struct GetValueRequestPrefix<'a> {
        pub key: &'a [u8],
        pub version: i64,
        #[fdb(skip_slots = 2)] // Optional<TagSet>
        pub tags: (),
        pub reply: ReplyPromise<GetReplyUnit>,
    }

    /// `GrvProxyInterface`: `serializer(ar, processId, provisional,
    /// getConsistentReadVersion)` (`processId` is an `Optional`: two slots).
    #[derive(Wire, Debug, PartialEq)]
    pub struct GrvProxyInterface<'a> {
        pub process_id: Option<&'a [u8]>,
        pub provisional: bool,
        pub get_consistent_read_version: RequestStream<GetReadVersionRequest>,
    }

    /// `CommitProxyInterface`: `serializer(ar, processId, provisional, commit)`.
    #[derive(Wire, Debug, PartialEq)]
    pub struct CommitProxyInterface<'a> {
        pub process_id: Option<&'a [u8]>,
        pub provisional: bool,
        pub commit: RequestStream<CommitTransactionRequest>,
    }

    /// `ClientDBInfo`: `serializer(ar, grvProxies, commitProxies, id, ...)`.
    #[derive(Wire, Debug, PartialEq)]
    #[fdb(file_identifier = 0x0051b648)]
    pub struct ClientDbInfo<'a> {
        pub grv_proxies: Vec<GrvProxyInterface<'a>>,
        pub commit_proxies: Vec<CommitProxyInterface<'a>>,
        pub id: Uid,
    }

    /// `StorageServerInterface`: `serializer(ar, uniqueID, locality, getValue, ...)`.
    #[derive(Wire, Debug, PartialEq)]
    pub struct StorageServerInterface {
        pub unique_id: Uid,
        #[fdb(skip_slots = 1)] // LocalityData
        pub locality: (),
        pub get_value: RequestStream<GetValueRequest>,
    }

    #[derive(Wire, Debug, PartialEq)]
    pub struct KeyRange<'a> {
        pub begin: &'a [u8],
        pub end: &'a [u8],
    }

    /// `std::pair<KeyRangeRef, std::vector<StorageServerInterface>>`: a table.
    #[derive(Wire, Debug, PartialEq)]
    pub struct Location<'a> {
        pub range: KeyRange<'a>,
        pub servers: Vec<StorageServerInterface>,
    }

    /// `GetKeyServerLocationsReply`: `serializer(ar, results, ...)`.
    #[derive(Wire, Debug, PartialEq)]
    #[fdb(file_identifier = 0x00a24af7)]
    pub struct GetKeyServerLocationsReply<'a> {
        pub results: Vec<Location<'a>>,
    }
}

use schema::*;

#[test]
fn get_value_request_reply_token() {
    for branch in BRANCHES {
        let record = release(branch, "get_value_request");
        let bytes = record_bytes(&record);
        let request: GetValueRequestPrefix = decode(&bytes).expect(branch);
        assert_eq!(
            request.reply,
            reply_promise(member(message(&record), "reply")),
            "{branch}"
        );
        assert_eq!(
            request.reply.token(),
            Uid::new(6817605410908798977, 9123448420122492929),
            "{branch}"
        );
        assert_eq!(request.key, hex_decode("757365722f6b657900ff"), "{branch}");
        assert_eq!(request.version, 1234567890123, "{branch}");
    }
}

/// Every interface of `info` against the dump of FDB's own message: the request streams
/// (endpoints and tokens), the provisional flags and the cluster id.
fn assert_client_db_info_matches(info: &ClientDbInfo, dump: &Value, what: &str) {
    let grv = vector(member(dump, "grvProxies"));
    assert_eq!(info.grv_proxies.len(), grv.len(), "{what}");
    assert!(!grv.is_empty(), "{what}");
    for (got, want) in info.grv_proxies.iter().zip(grv) {
        let want_stream = request_stream(member(want, "getConsistentReadVersion"));
        assert_eq!(got.get_consistent_read_version, want_stream, "{what}");
        assert_eq!(
            got.provisional,
            member(want, "provisional")["value"],
            "{what}"
        );
    }
    let commit = vector(member(dump, "commitProxies"));
    assert_eq!(info.commit_proxies.len(), commit.len(), "{what}");
    for (got, want) in info.commit_proxies.iter().zip(commit) {
        assert_eq!(got.commit, request_stream(member(want, "commit")), "{what}");
        assert_eq!(
            got.provisional,
            member(want, "provisional")["value"],
            "{what}"
        );
    }
    assert_eq!(info.id, uid(member(dump, "id")), "{what}");
}

#[test]
fn client_db_info_request_streams() {
    for branch in BRANCHES {
        let record = release(branch, "client_db_info");
        let bytes = record_bytes(&record);
        let info: ClientDbInfo = decode(&bytes).expect(branch);
        let dump = message(&record);
        assert_client_db_info_matches(&info, dump, branch);

        // Re-encoding what was decoded and decoding that gives the same dump. (Not
        // byte-identical to FDB's message: the schema stops after `id`, so the slots after
        // it are written absent, and this writer's layout is its own.)
        let again = encode(&info).expect("encode");
        let reread: ClientDbInfo = decode(&again).expect(branch);
        assert_eq!(reread, info, "{branch}");
        assert_client_db_info_matches(&reread, dump, &format!("{branch} re-encoded"));
        assert_ne!(again, bytes, "{branch}");

        // The first GRV proxy, spelled out: 10.0.0.1:4500 plain, and fdxx::1 TLS on 4501.
        let endpoint = info.grv_proxies[0].get_consistent_read_version.endpoint();
        assert_eq!(
            endpoint.addresses.address,
            NetworkAddress {
                ip: IpAddress::V4(0x0a00_0001),
                port: 4500,
                flags: 0,
                from_hostname: false,
            },
            "{branch}"
        );
        let secondary = endpoint.addresses.secondary_address.expect("secondary");
        assert_eq!(secondary.port, 4501, "{branch}");
        assert!(secondary.is_tls() && secondary.is_public(), "{branch}");
        assert!(matches!(secondary.ip, IpAddress::V6(ip) if ip[0] == 0xfd && ip[15] == 1));
        assert_eq!(
            endpoint.token,
            Uid::new(7484982580689764353, 7484982580689764352),
            "{branch}"
        );
    }
}

#[test]
fn key_server_locations_request_streams() {
    for branch in BRANCHES {
        let record = release(branch, "get_key_server_locations_reply");
        let bytes = record_bytes(&record);
        let reply: GetKeyServerLocationsReply = decode(&bytes).expect(branch);
        let results = vector(member(message(&record), "results"));
        assert_eq!(reply.results.len(), results.len(), "{branch}");
        assert!(!results.is_empty(), "{branch}");
        for (got, want) in reply.results.iter().zip(results) {
            let servers = vector(member(want, "second"));
            assert_eq!(got.servers.len(), servers.len(), "{branch}");
            for (got, want) in got.servers.iter().zip(servers) {
                assert_eq!(got.unique_id, uid(member(want, "uniqueID")), "{branch}");
                assert_eq!(
                    got.get_value,
                    request_stream(member(want, "getValue")),
                    "{branch}"
                );
                // An interface serializes its first endpoint only; `getKey` is the next.
                let get_key = got.get_value.endpoint().adjusted(1);
                assert_eq!(
                    get_key.addresses,
                    got.get_value.endpoint().addresses,
                    "{branch}"
                );
                assert_eq!(
                    get_key.token.first,
                    got.get_value.endpoint().token.first + (1 << 32)
                );
            }
        }
        let again = encode(&reply).expect("encode");
        assert_eq!(
            decode::<GetKeyServerLocationsReply>(&again).expect("decode"),
            reply
        );
    }
}

#[test]
fn absent_request_stream_is_default() {
    use fdb_native_wire::Decode;
    let stream = <RequestStream<GetValueRequest> as Decode>::absent();
    assert_eq!(stream, RequestStream::default());
    assert_eq!(stream.endpoint().token, Uid::default());
    // C++ default-constructs a missing table, and NetworkAddress() is private.
    assert!(!stream.endpoint().addresses.address.is_public());
}
