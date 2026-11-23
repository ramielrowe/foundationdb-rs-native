use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use proptest::prelude::*;

use super::*;

fn parse(s: &str) -> ClusterConnectionString {
    s.parse()
        .unwrap_or_else(|e| panic!("`{s}` should parse: {e}"))
}

fn assert_invalid(s: &str) {
    assert!(
        s.parse::<ClusterConnectionString>().is_err(),
        "`{s}` should be rejected"
    );
}

fn addr(s: &str, tls: bool) -> Coordinator {
    Coordinator::Addr {
        addr: s.parse::<SocketAddr>().unwrap(),
        tls,
    }
}

fn host(name: &str, port: u16, tls: bool) -> Coordinator {
    Coordinator::Host {
        name: name.to_owned(),
        port,
        tls,
    }
}

/// `input` parses and prints back unchanged, with and without FDB's comment
/// decoration.
fn assert_round_trips_commented(input: &str) -> ClusterConnectionString {
    let cs = parse(input);
    assert_eq!(cs.to_string(), input);
    let commented = format!("#start of comment\n{input}\n# asdfasdf ##");
    assert_eq!(parse(&commented), cs);
    cs
}

// ---------------------------------------------------------------------------
// FDB's own tests, MonitorLeader.cpp (7.4: 128-268, 360-388; 8.0: 132-272,
// 363-391).

#[test]
fn fdb_parse_connection_string_addresses() {
    assert_round_trips_commented("asdf:2345@1.1.1.1:345");

    // Duplicate addresses. FDB's test only asserts the error if one is thrown;
    // the code does throw.
    assert_eq!(
        "asdf:2345@1.1.1.1:345,1.1.1.1:345"
            .parse::<ClusterConnectionString>()
            .unwrap_err(),
        ConnectionStringError::DuplicateCoordinator("1.1.1.1:345".into())
    );

    let cs = assert_round_trips_commented("0xxdeadbeef:100100100@1.1.1.1:34534,5.1.5.3:23443");
    assert_eq!(cs.description(), "0xxdeadbeef");
    assert_eq!(cs.id(), "100100100");
    assert_eq!(cs.key(), "0xxdeadbeef:100100100");
    assert_eq!(
        cs.coordinators(),
        [addr("1.1.1.1:34534", false), addr("5.1.5.3:23443", false)]
    );

    let cs = assert_round_trips_commented("0xxdeadbeef:100100100@[::1]:1234,[::1]:1235");
    assert_eq!(
        cs.coordinators(),
        [addr("[::1]:1234", false), addr("[::1]:1235", false)]
    );

    assert_round_trips_commented(
        "0xxdeadbeef:100100100@[abcd:dcba::1]:1234,[abcd:dcba::abcd:1]:1234",
    );
}

#[test]
fn fdb_parse_connection_string_hostnames() {
    let cs = assert_round_trips_commented("asdf:2345@localhost:1234");
    assert_eq!(cs.coordinators(), [host("localhost", 1234, false)]);

    assert_eq!(
        "asdf:2345@localhost:1234,localhost:1234"
            .parse::<ClusterConnectionString>()
            .unwrap_err(),
        ConnectionStringError::DuplicateCoordinator("localhost:1234".into())
    );

    let cs = assert_round_trips_commented("0xxdeadbeef:100100100@localhost:34534,host-name:23443");
    assert_eq!(cs.coordinators().len(), 2);
    assert!(cs.coordinators().iter().all(Coordinator::is_host));

    let cs = assert_round_trips_commented(
        "0xxdeadbeef:100100100@localhost:34534,host-name_part1.host-name_part2:1234:tls",
    );
    assert_eq!(
        cs.coordinators(),
        [
            host("localhost", 34534, false),
            host("host-name_part1.host-name_part2", 1234, true)
        ]
    );
}

#[test]
fn fdb_connection_string_from_hostnames() {
    // "/fdbclient/MonitorLeader/ConnectionString/hostname"
    let cs = ClusterConnectionString::new(
        "TestCluster",
        "0",
        [
            host("localhost", 1234, false),
            host("host-name", 5678, false),
        ],
    )
    .unwrap();
    assert_eq!(cs.coordinators().len(), 2);
    assert_eq!(
        cs.to_string(),
        "TestCluster:0@localhost:1234,host-name:5678"
    );
    assert_eq!(parse("TestCluster:0@localhost:1234,host-name:5678"), cs);

    assert_eq!(
        ClusterConnectionString::new(
            "TestCluster",
            "0",
            [
                host("localhost", 1234, false),
                host("localhost", 1234, false)
            ],
        )
        .unwrap_err(),
        ConnectionStringError::DuplicateCoordinator("localhost:1234".into())
    );
}

#[test]
fn fdb_partial_resolve_string_parses() {
    // The parse half of "/fdbclient/MonitorLeader/PartialResolve" (8.0: 299).
    let cs = parse("TestCluster:0@host.name:1234,host-name:5678");
    assert_eq!(
        cs.coordinators(),
        [
            host("host.name", 1234, false),
            host("host-name", 5678, false)
        ]
    );
}

/// Minimal deterministic generator for the ported fuzz test.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn random01(&mut self) -> f64 {
        (self.next() % 1_000_000) as f64 / 1_000_000.0
    }
    fn choice<T: Copy>(&mut self, items: &[T]) -> T {
        items[(self.next() % items.len() as u64) as usize]
    }
}

#[test]
fn fdb_parse_connection_string_fuzz() {
    // "/fdbclient/MonitorLeader/parseConnectionString/fuzz": sprinkle
    // whitespace and comments into a fixed string; the canonical form must
    // not change.
    let connection_string = "0xxdeadbeef:100100100@1.1.1.1:34534,5.1.5.3:23443";
    let comment_chars: Vec<char> = "asdfzxcv123345:!@#$#$&()<\"' \t".chars().collect();
    let mut rng = Lcg(0x5eed);
    for _ in 0..10_000 {
        let mut output = String::new();
        let mut chars = connection_string.chars().peekable();
        while chars.peek().is_some() {
            if rng.random01() < 0.1 {
                output.push(rng.choice(&[' ', '\t', '\n', '\r']));
            }
            if rng.random01() < 0.5 {
                output.extend(chars.next());
            }
            if rng.random01() < 0.1 {
                output.push('#');
                for _ in 0..(rng.next() % 20) {
                    output.push(rng.choice(&comment_chars));
                }
                output.push(rng.choice(&['\n', '\r']));
            }
        }
        assert_eq!(parse(&output).to_string(), connection_string, "{output:?}");
    }
}

// ---------------------------------------------------------------------------
// FDB's Hostname and NetworkAddress tests (flow/Hostname.cpp 7.4: 120-168,
// 8.0: 118-169; flow/network.cpp 7.4: 417, 8.0: 436).

#[test]
fn fdb_hostname_parse_and_is_hostname() {
    let cases = [
        ("localhost:1234", "localhost", 1234, false),
        ("host-name:1234", "host-name", 1234, false),
        ("host.name:1234", "host.name", 1234, false),
        (
            "host-name_part1.host-name_part2:1234:tls",
            "host-name_part1.host-name_part2",
            1234,
            true,
        ),
    ];
    for (s, name, port, tls) in cases {
        assert!(is_hostname(s), "{s}");
        let c = Coordinator::parse(s).unwrap();
        assert_eq!(c, host(name, port, tls));
        assert_eq!(c.to_string(), s);
    }

    for s in [
        "127.0.0.1:1234",
        "127.0.0.1:1234:tls",
        "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4800",
        "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4800:tls",
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334:tls",
        "[::1]:4800",
        "[::1]:4800:tls",
        "1234",
    ] {
        assert!(!is_hostname(s), "{s}");
    }
}

#[test]
fn fdb_network_address_ipv6_and_tls() {
    // "/flow/network/ipaddress"
    assert_eq!(addr("[::1]:4800", false).to_string(), "[::1]:4800");
    assert_eq!(
        Coordinator::parse("[::1]:4800").unwrap().to_string(),
        "[::1]:4800"
    );

    let c = Coordinator::parse("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4800").unwrap();
    assert!(matches!(c, Coordinator::Addr { addr, tls: false } if addr.is_ipv6()));
    assert_eq!(c.to_string(), "[2001:db8:85a3::8a2e:370:7334]:4800");

    let c = Coordinator::parse("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4800:tls(fromHostname)")
        .unwrap();
    assert!(c.tls());
    // `(fromHostname)` is accepted and dropped.
    assert_eq!(c.to_string(), "[2001:db8:85a3::8a2e:370:7334]:4800:tls");

    // IPAddress::parse on non-addresses fails: no brackets, no port.
    assert!(Coordinator::parse("2001").is_err());
    assert!(Coordinator::parse("8.8.8.8:12:garbage").is_err());
}

// ---------------------------------------------------------------------------
// Grammar details.

#[test]
fn key_rules() {
    // Empty description and/or ID are accepted by FDB's parseKey.
    for s in [
        ":@1.1.1.1:1",
        "a:@1.1.1.1:1",
        ":b@1.1.1.1:1",
        "_:0@1.1.1.1:1",
    ] {
        assert_eq!(parse(s).to_string(), s);
    }
    // Underscore is fine in the description only.
    assert_invalid("a:b_c@1.1.1.1:1");
    assert_eq!(parse("a_b:c@1.1.1.1:1").description(), "a_b");
    // Dash, dot, a second colon, non-ASCII letters.
    assert_invalid("a-b:c@1.1.1.1:1");
    assert_invalid("a.b:c@1.1.1.1:1");
    assert_invalid("a:b:c@1.1.1.1:1");
    assert_invalid("\u{e9}:c@1.1.1.1:1");
    assert_invalid("a:\u{e9}@1.1.1.1:1");
    // Missing colon (missing ID), missing @.
    assert_eq!(
        "abc@1.1.1.1:1"
            .parse::<ClusterConnectionString>()
            .unwrap_err(),
        ConnectionStringError::MissingKeyColon("abc".into())
    );
    assert_eq!(
        "a:b".parse::<ClusterConnectionString>().unwrap_err(),
        ConnectionStringError::MissingAt
    );
    assert_eq!(
        "".parse::<ClusterConnectionString>().unwrap_err(),
        ConnectionStringError::MissingAt
    );
    // Only the first '@' splits; a second one lands in a coordinator.
    assert_invalid("a:b@1.1.1.1:1@2.2.2.2:2");
    // No length limit.
    let long = format!("{}:{}@1.1.1.1:1", "d".repeat(5000), "9".repeat(5000));
    assert_eq!(parse(&long).to_string(), long);
}

#[test]
fn coordinator_list_rules() {
    for s in [
        "a:b@",
        "a:b@,",
        "a:b@1.1.1.1:1,",
        "a:b@,1.1.1.1:1",
        "a:b@1.1.1.1:1,,2.2.2.2:2",
    ] {
        assert_eq!(
            s.parse::<ClusterConnectionString>().unwrap_err(),
            ConnectionStringError::EmptyCoordinator,
            "{s}"
        );
    }
}

#[test]
fn bad_addresses_and_ports() {
    for s in [
        "a:b@1.1.1.1",           // no port
        "a:b@1.1.1.1:",          // empty port
        "a:b@1.1.1.1:65536",     // port out of range
        "a:b@1.1.1.1:-1",        // sign
        "a:b@1.1.1.1:+1",        // sign
        "a:b@1.1.1.1:1x",        // trailing garbage
        "a:b@1.1.1.1.1:1",       // five octets
        "a:b@256.1.1.1:1",       // octet out of range
        "a:b@1.1.1.1:1:tls:tls", // doubled suffix
        "a:b@1.1.1.1:1:TLS",     // suffix is case sensitive
        "a:b@1234:5",            // all digits, parsed as address
        "a:b@12.34.5:6",         // digit/dot host with >= 4 digits
        "a:b@[::1]",             // IPv6 without port
        "a:b@[::1]:",            // IPv6 empty port
        "a:b@[::1]5",            // missing colon after bracket
        "a:b@[::1:5",            // unterminated bracket
        "a:b@[::1]:70000",       // IPv6 port out of range
        "a:b@[::1]:5x",          // IPv6 trailing garbage
        "a:b@[::gg]:5",          // bad IPv6
        "a:b@::1:5",             // IPv6 without brackets
        "a:b@[]:5",              // empty brackets
        "a:b@host",              // hostname without port
        "a:b@host:",             // hostname empty port
        "a:b@host:port",         // non-numeric port
        "a:b@host:70000",        // hostname port out of range
        "a:b@host:1:TLS",
        "a:b@.host:1",        // leading dot
        "a:b@a..b:1",         // doubled dot
        "a:b@ho$t:1",         // illegal char
        "a:b@h\u{f6}st:1",    // non-ASCII
        "a:b@:1",             // empty host
        "a:b@(fromHostname)", // nothing left after the cut
    ] {
        assert_invalid(s);
    }
}

#[test]
fn hostname_vs_ip_classification() {
    // Digit-only names with at most three digits are hostnames.
    assert_eq!(parse("a:b@123:80").coordinators(), [host("123", 80, false)]);
    assert_eq!(
        parse("a:b@1.2.3:80").coordinators(),
        [host("1.2.3", 80, false)]
    );
    // Trailing dot is allowed by the regex, so this is a hostname, not an IP.
    assert_eq!(
        parse("a:b@1.1.1.:1").coordinators(),
        [host("1.1.1.", 1, false)]
    );
    // Trailing dot is allowed by the regex.
    assert_eq!(
        parse("a:b@host.:80").coordinators(),
        [host("host.", 80, false)]
    );
    // Names that merely contain digits are fine.
    assert_eq!(
        parse("a:b@1.2.3.4x:80").coordinators(),
        [host("1.2.3.4x", 80, false)]
    );
    assert_eq!(parse("a:b@-:80").coordinators(), [host("-", 80, false)]);
    // Leading zeros in ports are normalised.
    assert_eq!(parse("a:b@h:0080").to_string(), "a:b@h:80");
    assert_eq!(parse("a:b@001.2.3.04:0080").to_string(), "a:b@1.2.3.4:80");
    // Port bounds.
    assert_eq!(parse("a:b@1.1.1.1:0").to_string(), "a:b@1.1.1.1:0");
    assert_eq!(parse("a:b@1.1.1.1:65535").to_string(), "a:b@1.1.1.1:65535");
}

#[test]
fn tls_suffix_and_duplicates() {
    // TLS and plain are distinct coordinators.
    let cs = parse("a:b@1.1.1.1:1,1.1.1.1:1:tls,h:1,h:1:tls");
    assert_eq!(cs.coordinators().len(), 4);
    assert_eq!(
        cs.coordinators()
            .iter()
            .map(Coordinator::tls)
            .collect::<Vec<_>>(),
        [false, true, false, true]
    );
    // Different spellings of the same address are duplicates.
    assert_invalid("a:b@1.1.1.1:1,01.1.1.1:0001");
    assert_invalid("a:b@[::1]:1,[0:0:0:0:0:0:0:1]:1");
    assert_invalid("a:b@1.1.1.1:1:tls,1.1.1.1:1:tls");
    assert_invalid("a:b@h:1:tls,h:1:tls");
    // Hostnames are case sensitive, as in FDB.
    assert_eq!(parse("a:b@Host:1,host:1").coordinators().len(), 2);
    // A hostname and an IP never collide.
    assert_eq!(parse("a:b@1.1.1.1:1,one:1").coordinators().len(), 2);
}

#[test]
fn mixed_hosts_and_addresses_are_canonically_ordered() {
    let cs = parse("a:b@h1:1,1.1.1.1:2,h2:3:tls,[::1]:4");
    assert_eq!(cs.to_string(), "a:b@1.1.1.1:2,[::1]:4,h1:1,h2:3:tls");
    assert_eq!(parse(&cs.to_string()), cs);
    // `new` applies the same ordering.
    let built = ClusterConnectionString::new(
        "a",
        "b",
        [
            host("h1", 1, false),
            addr("1.1.1.1:2", false),
            host("h2", 3, true),
            addr("[::1]:4", false),
        ],
    )
    .unwrap();
    assert_eq!(built, cs);
}

#[test]
fn new_validates() {
    assert_eq!(
        ClusterConnectionString::new("a", "b", []).unwrap_err(),
        ConnectionStringError::EmptyCoordinator
    );
    assert!(matches!(
        ClusterConnectionString::new("a-", "b", [host("h", 1, false)]),
        Err(ConnectionStringError::InvalidDescription(_))
    ));
    assert!(matches!(
        ClusterConnectionString::new("a", "b_", [host("h", 1, false)]),
        Err(ConnectionStringError::InvalidId(_))
    ));
}

#[test]
fn comments_and_whitespace() {
    let want = parse("desc:ID@1.1.1.1:1,2.2.2.2:2");

    for input in [
        "desc:ID@1.1.1.1:1,2.2.2.2:2\n",
        "# DO NOT EDIT!\n# This file is auto-generated, it is not to be edited by hand\ndesc:ID@1.1.1.1:1,2.2.2.2:2\n",
        "  desc : ID @ 1.1.1.1:1 ,\t2.2.2.2:2  \r\n",
        "desc:ID@1.1.1.1:1,2.2.2.2:2 # trailing comment",
        "desc:ID@1.1.1.1:1,2.2.2.2:2#no space",
        "desc:ID@1.1.1.1:1,#comment, with, commas and @ and 3.3.3.3:3\n2.2.2.2:2",
        "#c\rdesc:ID@1.1.1.1:1,2.2.2.2:2",
        "#c1\n#c2\n\n\n desc:ID@1.1.1.1:1,\n2.2.2.2:2\n#c3",
        // Whitespace and comments inside a token vanish before parsing.
        "de sc:I\tD@1.1.#x\n1.1:1,2.2.2.2:2",
        "desc:ID@1.1.1.1:1,2.2.2.2:2\n#",
        "desc:ID@1.1.1.1:1,2.2.2.2:2#",
    ] {
        assert_eq!(parse(input), want, "{input:?}");
    }

    // Only the four FDB whitespace characters are removed.
    assert_invalid("desc:ID@1.1.1.1:1,\u{b}2.2.2.2:2");
    assert_invalid("desc:ID@1.1.1.1:1,\u{c}2.2.2.2:2");
    assert_invalid("desc:ID@1.1.1.1:1,\u{a0}2.2.2.2:2");
    // A comment hides an otherwise fatal problem; an empty remainder fails.
    assert_invalid("# desc:ID@1.1.1.1:1");
    assert_invalid("desc:ID#@1.1.1.1:1");
    assert_invalid("   \n\t ");
}

#[test]
fn bytes_in_comments_are_ignored() {
    let mut input = b"# \xff\xfe\x00 binary comment\ndesc:ID@1.1.1.1:1".to_vec();
    input.extend_from_slice(b" # \xc3\x28 \n");
    let cs = ClusterConnectionString::parse_bytes(&input).unwrap();
    assert_eq!(cs.to_string(), "desc:ID@1.1.1.1:1");

    // Outside comments, bytes that are not UTF-8 are just invalid.
    assert!(ClusterConnectionString::parse_bytes(b"desc:ID@1.1.1.1:1,\xff:1").is_err());
    assert!(ClusterConnectionString::parse_bytes(b"d\xffsc:ID@1.1.1.1:1").is_err());
}

#[test]
fn errors_display() {
    let e = "a:b@1.1.1.1:99999"
        .parse::<ClusterConnectionString>()
        .unwrap_err();
    assert_eq!(e.to_string(), "invalid coordinator `1.1.1.1:99999`");
}

// ---------------------------------------------------------------------------
// Discovery.

fn write_cluster_file(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
}

#[test]
fn discovery_explicit_wins() {
    let dir = tempfile::tempdir().unwrap();
    write_cluster_file(&dir.path().join("fdb.cluster"), "a:b@1.1.1.1:1");
    let loc = lookup_cluster_file_with(
        Some(Path::new("/explicit/path")),
        Some(OsStr::new("/from/env")),
        dir.path(),
        Path::new("/default"),
    );
    assert_eq!(loc.path, PathBuf::from("/explicit/path"));
    assert_eq!(loc.source, ClusterFileSource::Explicit);
    assert!(!loc.is_default());
}

#[test]
fn discovery_empty_explicit_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let loc = lookup_cluster_file_with(
        Some(Path::new("")),
        Some(OsStr::new("/from/env")),
        dir.path(),
        Path::new("/default"),
    );
    assert_eq!(loc.source, ClusterFileSource::EnvVar);
    assert_eq!(loc.path, PathBuf::from("/from/env"));
    assert!(!loc.is_default());
}

#[test]
fn discovery_env_var_is_final_even_if_wrong() {
    let dir = tempfile::tempdir().unwrap();
    write_cluster_file(&dir.path().join("fdb.cluster"), "a:b@1.1.1.1:1");
    // Points nowhere, empty, and the local file exists: still the env value.
    for value in ["/does/not/exist", ""] {
        let loc = lookup_cluster_file_with(
            None,
            Some(OsStr::new(value)),
            dir.path(),
            Path::new("/default"),
        );
        assert_eq!(loc.source, ClusterFileSource::EnvVar);
        assert_eq!(loc.path, PathBuf::from(value));
    }
    let err = ClusterFile::open_location(lookup_cluster_file_with(
        None,
        Some(OsStr::new("")),
        dir.path(),
        Path::new("/default"),
    ))
    .unwrap_err();
    assert!(matches!(
        err,
        ClusterFileError::NotFound {
            is_default: false,
            ..
        }
    ));
}

#[test]
fn discovery_current_dir_then_default() {
    let dir = tempfile::tempdir().unwrap();
    let default = dir.path().join("default.cluster");

    // Nothing local: the default path, whether or not it exists.
    let loc = lookup_cluster_file_with(None, None, dir.path(), &default);
    assert_eq!(loc.source, ClusterFileSource::PlatformDefault);
    assert_eq!(loc.path, default);
    assert!(loc.is_default());

    // A local fdb.cluster takes precedence over the default.
    write_cluster_file(&dir.path().join("fdb.cluster"), "a:b@1.1.1.1:1");
    write_cluster_file(&default, "other:id@2.2.2.2:2");
    let loc = lookup_cluster_file_with(None, None, dir.path(), &default);
    assert_eq!(loc.source, ClusterFileSource::CurrentDir);
    assert_eq!(loc.path, dir.path().join("fdb.cluster"));
    assert!(loc.is_default());
    let loaded = ClusterFile::open_location(loc).unwrap();
    assert_eq!(loaded.connection_string.to_string(), "a:b@1.1.1.1:1");

    // The relative form used for the real current directory.
    let loc = lookup_cluster_file_with(None, None, Path::new(""), Path::new("/nonexistent-dir"));
    assert!(matches!(
        loc.source,
        ClusterFileSource::CurrentDir | ClusterFileSource::PlatformDefault
    ));
    if loc.source == ClusterFileSource::CurrentDir {
        assert_eq!(loc.path, PathBuf::from("fdb.cluster"));
    }
}

#[test]
fn default_path_per_platform() {
    let p = default_cluster_file_path();
    assert_eq!(p.file_name().unwrap(), "fdb.cluster");
    #[cfg(target_os = "linux")]
    assert_eq!(p, PathBuf::from("/etc/foundationdb/fdb.cluster"));
    #[cfg(any(target_os = "macos", target_os = "freebsd"))]
    assert_eq!(p, PathBuf::from("/usr/local/etc/foundationdb/fdb.cluster"));
}

#[test]
fn public_lookup_honours_explicit_path() {
    // Independent of the environment: an explicit path always wins.
    let loc = lookup_cluster_file(Some(Path::new("some/where.cluster")));
    assert_eq!(loc.source, ClusterFileSource::Explicit);
    assert_eq!(loc.path, PathBuf::from("some/where.cluster"));
}

// ---------------------------------------------------------------------------
// Reading files.

#[test]
fn read_file_ok_and_open_or_default_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("my.cluster");
    write_cluster_file(
        &path,
        "# DO NOT EDIT!\n# This file is auto-generated, it is not to be edited by hand\n\
         test:abc123@127.0.0.1:4500,[::1]:4501:tls,db.example.com:4502\n",
    );
    let cs = read_cluster_file(&path).unwrap();
    assert_eq!(
        cs.to_string(),
        "test:abc123@127.0.0.1:4500,[::1]:4501:tls,db.example.com:4502"
    );
    let cf = ClusterFile::open_or_default(Some(&path)).unwrap();
    assert_eq!(cf.connection_string, cs);
    assert_eq!(cf.location.source, ClusterFileSource::Explicit);
}

#[test]
fn read_file_errors() {
    let dir = tempfile::tempdir().unwrap();

    let missing = dir.path().join("missing");
    let err = read_cluster_file(&missing).unwrap_err();
    assert!(matches!(err, ClusterFileError::NotFound { .. }));
    assert!(err.to_string().starts_with("Unable to read cluster file `"));

    let bad = dir.path().join("bad");
    write_cluster_file(&bad, "no at sign here\n");
    let err = read_cluster_file(&bad).unwrap_err();
    assert!(matches!(
        err,
        ClusterFileError::Invalid {
            source: ConnectionStringError::MissingAt,
            ..
        }
    ));
    assert!(err.to_string().starts_with("Invalid cluster file `"));
    assert!(std::error::Error::source(&err).is_some());

    let empty = dir.path().join("empty");
    write_cluster_file(&empty, "");
    assert!(matches!(
        read_cluster_file(&empty).unwrap_err(),
        ClusterFileError::Invalid { .. }
    ));

    // A directory opens but cannot be read.
    assert!(read_cluster_file(dir.path()).is_err());
}

#[test]
fn read_file_size_limit() {
    let dir = tempfile::tempdir().unwrap();
    let body = "a:b@1.1.1.1:1\n";

    // Exactly MAX_CLUSTER_FILE_BYTES is accepted (padding is a comment).
    let mut ok = String::from("#");
    ok.push_str(&"x".repeat(MAX_CLUSTER_FILE_BYTES - body.len() - 2));
    ok.push('\n');
    ok.push_str(body);
    assert_eq!(ok.len(), MAX_CLUSTER_FILE_BYTES);
    let path = dir.path().join("max");
    write_cluster_file(&path, &ok);
    assert_eq!(
        read_cluster_file(&path).unwrap().to_string(),
        "a:b@1.1.1.1:1"
    );

    // One byte more is file_too_large.
    ok.push('\n');
    let path = dir.path().join("big");
    write_cluster_file(&path, &ok);
    let err = read_cluster_file(&path).unwrap_err();
    assert!(matches!(err, ClusterFileError::TooLarge { .. }), "{err}");

    // The string parser itself has no limit.
    assert!(ok.parse::<ClusterConnectionString>().is_ok());
}

#[test]
fn not_found_message_for_default_lookup() {
    let loc = ClusterFileLocation {
        path: PathBuf::from("/nonexistent-dir/fdb.cluster"),
        source: ClusterFileSource::PlatformDefault,
    };
    let err = ClusterFile::open_location(loc).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.starts_with("Unable to read cluster file `./fdb.cluster' or `"),
        "{msg}"
    );
    assert!(msg.contains("FDB_CLUSTER_FILE unset"), "{msg}");
}

// ---------------------------------------------------------------------------
// Property tests.

fn arb_description() -> impl Strategy<Value = String> {
    "[A-Za-z0-9_]{0,24}"
}

fn arb_id() -> impl Strategy<Value = String> {
    "[A-Za-z0-9]{0,24}"
}

fn arb_coordinator() -> impl Strategy<Value = Coordinator> {
    prop_oneof![
        (any::<[u8; 4]>(), any::<u16>(), any::<bool>()).prop_map(|(ip, port, tls)| {
            Coordinator::Addr {
                addr: SocketAddr::from((ip, port)),
                tls,
            }
        }),
        (any::<[u16; 8]>(), any::<u16>(), any::<bool>()).prop_map(|(segments, port, tls)| {
            Coordinator::Addr {
                addr: SocketAddr::from((segments, port)),
                tls,
            }
        }),
        // Starts with a letter so it can never look like an IPv4 address.
        (
            "[A-Za-z_-][A-Za-z0-9_-]{0,8}(\\.[A-Za-z0-9_-]{1,8}){0,2}",
            any::<u16>(),
            any::<bool>()
        )
            .prop_map(|(name, port, tls)| Coordinator::Host { name, port, tls }),
    ]
}

fn arb_connection_string() -> impl Strategy<Value = ClusterConnectionString> {
    (
        arb_description(),
        arb_id(),
        prop::collection::vec(arb_coordinator(), 1..8),
    )
        .prop_map(|(description, id, coordinators)| {
            let mut seen = HashSet::new();
            let unique = coordinators.into_iter().filter(|c| seen.insert(c.clone()));
            ClusterConnectionString::new(&description, &id, unique).unwrap()
        })
}

proptest! {
    #[test]
    fn round_trip(cs in arb_connection_string()) {
        let text = cs.to_string();
        let again: ClusterConnectionString = text.parse().unwrap();
        prop_assert_eq!(&again, &cs);
        prop_assert_eq!(again.to_string(), text);
    }

    #[test]
    fn round_trip_with_noise(
        cs in arb_connection_string(),
        noise in prop::collection::vec((0usize..200, "[ \t\r\n]{0,3}", "[a-z0-9:@,# ]{0,10}"), 0..8),
    ) {
        // Insert whitespace and comments at arbitrary positions of the
        // original text. Inserting from the highest position down keeps every
        // insertion point inside the original text, never inside a comment
        // added earlier, and each comment ends with the newline we add.
        let text = cs.to_string();
        let mut chars: Vec<char> = text.chars().collect();
        let slots = chars.len() + 1;
        let mut noise: Vec<_> = noise.into_iter().map(|(p, w, c)| (p % slots, w, c)).collect();
        noise.sort_by_key(|(at, _, _)| std::cmp::Reverse(*at));
        for (at, ws, comment) in noise {
            let insert: Vec<char> = format!("{ws}#{comment}\n").chars().collect();
            chars.splice(at..at, insert);
        }
        let noisy: String = chars.into_iter().collect();
        let parsed: ClusterConnectionString = noisy.parse().unwrap();
        prop_assert_eq!(parsed, cs);
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..300)) {
        let _ = ClusterConnectionString::parse_bytes(&bytes);
    }

    #[test]
    fn arbitrary_text_never_panics(s in "\\PC{0,200}") {
        let _ = s.parse::<ClusterConnectionString>();
        let _ = Coordinator::parse(&s);
    }

    #[test]
    fn grammar_shaped_text_never_panics(
        s in "[a-zA-Z0-9_:@,.\\[\\]# \\n()%+-]{0,80}"
    ) {
        if let Ok(cs) = s.parse::<ClusterConnectionString>() {
            // Anything accepted must be a fixed point of print and parse.
            let printed = cs.to_string();
            prop_assert_eq!(printed.parse::<ClusterConnectionString>().unwrap(), cs);
        }
    }
}
