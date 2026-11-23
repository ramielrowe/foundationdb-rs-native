//! Cluster file and connection string parsing, and cluster-file discovery.
//!
//! This module reproduces what libfdb_c does with `ClusterConnectionString`
//! and `ClusterConnectionFile`, without any I/O beyond reading the file.
//! Parsing is pure: hostnames are kept unresolved as
//! [`Coordinator::Host`] and are resolved later through the transport
//! runtime's DNS hook.
//!
//! # Connection string grammar
//!
//! ```text
//! description:ID@coordinator,coordinator,...
//! coordinator := IPv4:port[:tls] | [IPv6]:port[:tls] | hostname:port[:tls]
//! ```
//!
//! # FDB rules reproduced here
//!
//! Citations are `file:line` on `origin/release-7.4` (7.4) and
//! `origin/release-8.0` (8.0) of the FoundationDB repository. The 7.4 files
//! carry `.actor.cpp` names, the 8.0 ones `.cpp`, since 8.0 replaced the actor
//! compiler by C++20 coroutines.
//!
//! ## Pre-processing (`trim`)
//!
//! `fdbclient/MonitorLeader[.actor].cpp`, anonymous-namespace `trim` (7.4:
//! 34-50, 8.0: 37-54), called first thing by the
//! `ClusterConnectionString(const std::string&)` constructor (7.4: 87-126,
//! 8.0: 91-130).
//!
//! * Everything from a `#` up to, but not including, the next `\n` or `\r` is
//!   dropped. A comment may therefore start anywhere, including in the middle
//!   of a coordinator, and an unterminated comment runs to end of input.
//! * Every space, `\t`, `\n` and `\r` is deleted, wherever it occurs (also
//!   inside tokens: `"a b:1@..."` is `"ab:1@..."`). No other whitespace
//!   (`\x0b`, `\x0c`, Unicode) is special.
//! * The input is a byte string. Comments may hold arbitrary bytes, so
//!   [`ClusterConnectionString::parse_bytes`] accepts `&[u8]`.
//! * The documented rule is "all characters on a line after the first `#`
//!   are ignored" (`documentation/sphinx/source/administration.rst` 8.0:
//!   140, "Cluster file format").
//!
//! ## Key (`description:ID`)
//!
//! The text before the *first* `@` is the key (no `@` at all is invalid).
//! `ClusterConnectionString::parseKey` (7.4: 409-436, 8.0: 412-439):
//!
//! * the first `:` splits the key into `description` and `ID`; no `:` is
//!   invalid;
//! * `description` may contain only `[A-Za-z0-9_]` (`isalnum` or `_`);
//! * `ID` may contain only `[A-Za-z0-9]`, so a second `:` is invalid;
//! * there is **no length limit** and **either part may be empty**
//!   (`":@1.1.1.1:1"` is accepted by FDB, and by this parser). The docs say
//!   "alphanumeric characters (a-z, A-Z, 0-9) and underscores"
//!   (`documentation/sphinx/source/guide-common.rst.inc` 8.0: 6-10).
//!
//! ## Coordinators
//!
//! The text after the first `@` is split on every `,`. A trailing comma, a
//! leading comma, an empty list and `",,"` all yield an empty entry, which is
//! invalid. For each entry (7.4: 101-124, 8.0: 105-128):
//!
//! 1. `Hostname::isHostname` (`flow/Hostname[.actor].cpp`, 7.4: 32-44,
//!    8.0: 30-42) decides between hostname and address. It is
//!    `!regex_match(s, ipv4Validation) && regex_match(s, validation)` with
//!    `validation = ^([\w\-]+\.?)+:([\d]+){1,}(:tls)?$` and
//!    `ipv4Validation = ^([\d]{1,3}\.?){4,}:([\d]+){1,}(:tls)?$`. So a host
//!    is a non-empty run of `[A-Za-z0-9_-]` and `.` that does not start with
//!    `.` and has no `..` (a trailing `.` is fine), followed by `:`, one or
//!    more digits and an optional literal `:tls`. A host made only of digits
//!    and dots with **four or more digits in total** (`1.2.3.4`, `12.34.5`,
//!    `1234`) is treated as an IPv4 address and, if that fails, is invalid.
//!    All-digit names with three or fewer digits (`123:80`) are hostnames.
//! 2. A hostname is split by `Hostname::parse` (7.4: 46-61, 8.0: 44-59):
//!    `:tls` is recognised as a suffix (length > 4) and stripped, then the
//!    first `:` separates host and service. The service is kept as a *string*
//!    and is not range-checked by FDB.
//! 3. Anything else goes to `NetworkAddress::parse` (`flow/network.cpp`, 7.4:
//!    148-186, 8.0: 160-198): empty is invalid; everything from a literal
//!    `(fromHostname)` is cut off (and sets a flag); a `:tls` suffix (length >
//!    4) is stripped; `[ipv6]:port` when it starts with `[` (the first `]`
//!    must be followed by `:`); otherwise `sscanf("%d.%d.%d.%d:%d%n")` which
//!    must consume the whole string.
//!
//! Duplicates are detected with `std::set`s, separately for addresses and
//! hostnames (7.4: 98-122, 8.0: 102-126; vector constructors 7.4: 390-407,
//! 8.0: 393-410). Identity is `(ip, port, tls)` for addresses
//! (`NetworkAddress::operator==`/`<`, `flow/include/flow/NetworkAddress.h`
//! 61-69 on both branches; the `fromHostname` flag is not part of it) and
//! `(host, service, tls)` for hostnames (`Hostname::operator==`/`<`,
//! `flow/include/flow/Hostname.h` 38-46 on both branches). So `1.1.1.1:1` and
//! `1.1.1.1:1:tls` are different coordinators, and hostname comparison is
//! case sensitive.
//!
//! Hostnames and IP addresses **may be mixed** freely. (The 8.0 admin docs
//! still say "IP addresses (not hostnames)" in "Cluster file format"; the
//! code accepts both, and the unit tests below are FDB's own.)
//!
//! ## Canonical form (`toString`)
//!
//! `ClusterConnectionString::toString` (7.4: 438-454, 8.0: 441-457): `key`,
//! `@`, then all **addresses** in input order, then all **hostnames** in
//! input order, comma separated. Mixed input is therefore re-ordered:
//! `"a:b@h:1,1.1.1.1:2"` prints as `"a:b@1.1.1.1:2,h:1"`.
//! [`ClusterConnectionString`] stores its coordinators in that canonical
//! order. Each address prints as `ip:port` / `[ip6]:port` (IPv6 in the
//! compressed form `boost::asio` produces, `NetworkAddress::toString`, 7.4:
//! 211-217, 8.0: 223-229) plus `:tls`; each hostname as `host:service[:tls]`
//! (`Hostname::toString`, `Hostname.h` 59).
//!
//! ## Cluster files
//!
//! `ClusterConnectionFile::ClusterConnectionFile(filename)`
//! (`fdbclient/ClusterConnectionFile[.actor].cpp`, 7.4: 26-34, 8.0: 25-33):
//! a file that cannot be opened is `no_cluster_file_found`; otherwise at most
//! `MAX_CLUSTER_FILE_BYTES = 60000` bytes are read
//! (`fdbclient/include/fdbclient/CoordinationInterface.h` 33 on both
//! branches; `readFileBytes` in `flow/Platform[.actor].cpp`, 7.4: 3245-3265,
//! 8.0: 3095-3114, throws `file_too_large` above that) and handed to the
//! connection-string constructor. The `# DO NOT EDIT!` header that
//! `persist` writes is just a comment.
//!
//! ## Discovery (`lookupClusterFileName`)
//!
//! `ClusterConnectionFile::lookupClusterFileName` (7.4: 107-123, 8.0:
//! 106-122), used by `openOrDefault` (7.4: 45-47, 8.0: 44-46) which
//! `Database::createDatabase(std::string connFileName, ...)` calls
//! (`fdbclient/NativeAPI.actor.cpp` 7.4: 2689):
//!
//! 1. a non-empty explicit path (an empty one counts as "none");
//! 2. the `FDB_CLUSTER_FILE` environment variable if it is **set at all**
//!    (`getenv != NULL`, `flow/Platform[.actor].cpp` 7.4: 3286-3293, 8.0:
//!    3136-3143; `CLUSTER_FILE_ENV_VAR_NAME`, `fdbclient/include/fdbclient/
//!    MonitorLeader.h` 7.4: 30, 8.0: 31). An empty value is used as is and
//!    leads to "not found"; there is no fall-through to later steps
//!    (comment at 7.4: 114-115, doc warning at `administration.rst` 8.0: 116);
//! 3. `./fdb.cluster`, if it can be opened (`fileExists` is `fopen(..., "rb")`
//!    succeeding, `flow/Platform` 7.4: 3167-3173, 8.0: 3017-3023);
//! 4. `platform::getDefaultClusterFilePath()` = `getDefaultConfigPath()` +
//!    `fdb.cluster` (7.4: 3350-3369, 8.0: 3200-3219): `/etc/foundationdb` on
//!    Linux, `/usr/local/etc/foundationdb` on macOS and FreeBSD,
//!    `%ProgramData%\foundationdb` (`CSIDL_COMMON_APPDATA`) on Windows.
//!
//! The returned pair also says whether the result was a default
//! (`true` for steps 3 and 4), which only affects the error message
//! (`getErrorString`, 7.4: 126-145, 8.0: 126-148); see
//! [`ClusterFileLocation::is_default`].
//!
//! # 7.4 versus 8.0
//!
//! There is **no behavioural difference** in anything this module covers.
//! `diff` of the relevant 7.4 and 8.0 sources shows only: the actor-compiler
//! to coroutine conversion, file renames (`.actor.cpp` to `.cpp`), added
//! braces, `.size()` to `.empty()`, copyright years, and unrelated additions
//! in `MonitorLeader.cpp` (`getLeader` priority handling and its test). The
//! four `ClusterConnectionString` unit tests (`parseConnectionString/
//! addresses`, `.../hostnames`, `ConnectionString/hostname`, `.../fuzz`;
//! 7.4: 128, 188, 241, 360; 8.0: 132, 192, 245, 363), the `Hostname` and
//! `NetworkAddress` tests (`Hostname.cpp` 7.4: 120-168, 8.0: 118-169;
//! `network.cpp` 7.4: 417, 8.0: 436) and the `/fdbclient/MonitorLeader/
//! PartialResolve` hostname test (7.4: 294, 8.0: 299) are the same, bar the rename of
//! `/flow/Hostname/hostname` to `/flow/Hostname/parse` in 8.0.
//!
//! # Deliberate differences from FDB
//!
//! FDB accepts some malformed inputs through accidents of `std::stoi`,
//! `sscanf` and integer truncation. This parser rejects them instead (they
//! would never come out of `toString`):
//!
//! * IPv4 octets are decimal digits with value 0-255 (FDB's `%d` takes a sign,
//!   leading whitespace, and computes `(a << 24) + ...` without range
//!   checks). Leading zeros are accepted, as in FDB, and normalised away on
//!   output.
//! * Ports are decimal digits with value 0-65535. FDB `stoi`s IPv6 ports,
//!   ignoring trailing garbage and truncating to 16 bits, and takes IPv4
//!   ports through `%d`. For hostnames FDB keeps the service as an arbitrary
//!   digit string; here it is a `u16`, so `host:99999` is rejected at parse
//!   time (FDB would fail later in `getaddrinfo`), and `host:01` equals
//!   `host:1` for duplicate detection and prints as `host:1`.
//! * IPv6 text is parsed by [`std::net::Ipv6Addr`], which rejects scope ids
//!   (`%eth0`) that `boost::asio::ip::address::from_string` allows. Output
//!   formatting of IPv4-compatible addresses (`::1.2.3.4`) may differ from
//!   boost (`::102:304` against `::1.2.3.4`); the value round-trips either way.
//! * `(fromHostname)` is accepted and dropped rather than reproduced in the
//!   output. It cannot occur in a cluster file written by FDB's client path.

use std::collections::HashSet;
use std::ffi::OsStr;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Name of the environment variable that selects the cluster file
/// (`CLUSTER_FILE_ENV_VAR_NAME`).
pub const CLUSTER_FILE_ENV_VAR: &str = "FDB_CLUSTER_FILE";

/// File name probed in the current directory and appended to the platform
/// config directory.
pub const CLUSTER_FILE_NAME: &str = "fdb.cluster";

/// Largest cluster file that is read (`MAX_CLUSTER_FILE_BYTES`).
pub const MAX_CLUSTER_FILE_BYTES: usize = 60_000;

/// Why a connection string was rejected. All variants correspond to FDB's
/// single `connection_string_invalid` (2104) error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectionStringError {
    /// There is no `@` between the key and the coordinator list.
    #[error("connection string has no '@' between the key and the coordinators")]
    MissingAt,
    /// The key has no `:` between description and ID.
    #[error("connection string key `{0}` has no ':' between description and ID")]
    MissingKeyColon(String),
    /// The description contains something other than `[A-Za-z0-9_]`.
    #[error("description `{0}` may only contain a-z, A-Z, 0-9 and '_'")]
    InvalidDescription(String),
    /// The ID contains something other than `[A-Za-z0-9]`.
    #[error("ID `{0}` may only contain a-z, A-Z and 0-9")]
    InvalidId(String),
    /// The coordinator list is empty, or has an empty entry (leading,
    /// trailing or doubled comma).
    #[error("coordinator list is empty or contains an empty entry")]
    EmptyCoordinator,
    /// A coordinator is neither a valid address nor a valid hostname.
    #[error("invalid coordinator `{0}`")]
    InvalidCoordinator(String),
    /// A coordinator appears twice.
    #[error("duplicate coordinator `{0}`")]
    DuplicateCoordinator(String),
}

/// One coordinator of a cluster, unresolved.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Coordinator {
    /// A literal IP address and port.
    Addr {
        /// IPv4 or IPv6 socket address.
        addr: SocketAddr,
        /// Whether the `:tls` suffix was present.
        tls: bool,
    },
    /// A DNS name, to be resolved through the transport runtime.
    Host {
        /// The host name exactly as written (case preserved).
        name: String,
        /// The port.
        port: u16,
        /// Whether the `:tls` suffix was present.
        tls: bool,
    },
}

impl Coordinator {
    /// Whether this coordinator uses TLS.
    pub fn tls(&self) -> bool {
        match self {
            Self::Addr { tls, .. } | Self::Host { tls, .. } => *tls,
        }
    }

    /// Whether this coordinator is a hostname that still needs resolving.
    pub fn is_host(&self) -> bool {
        matches!(self, Self::Host { .. })
    }

    /// Parses a single coordinator the way one comma-separated entry of a
    /// connection string is parsed. No whitespace or comment handling is done.
    pub fn parse(entry: &str) -> Result<Self, ConnectionStringError> {
        parse_coordinator(entry.as_bytes())
    }
}

impl fmt::Display for Coordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Addr { addr, tls } => {
                write!(f, "{addr}")?;
                if *tls {
                    f.write_str(":tls")?;
                }
            }
            Self::Host { name, port, tls } => {
                write!(f, "{name}:{port}")?;
                if *tls {
                    f.write_str(":tls")?;
                }
            }
        }
        Ok(())
    }
}

impl FromStr for Coordinator {
    type Err = ConnectionStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// A parsed FDB connection string (`description:ID@coord,coord,...`).
///
/// Coordinators are held in FDB's canonical order: all addresses in input
/// order, then all hostnames in input order. [`Display`](fmt::Display) prints
/// FDB's `toString()`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterConnectionString {
    description: String,
    id: String,
    coordinators: Vec<Coordinator>,
}

impl ClusterConnectionString {
    /// Builds a connection string from parts, validating them exactly like a
    /// parsed one (character sets, duplicates) and additionally requiring at
    /// least one coordinator. Coordinators are put in canonical order.
    pub fn new(
        description: &str,
        id: &str,
        coordinators: impl IntoIterator<Item = Coordinator>,
    ) -> Result<Self, ConnectionStringError> {
        check_description(description.as_bytes())?;
        check_id(id.as_bytes())?;
        let mut seen = HashSet::new();
        let (mut addrs, mut hosts) = (Vec::new(), Vec::new());
        for c in coordinators {
            if !seen.insert(c.clone()) {
                return Err(ConnectionStringError::DuplicateCoordinator(c.to_string()));
            }
            if c.is_host() {
                hosts.push(c);
            } else {
                addrs.push(c);
            }
        }
        addrs.append(&mut hosts);
        if addrs.is_empty() {
            return Err(ConnectionStringError::EmptyCoordinator);
        }
        Ok(Self {
            description: description.to_owned(),
            id: id.to_owned(),
            coordinators: addrs,
        })
    }

    /// Parses a connection string or the contents of a cluster file,
    /// including `#` comments and whitespace, from raw bytes.
    ///
    /// Never panics. There is no size limit here; [`read_cluster_file`]
    /// enforces [`MAX_CLUSTER_FILE_BYTES`].
    pub fn parse_bytes(input: &[u8]) -> Result<Self, ConnectionStringError> {
        let trimmed = strip_comments_and_whitespace(input);
        let at = trimmed
            .iter()
            .position(|&b| b == b'@')
            .ok_or(ConnectionStringError::MissingAt)?;
        let (description, id) = parse_key(&trimmed[..at])?;

        let mut seen = HashSet::new();
        let (mut addrs, mut hosts) = (Vec::new(), Vec::new());
        for entry in trimmed[at + 1..].split(|&b| b == b',') {
            let coordinator = parse_coordinator(entry)?;
            if !seen.insert(coordinator.clone()) {
                return Err(ConnectionStringError::DuplicateCoordinator(
                    coordinator.to_string(),
                ));
            }
            if coordinator.is_host() {
                hosts.push(coordinator);
            } else {
                addrs.push(coordinator);
            }
        }
        addrs.append(&mut hosts);
        Ok(Self {
            description,
            id,
            coordinators: addrs,
        })
    }

    /// The description part of the key.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// The ID part of the key.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The full key, `description:ID` (`ClusterConnectionString::clusterKey`).
    pub fn key(&self) -> String {
        format!("{}:{}", self.description, self.id)
    }

    /// The coordinators in canonical order (addresses, then hostnames).
    pub fn coordinators(&self) -> &[Coordinator] {
        &self.coordinators
    }
}

impl fmt::Display for ClusterConnectionString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}@", self.description, self.id)?;
        for (i, c) in self.coordinators.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{c}")?;
        }
        Ok(())
    }
}

impl FromStr for ClusterConnectionString {
    type Err = ConnectionStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_bytes(s.as_bytes())
    }
}

// ---------------------------------------------------------------------------
// Parsing internals.

/// `trim` in MonitorLeader.cpp: drop `#` comments (to, not including, the next
/// `\n` or `\r`) and every space, tab, CR and LF.
fn strip_comments_and_whitespace(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        match input[i] {
            b'#' => {
                while i < input.len() && input[i] != b'\n' && input[i] != b'\r' {
                    i += 1;
                }
            }
            b' ' | b'\t' | b'\n' | b'\r' => i += 1,
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    out
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn check_description(desc: &[u8]) -> Result<(), ConnectionStringError> {
    if desc.iter().all(|&b| b.is_ascii_alphanumeric() || b == b'_') {
        Ok(())
    } else {
        Err(ConnectionStringError::InvalidDescription(lossy(desc)))
    }
}

fn check_id(id: &[u8]) -> Result<(), ConnectionStringError> {
    if id.iter().all(u8::is_ascii_alphanumeric) {
        Ok(())
    } else {
        Err(ConnectionStringError::InvalidId(lossy(id)))
    }
}

/// `ClusterConnectionString::parseKey`.
fn parse_key(key: &[u8]) -> Result<(String, String), ConnectionStringError> {
    let colon = key
        .iter()
        .position(|&b| b == b':')
        .ok_or_else(|| ConnectionStringError::MissingKeyColon(lossy(key)))?;
    let (desc, id) = (&key[..colon], &key[colon + 1..]);
    check_description(desc)?;
    check_id(id)?;
    // Both are ASCII after the checks above.
    Ok((lossy(desc), lossy(id)))
}

fn parse_coordinator(entry: &[u8]) -> Result<Coordinator, ConnectionStringError> {
    if entry.is_empty() {
        return Err(ConnectionStringError::EmptyCoordinator);
    }
    let invalid = || ConnectionStringError::InvalidCoordinator(lossy(entry));
    let s = std::str::from_utf8(entry).map_err(|_| invalid())?;
    let parsed = if is_hostname(s) {
        parse_hostname(s)
    } else {
        parse_address(s)
    };
    parsed.ok_or_else(invalid)
}

/// `Hostname::isHostname`, evaluated without a regex engine.
fn is_hostname(s: &str) -> bool {
    let Some((host, rest)) = s.split_once(':') else {
        return false;
    };
    let port = rest.strip_suffix(":tls").unwrap_or(rest);
    if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    // ^([\w\-]+\.?)+$ : word characters and dashes, optionally followed by a
    // single dot, repeated: non-empty, no leading dot, no "..".
    let host = host.as_bytes();
    if host.is_empty()
        || host[0] == b'.'
        || !host
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        || host.windows(2).any(|w| w == b"..")
    {
        return false;
    }
    // ipv4Validation: ([\d]{1,3}\.?){4,} matches exactly the digit/dot hosts
    // with at least four digits (each digit can be its own group).
    let only_digits_and_dots = host.iter().all(|&b| b.is_ascii_digit() || b == b'.');
    let digit_count = host.iter().filter(|b| b.is_ascii_digit()).count();
    !(only_digits_and_dots && digit_count >= 4)
}

/// `Hostname::parse`, after `is_hostname` accepted the string.
fn parse_hostname(s: &str) -> Option<Coordinator> {
    let (f, tls) = strip_tls(s);
    let (name, port) = f.split_once(':')?;
    Some(Coordinator::Host {
        name: name.to_owned(),
        port: parse_port(port)?,
        tls,
    })
}

/// A `:tls` suffix counts only when the string is longer than four bytes.
fn strip_tls(s: &str) -> (&str, bool) {
    match s.strip_suffix(":tls") {
        Some(rest) if s.len() > 4 => (rest, true),
        _ => (s, false),
    }
}

/// `NetworkAddress::parse` (without the 16-bit truncation quirks).
fn parse_address(s: &str) -> Option<Coordinator> {
    let s = s.find("(fromHostname)").map_or(s, |pos| &s[..pos]);
    let (f, tls) = strip_tls(s);
    let addr = if f.starts_with('[') {
        let end = f.find(']')?;
        let port = f[end + 1..].strip_prefix(':')?;
        let ip = IpAddr::from_str(&f[1..end]).ok()?;
        SocketAddr::new(ip, parse_port(port)?)
    } else {
        let (ip, port) = f.split_once(':')?;
        let mut octets = [0u8; 4];
        let mut parts = ip.split('.');
        for octet in &mut octets {
            *octet = parse_octet(parts.next()?)?;
        }
        if parts.next().is_some() {
            return None;
        }
        SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), parse_port(port)?)
    };
    Some(Coordinator::Addr { addr, tls })
}

fn parse_port(s: &str) -> Option<u16> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn parse_octet(s: &str) -> Option<u8> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

// ---------------------------------------------------------------------------
// Cluster file discovery and loading.

/// Which step of the lookup chose a cluster file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ClusterFileSource {
    /// A non-empty path was passed explicitly.
    Explicit,
    /// The `FDB_CLUSTER_FILE` environment variable (set, possibly empty).
    EnvVar,
    /// `fdb.cluster` in the current directory.
    CurrentDir,
    /// The platform default path.
    PlatformDefault,
}

/// The result of cluster-file discovery: `lookupClusterFileName`'s pair of
/// resolved name and was-default flag.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterFileLocation {
    /// The chosen path. It may not exist.
    pub path: PathBuf,
    /// Which lookup step produced it.
    pub source: ClusterFileSource,
}

impl ClusterFileLocation {
    /// FDB's `isDefaultFile`: true when the path came from `./fdb.cluster` or
    /// the platform default, false for an explicit path or the environment
    /// variable.
    pub fn is_default(&self) -> bool {
        matches!(
            self.source,
            ClusterFileSource::CurrentDir | ClusterFileSource::PlatformDefault
        )
    }
}

/// `platform::getDefaultClusterFilePath()`: the system-dependent default
/// location (`/etc/foundationdb/fdb.cluster` on Linux,
/// `/usr/local/etc/foundationdb/fdb.cluster` on macOS and FreeBSD,
/// `%ProgramData%\foundationdb\fdb.cluster` on Windows).
pub fn default_cluster_file_path() -> PathBuf {
    default_config_dir().join(CLUSTER_FILE_NAME)
}

#[cfg(windows)]
fn default_config_dir() -> PathBuf {
    let base = std::env::var_os("ProgramData").unwrap_or_else(|| "C:\\ProgramData".into());
    PathBuf::from(base).join("foundationdb")
}

#[cfg(any(target_os = "macos", target_os = "freebsd"))]
fn default_config_dir() -> PathBuf {
    PathBuf::from("/usr/local/etc/foundationdb")
}

#[cfg(not(any(windows, target_os = "macos", target_os = "freebsd")))]
fn default_config_dir() -> PathBuf {
    PathBuf::from("/etc/foundationdb")
}

/// `ClusterConnectionFile::lookupClusterFileName` against the real process
/// environment and current directory.
///
/// The returned path is not checked for existence, except that `./fdb.cluster`
/// is only chosen when it can be opened.
pub fn lookup_cluster_file(explicit: Option<&Path>) -> ClusterFileLocation {
    lookup_cluster_file_with(
        explicit,
        std::env::var_os(CLUSTER_FILE_ENV_VAR).as_deref(),
        Path::new(""),
        &default_cluster_file_path(),
    )
}

/// The lookup chain with every input made explicit, so it can be tested
/// without touching process-global state.
///
/// * `env_value` is the value of `FDB_CLUSTER_FILE`, `None` when unset.
/// * `local_dir` is the directory in which `fdb.cluster` is probed (the
///   current directory is the empty path).
/// * `default_path` is used when nothing else matched.
pub fn lookup_cluster_file_with(
    explicit: Option<&Path>,
    env_value: Option<&OsStr>,
    local_dir: &Path,
    default_path: &Path,
) -> ClusterFileLocation {
    if let Some(path) = explicit.filter(|p| !p.as_os_str().is_empty()) {
        return ClusterFileLocation {
            path: path.to_path_buf(),
            source: ClusterFileSource::Explicit,
        };
    }
    if let Some(value) = env_value {
        // Set but wrong stays wrong: no fall-back to the later steps.
        return ClusterFileLocation {
            path: PathBuf::from(value),
            source: ClusterFileSource::EnvVar,
        };
    }
    let local = local_dir.join(CLUSTER_FILE_NAME);
    if File::open(&local).is_ok() {
        return ClusterFileLocation {
            path: local,
            source: ClusterFileSource::CurrentDir,
        };
    }
    ClusterFileLocation {
        path: default_path.to_path_buf(),
        source: ClusterFileSource::PlatformDefault,
    }
}

/// Why a cluster file could not be loaded (`ClusterConnectionFile::
/// getErrorString`).
#[derive(Debug)]
#[non_exhaustive]
pub enum ClusterFileError {
    /// The file could not be opened (`no_cluster_file_found`).
    NotFound {
        /// The path that was tried.
        path: PathBuf,
        /// Whether the path came from the default lookup steps.
        is_default: bool,
        /// The underlying error.
        source: io::Error,
    },
    /// The file is larger than [`MAX_CLUSTER_FILE_BYTES`] (`file_too_large`).
    TooLarge {
        /// The offending file.
        path: PathBuf,
    },
    /// The file was opened but reading it failed.
    Io {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// The contents are not a valid connection string.
    Invalid {
        /// The file.
        path: PathBuf,
        /// What is wrong with it.
        source: ConnectionStringError,
    },
}

impl fmt::Display for ClusterFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound {
                is_default: true,
                source,
                ..
            } => write!(
                f,
                "Unable to read cluster file `./{CLUSTER_FILE_NAME}' or `{}' and \
                 {CLUSTER_FILE_ENV_VAR} unset: {source}",
                default_cluster_file_path().display()
            ),
            Self::NotFound { path, source, .. } => {
                write!(
                    f,
                    "Unable to read cluster file `{}': {source}",
                    path.display()
                )
            }
            Self::TooLarge { path } => write!(
                f,
                "Cluster file `{}' is larger than {MAX_CLUSTER_FILE_BYTES} bytes",
                path.display()
            ),
            Self::Io { path, source } => {
                write!(
                    f,
                    "Unexpected error loading cluster file `{}': {source}",
                    path.display()
                )
            }
            Self::Invalid { path, source } => {
                write!(f, "Invalid cluster file `{}': {source}", path.display())
            }
        }
    }
}

impl std::error::Error for ClusterFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotFound { source, .. } | Self::Io { source, .. } => Some(source),
            Self::Invalid { source, .. } => Some(source),
            Self::TooLarge { .. } => None,
        }
    }
}

fn read_cluster_file_at(
    path: &Path,
    is_default: bool,
) -> Result<ClusterConnectionString, ClusterFileError> {
    let file = File::open(path).map_err(|source| ClusterFileError::NotFound {
        path: path.to_path_buf(),
        is_default,
        source,
    })?;
    let mut bytes = Vec::new();
    file.take(MAX_CLUSTER_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| ClusterFileError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.len() > MAX_CLUSTER_FILE_BYTES {
        return Err(ClusterFileError::TooLarge {
            path: path.to_path_buf(),
        });
    }
    ClusterConnectionString::parse_bytes(&bytes).map_err(|source| ClusterFileError::Invalid {
        path: path.to_path_buf(),
        source,
    })
}

/// Reads and parses the cluster file at `path`
/// (`ClusterConnectionFile(filename)`).
pub fn read_cluster_file(path: &Path) -> Result<ClusterConnectionString, ClusterFileError> {
    read_cluster_file_at(path, false)
}

/// A loaded cluster file: where it came from and what it says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterFile {
    /// How the path was chosen.
    pub location: ClusterFileLocation,
    /// The parsed contents.
    pub connection_string: ClusterConnectionString,
}

impl ClusterFile {
    /// `ClusterConnectionFile::openOrDefault`: look the file up (explicit
    /// path, `FDB_CLUSTER_FILE`, `./fdb.cluster`, platform default) and load it.
    pub fn open_or_default(explicit: Option<&Path>) -> Result<Self, ClusterFileError> {
        Self::open_location(lookup_cluster_file(explicit))
    }

    /// Loads an already looked-up location.
    pub fn open_location(location: ClusterFileLocation) -> Result<Self, ClusterFileError> {
        let connection_string = read_cluster_file_at(&location.path, location.is_default())?;
        Ok(Self {
            location,
            connection_string,
        })
    }
}

#[cfg(test)]
mod tests;
