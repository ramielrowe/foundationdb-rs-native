//! TLS for FoundationDB connections: a rustls client session over any
//! `futures-io` stream, configured the way FoundationDB configures TLS.
//!
//! Enabled by the `tls` cargo feature (off by default).
//!
//! # Use
//!
//! ```no_run
//! # #[cfg(feature = "tokio")]
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use fdb_native_transport::runtime::{Runtime, TokioRuntime};
//! use fdb_native_transport::tls::{TlsConfig, TlsConnector};
//!
//! // Explicit options first, then FDB_TLS_* variables and /etc/foundationdb.
//! let loaded = TlsConfig::new().ca_path("/etc/fdb/ca.pem").load()?;
//! let connector = TlsConnector::new(&loaded)?;
//!
//! let runtime = TokioRuntime::new();
//! let tcp = runtime.connect("10.0.0.1:4500".parse()?).await?;
//! let _secure = connector.connect(tcp).await?; // AsyncRead + AsyncWrite
//! # Ok(())
//! # }
//! ```
//!
//! * [`TlsConfig`] holds explicit settings (FoundationDB's network options);
//!   [`TlsConfig::load`] resolves them against `FDB_TLS_*` variables and the
//!   default config directory and reads the files, giving a
//!   [`LoadedTlsConfig`].
//! * [`TlsConnector`] is built from that and wraps a connected stream in a
//!   [`TlsStream`], which implements `futures_io::{AsyncRead, AsyncWrite}`
//!   and so drops in wherever the transport uses a `Runtime::Stream`.
//! * Server certificates are checked by a pluggable
//!   [`ServerCertVerifier`](rustls::client::danger::ServerCertVerifier). The
//!   default, [`ChainVerifier`], validates the chain to the configured CA and
//!   nothing else. FoundationDB's `verify_peers` rules plug in through
//!   [`TlsConnector::with_verifier`].
//!
//! # Crypto provider
//!
//! rustls with the **`ring`** provider, always, via the `tls` feature. The
//! `tls-aws-lc-rs` feature additionally compiles `aws-lc-rs` and makes it the
//! provider instead ([`crypto_provider`]); `ring` stays compiled in because
//! cargo features are additive. `aws-lc-rs` needs a C toolchain (and CMake on
//! some targets), which is why `ring` is the default. Neither provider changes
//! behaviour described below.
//!
//! # Why `tls` is off by default
//!
//! Most FoundationDB clusters are plaintext, and the TLS stack pulls in
//! rustls, a crypto provider (a C/assembly build) and the PKCS#8 decryption
//! crates. The crate that exposes the client API turns the feature on for
//! users who ask for TLS; the transport stays light for the rest.
//!
//! # What FoundationDB does
//!
//! Everything in this module was checked against the `release-7.4` and
//! `release-8.0` branches of apple/foundationdb. Paths are relative to the
//! repository root; 8.0 dropped the Flow actor compiler, so `X.actor.cpp` on
//! 7.4 is `X.cpp` on 8.0 (`flow/TLSConfig.actor.cpp` is `flow/TLSConfig.cpp`,
//! `flow/Net2.actor.cpp` is `flow/Net2.cpp`, `fdbrpc/FlowTransport.actor.cpp`
//! is `fdbrpc/FlowTransport.cpp`). Line numbers are given as `7.4/8.0`. The
//! two branches agree on every point below, except where noted.
//!
//! ## How TLS is selected
//!
//! * Only by the `:tls` suffix on an address: `NetworkAddress::parse` strips a
//!   trailing `:tls` and sets the TLS flag (`flow/network.cpp` 161/173).
//!   Coordinators in the cluster file and server addresses are written
//!   `host:port:tls`. There is no other switch, and no negotiation: a TLS
//!   server only talks to TLS peers.
//! * `Net2::connect` (`flow/Net2.actor.cpp` 2210 / `flow/Net2.cpp` 1978) opens
//!   an `SSLConnection` when `toAddr.isTLS()`. Otherwise it connects in
//!   plaintext, unless `TLS_disable_plaintext_connection` /
//!   `FDB_TLS_DISABLE_PLAINTEXT_CONNECTION` is set, in which case it throws
//!   `connection_failed`.
//! * TLS starts immediately after the TCP connection is established:
//!   `FlowTransport`'s connection actor awaits `conn->connectHandshake()`
//!   right after `connect()` (`fdbrpc/FlowTransport.actor.cpp` 837 /
//!   `fdbrpc/FlowTransport.cpp` 949), before the `ConnectPacket` is sent.
//! * Over TLS the packet framing drops the XXH3 checksum:
//!   `checksumEnabled = !peerAddress.isTLS()` (`FlowTransport` 1335 / 1451 for
//!   reads, 2290 / 2447 for writes).
//!
//! ## Protocol versions, ciphers, ALPN, resumption
//!
//! * FoundationDB configures none of these. Its `boost::asio::ssl::context` is
//!   created with the generic `tls` method (`Net2` constructor,
//!   `flow/Net2.actor.cpp` 1516 / `flow/Net2.cpp` 1396) and
//!   `ConfigureSSLContext` only sets `default_workarounds`, the verify mode,
//!   the password callback, CA, key and certificate (`flow/TLSConfig.actor.cpp`
//!   122-156 / `flow/TLSConfig.cpp` 121-155). A search of `flow/`, `fdbrpc/`,
//!   `fdbclient/` and `fdbserver/` on both branches finds no
//!   `SSL_CTX_set_*`, min/max protocol, cipher list, ALPN or session-cache
//!   call. So the versions and ciphers are whatever the statically linked
//!   OpenSSL defaults to (`cmake/FDBComponents.cmake` 27-37 on 7.4); that
//!   depends on the OpenSSL release the binary was built with, and modern
//!   OpenSSL negotiates TLS 1.2 or 1.3.
//! * **ALPN: none.** Neither side offers or checks a protocol.
//! * **Session resumption: none.** Each `SSLConnection` creates a fresh
//!   `ssl_sock`; the code never saves or sets an `SSL_SESSION`, so a client
//!   always does a full handshake even though a TLS 1.3 server may issue
//!   tickets.
//! * This module speaks TLS 1.2 and 1.3 (rustls' only versions) with rustls'
//!   default ECDHE + AEAD suites, offers no ALPN, and disables resumption and
//!   early data. Servers that still require TLS 1.0/1.1, RSA key exchange or
//!   CBC suites are not supported.
//!
//! ## SNI and hostname verification
//!
//! * **No SNI, no hostname check for cluster peers.** Peer connections go
//!   through `INetworkConnections::connect(NetworkAddress)`
//!   (`FlowTransport` 832 / 942), which creates the `SSLConnection` with an
//!   empty `sni_hostname`. `doConnectHandshake` calls `SSL_set_tlsext_host_name`
//!   and `SSL_set1_host` only when `sni_hostname` is non-empty
//!   (`Net2.actor.cpp` 1144-1150 / `Net2.cpp` 1104-1120).
//! * `sni_hostname` is set only by `connectExternalWithHostname`, reached from
//!   `INetworkConnections::connect(host, service, isTLS)`
//!   (`flow/network.cpp` 407/419), which is used for external servers (the
//!   HTTP client for blob stores and KMS), never for FDB peers. On 8.0 that
//!   path also bypasses the verify-peers policy and checks the hostname.
//! * This module sets `enable_sni = false` and passes rustls a placeholder
//!   server name that the verifier ignores; [`ChainVerifier`] treats
//!   `NotValidForName` as success.
//!
//! ## What a client presents and how files are handled
//!
//! * The context loads the CA with `add_certificate_authority` (all
//!   certificates in the PEM are trust anchors), the key with `use_private_key`
//!   (PEM) and the certificate with `use_certificate_chain`
//!   (`TLSConfig.actor.cpp` 134-147 / `TLSConfig.cpp` 133-146). The first
//!   certificate is the client's own; the rest are sent as its chain.
//! * Only the CA bytes are trust anchors. The documentation
//!   (`documentation/sphinx/source/tls.rst`, "The certificate file") says the
//!   last certificate in the certificate file is trusted, but the code never
//!   adds it to the store. With no CA, OpenSSL's store is empty and every peer
//!   is rejected; there is no default trust store (`set_default_verify_paths`
//!   is never called). [`TlsConnector::new`] reports this up front as
//!   [`TlsError::NoTrustAnchors`].
//! * A client may have no certificate at all. FoundationDB then still connects
//!   (its servers accept "untrusted" clients, below).
//! * Keys: the docs require PKCS#8 (`tls.rst`, "The key file"), possibly
//!   encrypted with `tls_password`; OpenSSL itself also reads PKCS#1 and SEC1
//!   PEM. This module reads all three, and encrypted PKCS#8 (see below).
//! * **Passwords:** `ConfigureSSLContext` installs a password callback that
//!   returns `LoadedTLSConfig::getPassword()` (explicit option, else
//!   `FDB_TLS_PASSWORD`, else empty), line 131 / 130. OpenSSL calls it only for an
//!   encrypted key. Pure-Rust support exists (the `pkcs8` crate), so
//!   encrypted keys work here when they are PKCS#8 with PBES2 (PBKDF2 and
//!   AES-CBC; what `openssl pkcs8 -topk8` and `openssl genpkey -aes256`
//!   write). PBES1 / PKCS#12-style schemes and OpenSSL's legacy
//!   `Proc-Type: 4,ENCRYPTED` PEM encryption are not supported and give
//!   [`TlsError::KeyDecrypt`] / [`TlsError::LegacyEncryptedKey`].
//! * Files are read whole and limited by the `CERT_FILE_MAX_SIZE` knob, 5 MiB
//!   (`flow/Knobs.cpp` 228 / 226); a larger file is an error
//!   ([`TlsError::FileTooLarge`]).
//! * FoundationDB reloads certificates every 12 hours when the files change
//!   (`TLS_CERT_REFRESH_DELAY_SECONDS`, `reloadCertificatesOnChange`). This
//!   module loads once; a new [`TlsConnector`] picks up new files.
//!
//! ## Option names, environment variables, defaults
//!
//! Client network options (`fdbclient/vexillographer/fdb.options`, identical on
//! both branches; handled in `fdbclient/NativeAPI.actor.cpp` 2795-2833 /
//! `NativeAPI.cpp` 745-784):
//!
//! | Option (code)                          | Value | Environment variable (`FDB_TLS_…`) | Here |
//! |----------------------------------------|-------|------------------------------------|------|
//! | `TLS_cert_path` (43)                   | path  | `CERTIFICATE_FILE`                 | [`TlsConfig::cert_path`] |
//! | `TLS_cert_bytes` (42)                  | PEM   |                                    | [`TlsConfig::cert_bytes`] |
//! | `TLS_key_path` (46)                    | path  | `KEY_FILE`                         | [`TlsConfig::key_path`] |
//! | `TLS_key_bytes` (45)                   | PEM   |                                    | [`TlsConfig::key_bytes`] |
//! | `TLS_ca_path` (53)                     | path  | `CA_FILE`                          | [`TlsConfig::ca_path`] |
//! | `TLS_ca_bytes` (52)                    | PEM   |                                    | [`TlsConfig::ca_bytes`] |
//! | `TLS_password` (54)                    | text  | `PASSWORD`                         | [`TlsConfig::password`] |
//! | `TLS_verify_peers` (47)                | rules | `VERIFY_PEERS`                     | [`TlsConfig::verify_peers`] |
//! | `TLS_disable_plaintext_connection` (55)| flag  | `DISABLE_PLAINTEXT_CONNECTION`     | [`TlsConfig::disable_plaintext_connection`] |
//! | `TLS_plugin` (41)                      |       |                                    | deprecated, ignored |
//!
//! * Order of precedence (`tls.rst`, "Configuring TLS"; code
//!   `TLSConfig::getCertificatePathSync` and friends, 184-228 / 202-218):
//!   explicit option, then environment variable, then default. The setters
//!   `setCertificatePath` / `setCertificateBytes` (and key, CA) clear each
//!   other (`TLSConfig.actor.h`). One quirk follows from `loadSync`: it
//!   resolves a *path* first (explicit path, env var, default file) and uses
//!   configured *bytes* only if no path results, so an env var or a
//!   `cert.pem` in the config directory beats `TLS_cert_bytes`. This module
//!   reproduces that; see [`TlsConfig::load_with`].
//! * The default files are `cert.pem` and `key.pem` in the default config
//!   directory, used if they exist: `/etc/foundationdb` on Linux,
//!   `/usr/local/etc/foundationdb` on macOS, `%ProgramData%\foundationdb` on
//!   Windows (`tls.rst`; `platform::getDefaultConfigPath`, `flow/Platform.cpp`;
//!   [`default_config_dir`]). The CA has no default file, and neither does
//!   the password (none: the key is assumed unencrypted).
//! * `TLS_verify_peers` clears previous rules and sets one string;
//!   the command line may repeat `--tls-verify-peers` to add more.
//!   `FDB_TLS_VERIFY_PEERS` is used when none is set. A client option and the
//!   `FDB_TLS_DISABLE_PLAINTEXT_CONNECTION` variable (only `0` and `1` parse)
//!   both exist on 7.4 and 8.0, and the variable wins over the option.
//!   8.0 adds `TLSConfig::isInsecure()` (no certificate configured); nothing
//!   in the client reads it.
//!
//! ## Peer verification (what `verify_peers` defaults to)
//!
//! * **Default and unset behaviour:** with no rule from options or
//!   `FDB_TLS_VERIFY_PEERS`, `getVerifyPeers()` returns `{"Check.Valid=1"}`
//!   (`TLSConfig.actor.cpp` 63-74 / `TLSConfig.cpp` 62-73; `tls.rst`,
//!   "Default Peer Verification"). That is one rule with no field criteria, so
//!   it accepts any peer whose chain OpenSSL validated against the CA. This
//!   is exactly what [`ChainVerifier`] does. The configured string is
//!   available as [`LoadedTlsConfig::verify_peers`] for a rule-aware verifier.
//! * **Both ends check, but servers do not demand a client certificate.**
//!   `ConfigureSSLContext` sets `verify_peer`, plus `verify_fail_if_no_peer_cert`
//!   unless the endpoint type is `SERVER` (lines 124-129 / 123-128): *a client
//!   requires the server to present a certificate; a server asks for one but
//!   accepts connections without*. A client that presents a certificate has it
//!   validated against the server's CA and `verify_peers`; one that presents
//!   none connects as an *untrusted* peer. `has_trusted_peer` is set only by
//!   the verify callback (`Net2.cpp` 982 and 1085 on 8.0), and `FlowTransport`
//!   delivers packets from an untrusted peer only to public endpoints
//!   (`receiver->isPublic()`, `FlowTransport.cpp` 1331 on 8.0). A client
//!   without a valid certificate can therefore connect to a TLS cluster but is
//!   limited to its public endpoints.
//! * **Per-certificate callback.** Peer rules run inside OpenSSL's verify
//!   callback, which fires once per certificate in the chain
//!   (`ConfigureSSLStream`, `TLSConfig.actor.cpp` 158 / `TLSConfig.cpp` 157;
//!   its doc comment in `TLSConfig.actor.h`). `TLSPolicy::verify_peer` first
//!   returns false if `preverified` is false (1003-1012 / 981-990), then
//!   evaluates the rules against the leaf, its issuer and the chain's last
//!   (root) certificate.
//! * Consequences for a verify-peers implementation: `Check.Valid=0` only
//!   skips rule matching; a chain OpenSSL rejected (`preverified == false`) is
//!   rejected regardless. `Check.Unexpired=0` is parsed into `verify_time`,
//!   but nothing reads that field, so expired certificates are rejected by
//!   OpenSSL in every case.
//!
//! # Differences from OpenSSL
//!
//! Chain validation uses `rustls-webpki`, which is stricter than OpenSSL in
//! ways that can reject a certificate FoundationDB accepts:
//!
//! * no SHA-1 or MD5 signatures, no RSA keys under 2048 bits (the FDB docs'
//!   example uses `rsa:2048`; older sample commands used SHA-1);
//! * X.509 v3 only; an end-entity certificate must not be a CA
//!   (`basicConstraints CA:TRUE`), and a present `extendedKeyUsage` must
//!   include `serverAuth`;
//! * a self-signed server certificate is accepted only if it is itself in the
//!   CA bundle (it then acts as its own trust anchor).
//!
//! Where FoundationDB tolerates a client certificate configured without a key
//! (it silently sends none), this module reports an error instead
//! ([`TlsError::CertificateWithoutKey`]).

mod config;
mod error;
mod pem;
mod stream;
mod verifier;

pub use config::{
    DEFAULT_CERTIFICATE_FILE_NAME, DEFAULT_KEY_FILE_NAME, DEFAULT_VERIFY_PEERS, ENV_CA_FILE,
    ENV_CERTIFICATE_FILE, ENV_DISABLE_PLAINTEXT_CONNECTION, ENV_KEY_FILE, ENV_PASSWORD,
    ENV_VERIFY_PEERS, LoadedTlsConfig, MAX_FILE_SIZE, TlsConfig, default_config_dir,
};
pub use error::TlsError;
pub use stream::{TlsConnector, TlsStream, rustls_error};
pub use verifier::{ChainVerifier, crypto_provider};

/// The rustls this module is built on, re-exported so a custom
/// [`ServerCertVerifier`](rustls::client::danger::ServerCertVerifier) uses
/// the same version.
pub use rustls;
