//! Loopback tests of the TLS client layer against a rustls server.
//!
//! The server side is `tokio-rustls` on a plain tokio socket; the client side
//! is this crate's `TlsConnector` over `TokioRuntime`'s `futures-io` stream.
//! Certificates are generated per test with rcgen.

#![cfg(all(feature = "tls", feature = "tokio"))]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fdb_native_transport::runtime::{Runtime, TokioRuntime, TokioStream};
use fdb_native_transport::tls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use fdb_native_transport::tls::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime,
};
use fdb_native_transport::tls::rustls::server::WebPkiClientVerifier;
use fdb_native_transport::tls::rustls::{
    self, CertificateError, DigitallySignedStruct, HandshakeKind, ProtocolVersion, RootCertStore,
    ServerConfig, SignatureScheme, SupportedProtocolVersion,
};
use fdb_native_transport::tls::{
    ChainVerifier, LoadedTlsConfig, TlsConfig, TlsConnector, TlsError, TlsStream, crypto_provider,
    rustls_error,
};
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, date_time_ymd,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;

const EC_KEY_PLAIN: &str = include_str!("fixtures/ec_key_plain.pem");
const EC_KEY_ENCRYPTED: &str = include_str!("fixtures/ec_key_aes256_hunter2.pem");

// ---------------------------------------------------------------- PKI

type Ca = CertifiedIssuer<'static, KeyPair>;

fn ca_params(common_name: &str) -> CertificateParams {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params
}

fn new_ca(common_name: &str) -> Ca {
    CertifiedIssuer::self_signed(ca_params(common_name), KeyPair::generate().unwrap()).unwrap()
}

fn new_intermediate(common_name: &str, parent: &Ca) -> Ca {
    CertifiedIssuer::signed_by(ca_params(common_name), KeyPair::generate().unwrap(), parent)
        .unwrap()
}

/// A leaf certificate and its key.
struct Leaf {
    cert: CertificateDer<'static>,
    cert_pem: String,
    key: KeyPair,
}

impl Leaf {
    fn key_pem(&self) -> String {
        self.key.serialize_pem()
    }

    fn key_der(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key.serialize_der()))
    }
}

fn leaf_with(
    issuer: &Ca,
    key: KeyPair,
    sans: &[&str],
    purpose: ExtendedKeyUsagePurpose,
    tweak: impl FnOnce(&mut CertificateParams),
) -> Leaf {
    let mut params =
        CertificateParams::new(sans.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>()).unwrap();
    params.extended_key_usages = vec![purpose];
    params.distinguished_name.push(
        DnType::CommonName,
        sans.first().copied().unwrap_or("client"),
    );
    tweak(&mut params);
    let cert = params.signed_by(&key, issuer).unwrap();
    Leaf {
        cert: cert.der().clone(),
        cert_pem: cert.pem(),
        key,
    }
}

fn server_leaf(issuer: &Ca, sans: &[&str]) -> Leaf {
    leaf_with(
        issuer,
        KeyPair::generate().unwrap(),
        sans,
        ExtendedKeyUsagePurpose::ServerAuth,
        |_| {},
    )
}

fn client_leaf(issuer: &Ca, name: &str) -> Leaf {
    leaf_with(
        issuer,
        KeyPair::generate().unwrap(),
        &[name],
        ExtendedKeyUsagePurpose::ClientAuth,
        |_| {},
    )
}

// ------------------------------------------------------------- server

/// What the server observed about one connection.
#[derive(Debug)]
struct Seen {
    error: Option<String>,
    sni: Option<String>,
    alpn: Option<Vec<u8>>,
    peer_certs: Option<Vec<CertificateDer<'static>>>,
    version: Option<ProtocolVersion>,
    kind: Option<HandshakeKind>,
}

struct ServerOptions {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    /// CA that client certificates must chain to; `None` asks for no client
    /// certificate.
    client_ca: Option<CertificateDer<'static>>,
    /// Accept clients that send no certificate (when `client_ca` is set).
    allow_anonymous: bool,
    versions: &'static [&'static SupportedProtocolVersion],
    alpn: Vec<Vec<u8>>,
}

impl ServerOptions {
    fn new(leaf: &Leaf, extra_chain: &[CertificateDer<'static>]) -> Self {
        let mut chain = vec![leaf.cert.clone()];
        chain.extend_from_slice(extra_chain);
        Self {
            chain,
            key: leaf.key_der(),
            client_ca: None,
            allow_anonymous: false,
            versions: rustls::DEFAULT_VERSIONS,
            alpn: Vec::new(),
        }
    }

    fn require_client_cert(mut self, ca: &Ca) -> Self {
        self.client_ca = Some(ca.der().clone());
        self
    }
}

struct Server {
    addr: SocketAddr,
    seen: mpsc::UnboundedReceiver<Seen>,
}

impl Server {
    /// Listens on loopback and serves any number of connections: each is
    /// TLS-accepted, described on `seen`, then echoed until EOF.
    async fn start(options: ServerOptions) -> Self {
        let provider = crypto_provider();
        let builder = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(options.versions)
            .unwrap();
        let builder = match &options.client_ca {
            None => builder.with_no_client_auth(),
            Some(ca) => {
                let mut roots = RootCertStore::empty();
                roots.add(ca.clone()).unwrap();
                let verifier =
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
                let verifier = if options.allow_anonymous {
                    verifier.allow_unauthenticated()
                } else {
                    verifier
                };
                builder.with_client_cert_verifier(verifier.build().unwrap())
            }
        };
        let mut config = builder
            .with_single_cert(options.chain, options.key)
            .unwrap();
        config.alpn_protocols = options.alpn;
        let acceptor = TlsAcceptor::from(Arc::new(config));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, seen) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let acceptor = acceptor.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut tls = match acceptor.accept(tcp).await {
                        Ok(tls) => tls,
                        Err(error) => {
                            let _ = tx.send(Seen {
                                error: Some(error.to_string()),
                                sni: None,
                                alpn: None,
                                peer_certs: None,
                                version: None,
                                kind: None,
                            });
                            return;
                        }
                    };
                    let conn = tls.get_ref().1;
                    let _ = tx.send(Seen {
                        error: None,
                        sni: conn.server_name().map(str::to_owned),
                        alpn: conn.alpn_protocol().map(<[u8]>::to_vec),
                        peer_certs: conn.peer_certificates().map(<[_]>::to_vec),
                        version: conn.protocol_version(),
                        kind: conn.handshake_kind(),
                    });
                    let mut buf = vec![0u8; 16 * 1024];
                    loop {
                        match tls.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if tls.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    let _ = tls.shutdown().await;
                });
            }
        });
        Self { addr, seen }
    }

    async fn next_seen(&mut self) -> Seen {
        tokio::time::timeout(std::time::Duration::from_secs(10), self.seen.recv())
            .await
            .expect("server saw no connection")
            .expect("server task ended")
    }
}

// ------------------------------------------------------------- client

fn loaded(config: TlsConfig) -> LoadedTlsConfig {
    config.load_with(|_| None, None).unwrap()
}

fn ca_config(ca: &Ca) -> TlsConfig {
    TlsConfig::new().ca_bytes(ca.pem())
}

async fn connect(
    connector: &TlsConnector,
    addr: SocketAddr,
) -> std::io::Result<TlsStream<TokioStream>> {
    let tcp = TokioRuntime::new().connect(addr).await?;
    connector.connect(tcp).await
}

async fn echo(stream: &mut TlsStream<TokioStream>, payload: &[u8]) {
    stream.write_all(payload).await.unwrap();
    stream.flush().await.unwrap();
    let mut back = vec![0u8; payload.len()];
    stream.read_exact(&mut back).await.unwrap();
    assert_eq!(back, payload);
}

// -------------------------------------------------------------- tests

#[tokio::test]
async fn handshake_succeeds_when_the_name_does_not_match_and_sends_no_sni() {
    let ca = new_ca("Test CA");
    // The server certificate is for another host entirely, and the client
    // connects by IP address.
    let leaf = server_leaf(&ca, &["not-this-host.example"]);
    let mut options = ServerOptions::new(&leaf, &[]);
    options.alpn = vec![b"h2".to_vec(), b"fdb".to_vec()];
    let mut server = Server::start(options).await;

    let connector = TlsConnector::new(&loaded(ca_config(&ca))).unwrap();
    assert!(!connector.has_client_certificate());
    let mut stream = connect(&connector, server.addr).await.unwrap();
    echo(&mut stream, b"hello fdb").await;

    let seen = server.next_seen().await;
    assert_eq!(seen.error, None);
    assert_eq!(seen.sni, None, "the client must not send SNI");
    assert_eq!(seen.alpn, None, "the client must not offer ALPN");
    assert!(
        seen.peer_certs.is_none(),
        "no client certificate configured"
    );
    assert_eq!(stream.alpn_protocol(), None);
    assert_eq!(stream.protocol_version(), Some(ProtocolVersion::TLSv1_3));
    let presented = stream.peer_certificates().unwrap();
    assert_eq!(presented[0], leaf.cert);
}

#[tokio::test]
async fn a_server_chain_through_an_intermediate_validates() {
    let root = new_ca("Root");
    let intermediate = new_intermediate("Intermediate", &root);
    let leaf = server_leaf(&intermediate, &["server.example"]);
    let server = Server::start(ServerOptions::new(&leaf, &[intermediate.der().clone()])).await;

    // Only the root is trusted; the intermediate arrives in the handshake.
    let connector = TlsConnector::new(&loaded(ca_config(&root))).unwrap();
    let stream = connect(&connector, server.addr).await.unwrap();
    assert_eq!(stream.peer_certificates().unwrap().len(), 2);
}

#[tokio::test]
async fn a_server_signed_by_an_untrusted_ca_is_rejected() {
    let trusted = new_ca("Trusted CA");
    let rogue = new_ca("Rogue CA");
    let leaf = server_leaf(&rogue, &["server.example"]);
    let server = Server::start(ServerOptions::new(&leaf, &[])).await;

    let connector = TlsConnector::new(&loaded(ca_config(&trusted))).unwrap();
    let error = connect(&connector, server.addr).await.unwrap_err();
    assert!(
        matches!(
            rustls_error(&error),
            Some(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer
            ))
        ),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn an_expired_server_certificate_is_rejected_even_without_a_name_check() {
    let ca = new_ca("Test CA");
    let leaf = leaf_with(
        &ca,
        KeyPair::generate().unwrap(),
        &["server.example"],
        ExtendedKeyUsagePurpose::ServerAuth,
        |params| {
            params.not_before = date_time_ymd(2000, 1, 1);
            params.not_after = date_time_ymd(2001, 1, 1);
        },
    );
    let server = Server::start(ServerOptions::new(&leaf, &[])).await;

    let connector = TlsConnector::new(&loaded(ca_config(&ca))).unwrap();
    let error = connect(&connector, server.addr).await.unwrap_err();
    assert!(
        matches!(
            rustls_error(&error),
            Some(rustls::Error::InvalidCertificate(
                CertificateError::Expired | CertificateError::ExpiredContext { .. }
            ))
        ),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn the_client_certificate_is_presented_and_validated_by_the_server() {
    let ca = new_ca("Test CA");
    let server_cert = server_leaf(&ca, &["server.example"]);
    let client = client_leaf(&ca, "fdb-client");
    let mut server =
        Server::start(ServerOptions::new(&server_cert, &[]).require_client_cert(&ca)).await;

    let config = ca_config(&ca)
        .cert_bytes(client.cert_pem.clone())
        .key_bytes(client.key_pem());
    let connector = TlsConnector::new(&loaded(config)).unwrap();
    assert!(connector.has_client_certificate());
    let mut stream = connect(&connector, server.addr).await.unwrap();
    echo(&mut stream, b"authenticated").await;

    let seen = server.next_seen().await;
    assert_eq!(seen.error, None);
    let certs = seen
        .peer_certs
        .expect("server received a client certificate");
    assert_eq!(certs[0], client.cert);
    assert_eq!(seen.sni, None);
}

#[tokio::test]
async fn the_client_certificate_works_over_tls12_too() {
    let ca = new_ca("Test CA");
    let server_cert = server_leaf(&ca, &["server.example"]);
    let client = client_leaf(&ca, "fdb-client");
    let mut options = ServerOptions::new(&server_cert, &[]).require_client_cert(&ca);
    static TLS12_ONLY: [&SupportedProtocolVersion; 1] = [&rustls::version::TLS12];
    options.versions = &TLS12_ONLY;
    let mut server = Server::start(options).await;

    let config = ca_config(&ca)
        .cert_bytes(client.cert_pem.clone())
        .key_bytes(client.key_pem());
    let connector = TlsConnector::new(&loaded(config)).unwrap();
    let mut stream = connect(&connector, server.addr).await.unwrap();
    assert_eq!(stream.protocol_version(), Some(ProtocolVersion::TLSv1_2));
    echo(&mut stream, b"tls1.2").await;

    let seen = server.next_seen().await;
    assert_eq!(seen.version, Some(ProtocolVersion::TLSv1_2));
    assert_eq!(seen.peer_certs.unwrap()[0], client.cert);
}

#[tokio::test]
async fn a_server_requiring_a_client_certificate_rejects_a_connector_without_one() {
    let ca = new_ca("Test CA");
    let server_cert = server_leaf(&ca, &["server.example"]);
    let mut server =
        Server::start(ServerOptions::new(&server_cert, &[]).require_client_cert(&ca)).await;

    let connector = TlsConnector::new(&loaded(ca_config(&ca))).unwrap();
    // With TLS 1.3 the client finishes its handshake before the server
    // objects, so the failure may land on connect or on the first I/O.
    let outcome = match connect(&connector, server.addr).await {
        Err(error) => Err(error),
        Ok(mut stream) => {
            let mut byte = [0u8; 1];
            match stream.write_all(b"x").await {
                Err(error) => Err(error),
                Ok(()) => stream.read(&mut byte).await.map(|_| ()),
            }
        }
    };
    assert!(
        outcome.is_err(),
        "server accepted a client with no certificate"
    );
    assert!(server.next_seen().await.error.is_some());
}

#[tokio::test]
async fn a_server_that_allows_anonymous_clients_accepts_a_connector_without_a_certificate() {
    // FoundationDB servers ask for a client certificate but do not insist.
    let ca = new_ca("Test CA");
    let server_cert = server_leaf(&ca, &["server.example"]);
    let mut options = ServerOptions::new(&server_cert, &[]).require_client_cert(&ca);
    options.allow_anonymous = true;
    let mut server = Server::start(options).await;

    let connector = TlsConnector::new(&loaded(ca_config(&ca))).unwrap();
    let mut stream = connect(&connector, server.addr).await.unwrap();
    echo(&mut stream, b"anonymous").await;
    let seen = server.next_seen().await;
    assert_eq!(seen.error, None);
    assert!(seen.peer_certs.is_none());
}

#[tokio::test]
async fn an_encrypted_client_key_is_decrypted_with_the_password() {
    let ca = new_ca("Test CA");
    let server_cert = server_leaf(&ca, &["server.example"]);
    // The fixture key, issued a certificate by the test CA.
    let client = leaf_with(
        &ca,
        KeyPair::from_pem(EC_KEY_PLAIN).unwrap(),
        &["fdb-client"],
        ExtendedKeyUsagePurpose::ClientAuth,
        |_| {},
    );
    let mut server =
        Server::start(ServerOptions::new(&server_cert, &[]).require_client_cert(&ca)).await;

    let config = ca_config(&ca)
        .cert_bytes(client.cert_pem.clone())
        .key_bytes(EC_KEY_ENCRYPTED);

    // Without the password the connector cannot be built.
    let error = TlsConnector::new(&loaded(config.clone())).unwrap_err();
    assert!(matches!(error, TlsError::KeyPasswordRequired), "{error:?}");
    let error = TlsConnector::new(&loaded(config.clone().password("wrong"))).unwrap_err();
    assert!(matches!(error, TlsError::KeyDecrypt(_)), "{error:?}");

    let connector = TlsConnector::new(&loaded(config.password("hunter2"))).unwrap();
    let mut stream = connect(&connector, server.addr).await.unwrap();
    echo(&mut stream, b"encrypted key").await;
    assert_eq!(server.next_seen().await.peer_certs.unwrap()[0], client.cert);
}

#[tokio::test]
async fn a_key_that_does_not_match_the_certificate_is_a_configuration_error() {
    let ca = new_ca("Test CA");
    let client = client_leaf(&ca, "fdb-client");
    let other = client_leaf(&ca, "someone-else");
    let config = ca_config(&ca)
        .cert_bytes(client.cert_pem.clone())
        .key_bytes(other.key_pem());
    let error = TlsConnector::new(&loaded(config)).unwrap_err();
    assert!(matches!(error, TlsError::Rustls(_)), "{error:?}");
}

#[tokio::test]
async fn a_certificate_without_a_key_is_a_configuration_error() {
    let ca = new_ca("Test CA");
    let client = client_leaf(&ca, "fdb-client");
    let config = ca_config(&ca).cert_bytes(client.cert_pem.clone());
    let error = TlsConnector::new(&loaded(config)).unwrap_err();
    assert!(
        matches!(error, TlsError::CertificateWithoutKey),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_connector_needs_a_ca_unless_a_verifier_is_supplied() {
    let error = TlsConnector::new(&loaded(TlsConfig::new())).unwrap_err();
    assert!(matches!(error, TlsError::NoTrustAnchors), "{error:?}");
}

#[tokio::test]
async fn key_and_certificate_can_be_read_from_files() {
    let ca = new_ca("Test CA");
    let server_cert = server_leaf(&ca, &["server.example"]);
    let client = client_leaf(&ca, "fdb-client");
    let mut server =
        Server::start(ServerOptions::new(&server_cert, &[]).require_client_cert(&ca)).await;

    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("tls-loopback-files");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("ca.pem"), ca.pem()).unwrap();
    // Certificate and key in one combined file, as the FDB docs suggest.
    std::fs::write(
        dir.join("fdb.pem"),
        format!("{}{}", client.cert_pem, client.key_pem()),
    )
    .unwrap();

    let config = TlsConfig::new()
        .ca_path(dir.join("ca.pem"))
        .cert_path(dir.join("fdb.pem"))
        .key_path(dir.join("fdb.pem"));
    let connector = TlsConnector::new(&config.load_with(|_| None, None).unwrap()).unwrap();
    let mut stream = connect(&connector, server.addr).await.unwrap();
    echo(&mut stream, b"from files").await;
    assert_eq!(server.next_seen().await.peer_certs.unwrap()[0], client.cert);
}

/// A verifier that counts calls, records what it was shown, and defers to
/// `ChainVerifier` unless told to reject.
struct SpyVerifier {
    inner: Option<ChainVerifier>,
    reject: bool,
    calls: AtomicUsize,
    seen: std::sync::Mutex<Option<(CertificateDer<'static>, usize)>>,
}

impl std::fmt::Debug for SpyVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SpyVerifier")
    }
}

impl SpyVerifier {
    fn new(inner: Option<ChainVerifier>, reject: bool) -> Arc<Self> {
        Arc::new(Self {
            inner,
            reject,
            calls: AtomicUsize::new(0),
            seen: std::sync::Mutex::new(None),
        })
    }
}

impl ServerCertVerifier for SpyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.seen.lock().unwrap() = Some((end_entity.clone().into_owned(), intermediates.len()));
        if self.reject {
            return Err(rustls::Error::General("rejected by spy".into()));
        }
        match &self.inner {
            Some(chain) => {
                chain.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
            }
            None => Ok(ServerCertVerified::assertion()),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &crypto_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &crypto_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        crypto_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[tokio::test]
async fn the_custom_verifier_is_invoked_with_the_presented_chain() {
    let root = new_ca("Root");
    let intermediate = new_intermediate("Intermediate", &root);
    let leaf = server_leaf(&intermediate, &["server.example"]);
    let server = Server::start(ServerOptions::new(&leaf, &[intermediate.der().clone()])).await;

    let config = loaded(ca_config(&root));
    let spy = SpyVerifier::new(Some(ChainVerifier::from_config(&config).unwrap()), false);
    let connector = TlsConnector::with_verifier(&config, spy.clone()).unwrap();
    let _stream = connect(&connector, server.addr).await.unwrap();

    assert_eq!(spy.calls.load(Ordering::SeqCst), 1);
    let (end_entity, intermediates) = spy.seen.lock().unwrap().clone().unwrap();
    assert_eq!(end_entity, leaf.cert);
    assert_eq!(intermediates, 1);
}

#[tokio::test]
async fn a_custom_verifier_can_reject_a_chain_the_ca_accepts() {
    let ca = new_ca("Test CA");
    let leaf = server_leaf(&ca, &["server.example"]);
    let server = Server::start(ServerOptions::new(&leaf, &[])).await;

    let config = loaded(ca_config(&ca));
    let spy = SpyVerifier::new(Some(ChainVerifier::from_config(&config).unwrap()), true);
    let connector = TlsConnector::with_verifier(&config, spy.clone()).unwrap();
    let error = connect(&connector, server.addr).await.unwrap_err();
    assert!(
        matches!(rustls_error(&error), Some(rustls::Error::General(m)) if m == "rejected by spy"),
        "unexpected error: {error:?}"
    );
    assert_eq!(spy.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_custom_verifier_replaces_chain_validation_and_needs_no_ca() {
    let rogue = new_ca("Rogue CA");
    let leaf = server_leaf(&rogue, &["server.example"]);
    let server = Server::start(ServerOptions::new(&leaf, &[])).await;

    let spy = SpyVerifier::new(None, false);
    let connector = TlsConnector::with_verifier(&loaded(TlsConfig::new()), spy.clone()).unwrap();
    let _stream = connect(&connector, server.addr).await.unwrap();
    assert_eq!(spy.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn data_round_trips_and_close_is_graceful() {
    let ca = new_ca("Test CA");
    let leaf = server_leaf(&ca, &["server.example"]);
    let server = Server::start(ServerOptions::new(&leaf, &[])).await;

    let connector = TlsConnector::new(&loaded(ca_config(&ca))).unwrap();
    let mut stream = connect(&connector, server.addr).await.unwrap();

    // Small, then larger than one TLS record (16 KiB) and one socket buffer.
    echo(&mut stream, b"ping").await;
    let big: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    let (mut reader, mut writer) = futures_lite::io::split(stream);
    let expected = big.clone();
    let read = async move {
        let mut back = vec![0u8; expected.len()];
        reader.read_exact(&mut back).await.unwrap();
        assert_eq!(back, expected);
        reader
    };
    let write = async move {
        writer.write_all(&big).await.unwrap();
        writer.flush().await.unwrap();
        writer
    };
    let (mut reader, mut writer) = tokio::join!(read, write);

    // close() sends close_notify; the echo server then closes its side, which
    // the client reads as a clean EOF.
    writer.close().await.unwrap();
    let mut rest = Vec::new();
    assert_eq!(reader.read_to_end(&mut rest).await.unwrap(), 0);
}

#[tokio::test]
async fn the_server_closing_reads_as_eof() {
    let ca = new_ca("Test CA");
    let leaf = server_leaf(&ca, &["server.example"]);
    let server = Server::start(ServerOptions::new(&leaf, &[])).await;

    let connector = TlsConnector::new(&loaded(ca_config(&ca))).unwrap();
    let mut stream = connect(&connector, server.addr).await.unwrap();
    // Half-close the write side: the echo server sees EOF and shuts down.
    stream.close().await.unwrap();
    let mut buf = [0u8; 8];
    // The server answers with its own close_notify: a clean EOF, not an
    // UnexpectedEof error.
    assert_eq!(stream.read(&mut buf).await.unwrap(), 0);
}

#[tokio::test]
async fn sessions_are_never_resumed() {
    let ca = new_ca("Test CA");
    let leaf = server_leaf(&ca, &["server.example"]);
    let mut server = Server::start(ServerOptions::new(&leaf, &[])).await;

    let connector = TlsConnector::new(&loaded(ca_config(&ca))).unwrap();
    for _ in 0..3 {
        let mut stream = connect(&connector, server.addr).await.unwrap();
        // Read once so any NewSessionTicket messages are processed.
        echo(&mut stream, b"x").await;
        let seen = server.next_seen().await;
        assert_eq!(seen.kind, Some(HandshakeKind::Full));
    }
}

#[tokio::test]
async fn connecting_over_a_stream_that_is_not_tls_fails_cleanly() {
    // A plaintext server: the handshake must error, not hang or panic.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        let _ = tcp.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
        let mut sink = [0u8; 1024];
        let _ = tcp.read(&mut sink).await;
    });
    let ca = new_ca("Test CA");
    let connector = TlsConnector::new(&loaded(ca_config(&ca))).unwrap();
    assert!(connect(&connector, addr).await.is_err());
}

#[tokio::test]
async fn a_self_signed_server_certificate_is_accepted_only_when_it_is_the_trust_anchor() {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["self-signed.example".to_owned()]).unwrap();
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let cert = params.self_signed(&key).unwrap();
    let server = Server::start(ServerOptions {
        chain: vec![cert.der().clone()],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        client_ca: None,
        allow_anonymous: false,
        versions: rustls::DEFAULT_VERSIONS,
        alpn: Vec::new(),
    })
    .await;

    let trusting = TlsConnector::new(&loaded(TlsConfig::new().ca_bytes(cert.pem()))).unwrap();
    connect(&trusting, server.addr).await.unwrap();

    let other_ca = new_ca("Other CA");
    let not_trusting = TlsConnector::new(&loaded(ca_config(&other_ca))).unwrap();
    assert!(connect(&not_trusting, server.addr).await.is_err());
}
