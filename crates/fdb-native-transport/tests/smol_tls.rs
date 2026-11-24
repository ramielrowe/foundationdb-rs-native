//! The TLS client layer (`futures-rustls` through `TlsConnector`) over the smol adapter's
//! stream (`SmolStream`): a handshake with a server certificate and with a client
//! certificate, a megabyte echoed through the session (writes that hit `Pending`, reads
//! across record boundaries), a graceful close, and an untrusted server.
//!
//! The server is `tokio-rustls` on a tokio runtime of its own thread, which shares nothing
//! with the client but the loopback socket; the client side runs on smol.

#![cfg(all(feature = "tls", feature = "smol"))]

use std::net::SocketAddr;
use std::sync::Arc;

use fdb_native_transport::runtime::{Runtime, SmolRuntime, SmolStream};
use fdb_native_transport::tls::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer,
};
use fdb_native_transport::tls::rustls::server::WebPkiClientVerifier;
use fdb_native_transport::tls::rustls::{RootCertStore, ServerConfig};
use fdb_native_transport::tls::{TlsConfig, TlsConnector, TlsStream, crypto_provider};
use futures_lite::future::block_on;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

type Ca = CertifiedIssuer<'static, KeyPair>;

fn new_ca(common_name: &str) -> Ca {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap()
}

struct Leaf {
    cert: CertificateDer<'static>,
    cert_pem: String,
    key: KeyPair,
}

fn leaf(issuer: &Ca, name: &str, purpose: ExtendedKeyUsagePurpose) -> Leaf {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![name.to_owned()]).unwrap();
    params.extended_key_usages = vec![purpose];
    params.distinguished_name.push(DnType::CommonName, name);
    let cert = params.signed_by(&key, issuer).unwrap();
    Leaf {
        cert: cert.der().clone(),
        cert_pem: cert.pem(),
        key,
    }
}

/// A TLS echo server on a tokio runtime of its own; stops when dropped.
struct Server {
    addr: SocketAddr,
    _runtime: tokio::runtime::Runtime,
}

impl Server {
    fn start(leaf: &Leaf, client_ca: Option<&Ca>) -> Server {
        let provider = crypto_provider();
        let builder = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap();
        let builder = match client_ca {
            None => builder.with_no_client_auth(),
            Some(ca) => {
                let mut roots = RootCertStore::empty();
                roots.add(ca.der().clone()).unwrap();
                let verifier =
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                        .build()
                        .unwrap();
                builder.with_client_cert_verifier(verifier)
            }
        };
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf.key.serialize_der()));
        let config = builder
            .with_single_cert(vec![leaf.cert.clone()], key)
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let addr = listener.local_addr().unwrap();
        runtime.spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let mut buf = vec![0_u8; 16 * 1024];
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
        Server {
            addr,
            _runtime: runtime,
        }
    }
}

fn connector(config: TlsConfig) -> TlsConnector {
    TlsConnector::new(&config.load_with(|_| None, None).unwrap()).unwrap()
}

async fn connect(
    connector: &TlsConnector,
    addr: SocketAddr,
) -> std::io::Result<TlsStream<SmolStream>> {
    let tcp = SmolRuntime::new().connect(addr).await?;
    connector.connect(tcp).await
}

#[test]
fn handshake_echo_a_megabyte_and_a_graceful_close() {
    let ca = new_ca("Test CA");
    let server_leaf = leaf(&ca, "server.example", ExtendedKeyUsagePurpose::ServerAuth);
    let server = Server::start(&server_leaf, None);
    let connector = connector(TlsConfig::new().ca_bytes(ca.pem()));

    block_on(async {
        let stream = connect(&connector, server.addr).await.unwrap();
        assert_eq!(stream.peer_certificates().unwrap()[0], server_leaf.cert);

        // A megabyte written while another task reads it back: the writer sees `Pending`.
        let payload: Vec<u8> = (0..(1_u32 << 20)).map(|i| (i % 251) as u8).collect();
        let (mut reader, mut writer) = futures_lite::io::split(stream);
        let rt = SmolRuntime::new();
        let expected = payload.clone();
        let read_back = rt.spawn(async move {
            let mut back = vec![0_u8; expected.len()];
            reader.read_exact(&mut back).await.unwrap();
            assert!(back == expected, "the echo differs");
            reader
        });
        writer.write_all(&payload).await.unwrap();
        writer.flush().await.unwrap();
        let mut reader = read_back.await.unwrap();

        // close_notify, then the server closes and we read EOF.
        writer.close().await.unwrap();
        let mut rest = Vec::new();
        assert_eq!(reader.read_to_end(&mut rest).await.unwrap(), 0);
    });
}

#[test]
fn a_client_certificate_is_presented_and_an_untrusted_server_is_rejected() {
    let ca = new_ca("Test CA");
    let server_leaf = leaf(&ca, "server.example", ExtendedKeyUsagePurpose::ServerAuth);
    let client_leaf = leaf(&ca, "client", ExtendedKeyUsagePurpose::ClientAuth);
    let server = Server::start(&server_leaf, Some(&ca));

    block_on(async {
        let with_cert = connector(
            TlsConfig::new()
                .ca_bytes(ca.pem())
                .cert_bytes(client_leaf.cert_pem.clone())
                .key_bytes(client_leaf.key.serialize_pem()),
        );
        assert!(with_cert.has_client_certificate());
        let mut stream = connect(&with_cert, server.addr).await.unwrap();
        stream.write_all(b"hello").await.unwrap();
        stream.flush().await.unwrap();
        let mut back = [0_u8; 5];
        stream.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"hello");

        // Trusting a different CA: the server's certificate is rejected by the client.
        let other = new_ca("Other CA");
        let distrustful = connector(TlsConfig::new().ca_bytes(other.pem()));
        let err = match connect(&distrustful, server.addr).await {
            Ok(_) => panic!("an untrusted server was accepted"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
    });
}
