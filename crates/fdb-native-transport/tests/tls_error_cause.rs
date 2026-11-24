//! TLS failures keep their cause on the way up: through `ConnectionError::Io` (a server that
//! refuses our client certificate, heard from on the first read with TLS 1.3) and
//! `PeerError::Tls` (we refuse the server's certificate), in `Display` and in the
//! `std::error::Error::source` chain. Against rustls servers on loopback; the same cases
//! against fdbserver are in `peer_tls_docker`.

#![cfg(all(feature = "tls", feature = "tokio"))]

use std::error::Error as _;
use std::sync::Arc;
use std::time::Duration;

use fdb_native_testkit::TestPki;
use fdb_native_transport::connection::well_known;
use fdb_native_transport::peer::{
    PeerError, PeerManager, PeerManagerConfig, ProtocolInfoReply, event_channel,
};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_transport::tls::peer::TlsStreamWrapper;
use fdb_native_transport::tls::rustls::pki_types::pem::PemObject;
use fdb_native_transport::tls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use fdb_native_transport::tls::rustls::server::WebPkiClientVerifier;
use fdb_native_transport::tls::rustls::{
    AlertDescription, CertificateError, Error as RustlsError, RootCertStore, ServerConfig,
    crypto::CryptoProvider,
};
use fdb_native_transport::tls::{TlsConfig, TlsConnector, crypto_provider};
use fdb_native_transport::{ConnectionError, tls};
use fdb_native_wire::{EncodeError, NetworkAddress};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// A server with `pki`'s server certificate; with `require_client_cert` it demands a client
/// certificate signed by `pki`'s CA.
fn acceptor(pki: &TestPki, require_client_cert: bool) -> TlsAcceptor {
    let provider: Arc<CryptoProvider> = crypto_provider();
    let chain = vec![CertificateDer::from_pem_slice(pki.server().cert_pem().as_bytes()).unwrap()];
    let key = PrivateKeyDer::from_pem_slice(pki.server().key_pem().as_bytes()).unwrap();
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap();
    let builder = if require_client_cert {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(pki.ca_pem().as_bytes()).unwrap())
            .unwrap();
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .unwrap();
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    TlsAcceptor::from(Arc::new(builder.with_single_cert(chain, key).unwrap()))
}

/// A manager whose client trusts `ca` (and presents `identity`, a certificate and key PEM).
fn manager(
    ca: &TestPki,
    identity: Option<(&str, &str)>,
) -> PeerManager<TokioRuntime, TlsStreamWrapper> {
    let mut config = TlsConfig::new().ca_bytes(ca.ca_pem());
    if let Some((cert, key)) = identity {
        config = config.cert_bytes(cert).key_bytes(key);
    }
    let loaded = config.load_with(|_| None, None).unwrap();
    let wrapper = TlsStreamWrapper::new(TlsConnector::new(&loaded).unwrap());
    let (sink, _events) = event_channel();
    PeerManager::with_stream_wrapper(
        TokioRuntime::new(),
        PeerManagerConfig::default(),
        wrapper,
        sink,
    )
}

async fn listen() -> (TcpListener, NetworkAddress) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let address = NetworkAddress::new(addr.ip().into(), addr.port(), true, true);
    (listener, address)
}

/// With TLS 1.3 our handshake completes before the server has checked our certificate, so a
/// server that refuses it is heard from on the first read, as a TLS alert. The connection
/// error keeps it: in its message, in its `source` chain and as the rustls alert.
#[tokio::test]
async fn a_refused_client_certificate_is_kept_as_the_connection_error_cause() {
    let pki = TestPki::generate().unwrap();
    let intruder = TestPki::generate_named("some-other-ca").unwrap();
    let (listener, address) = listen().await;
    let acceptor = acceptor(&pki, true);
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        // The server's handshake fails: it sends an alert and drops the connection.
        assert!(acceptor.accept(tcp).await.is_err());
    });

    // Trust the server's CA but present a certificate from another one.
    let identity = intruder.client();
    let mgr = manager(&pki, Some((identity.cert_pem(), identity.key_pem())));
    let peer = mgr.peer(&address);
    let error = tokio::time::timeout(Duration::from_secs(10), peer.connect())
        .await
        .expect("connect finishes")
        .unwrap_err();

    let PeerError::Connection(ConnectionError::Io(io)) = &error else {
        panic!("expected an I/O error from the first read: {error:?}");
    };
    assert_eq!(io.kind(), std::io::ErrorKind::InvalidData);
    let alert =
        tls::alert_received(io.io_error().unwrap()).unwrap_or_else(|| panic!("no alert in {io:?}"));
    // What a server sends for a client certificate from an unknown CA.
    assert!(
        matches!(
            alert,
            AlertDescription::UnknownCA
                | AlertDescription::BadCertificate
                | AlertDescription::CertificateUnknown
        ),
        "{alert:?}"
    );

    // Display and the source chain say it, not just "invalid data".
    let text = error.to_string();
    assert!(text.contains("received fatal alert"), "{text}");
    assert!(text.contains(&format!("{alert:?}")), "{text}");
    // `PeerError::Connection` is transparent: its source is the connection error's.
    let io_source = error.source().expect("the I/O error");
    assert!(io_source.to_string().contains(&format!("{alert:?}")));
    let cause = io_source.source().expect("the TLS library's error");
    assert!(matches!(
        cause.downcast_ref::<RustlsError>(),
        Some(RustlsError::AlertReceived(a)) if *a == alert
    ));

    server.await.unwrap();
    mgr.shutdown().await;
}

/// The reverse: we refuse the server's certificate. The handshake fails in the stream wrapper
/// (`PeerError::Tls`) with the rustls error as its source, and a request that was not sent
/// because of it carries the same cause.
#[tokio::test]
async fn a_refused_server_certificate_is_kept_as_the_peer_error_cause() {
    let pki = TestPki::generate().unwrap();
    let other = TestPki::generate_named("some-other-ca").unwrap();
    let (listener, address) = listen().await;
    let acceptor = acceptor(&pki, false);
    let server = tokio::spawn(async move {
        // The client refuses the certificate and ends the handshake, each time it retries
        // (a request waits for the next attempt).
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            assert!(acceptor.accept(tcp).await.is_err());
        }
    });

    // Trust another CA: the server's certificate is from an unknown issuer.
    let mgr = manager(&other, None);
    let peer = mgr.peer(&address);
    let error = tokio::time::timeout(Duration::from_secs(10), peer.connect())
        .await
        .expect("connect finishes")
        .unwrap_err();

    let PeerError::Tls {
        kind,
        message,
        source,
    } = &error
    else {
        panic!("expected a TLS error: {error:?}");
    };
    assert_eq!(*kind, std::io::ErrorKind::InvalidData);
    assert!(
        message.to_lowercase().contains("unknownissuer"),
        "{message}"
    );
    let unknown_issuer = |e: Option<&RustlsError>| {
        matches!(
            e,
            Some(RustlsError::InvalidCertificate(
                CertificateError::UnknownIssuer
            ))
        )
    };
    assert!(unknown_issuer(tls::rustls_error(
        source.io_error().unwrap()
    )));
    let io_source = error.source().expect("the I/O error");
    let cause = io_source.source().expect("the TLS library's error");
    assert!(unknown_issuer(cause.downcast_ref::<RustlsError>()));

    // A request refused for it says why.
    let not_sent = peer
        .request_as::<ProtocolInfoReply, _>(
            well_known::PROTOCOL_INFO,
            |_reply, _version, _out| -> Result<(), EncodeError> { Ok(()) },
        )
        .await
        .unwrap_err();
    let RequestError::NotSent(Some(ConnectionError::Io(io))) = &not_sent else {
        panic!("{not_sent:?}");
    };
    assert!(unknown_issuer(tls::rustls_error(io.io_error().unwrap())));
    assert!(
        not_sent
            .to_string()
            .to_lowercase()
            .contains("unknownissuer")
    );

    server.abort();
    mgr.shutdown().await;
}
