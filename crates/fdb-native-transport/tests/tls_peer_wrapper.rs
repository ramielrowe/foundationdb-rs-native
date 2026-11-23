//! `tls::peer::TlsStreamWrapper` against a rustls server on loopback: the truncated close
//! FoundationDB sends reads as end of stream, and a handshake that never completes is bounded
//! by the peer manager's connect timeout.

#![cfg(all(feature = "tls", feature = "tokio"))]

use std::sync::Arc;
use std::time::{Duration, Instant};

use fdb_native_testkit::TestPki;
use fdb_native_transport::peer::{
    PeerError, PeerManager, PeerManagerConfig, StreamWrapper, event_channel,
};
use fdb_native_transport::runtime::{Runtime, TokioRuntime};
use fdb_native_transport::tls::peer::TlsStreamWrapper;
use fdb_native_transport::tls::rustls::pki_types::pem::PemObject;
use fdb_native_transport::tls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use fdb_native_transport::tls::rustls::{ServerConfig, crypto::CryptoProvider};
use fdb_native_transport::tls::{TlsConfig, TlsConnector, crypto_provider};
use fdb_native_wire::NetworkAddress;
use futures_lite::io::AsyncReadExt;
use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

fn acceptor(pki: &TestPki) -> TlsAcceptor {
    let provider: Arc<CryptoProvider> = crypto_provider();
    let chain = vec![CertificateDer::from_pem_slice(pki.server().cert_pem().as_bytes()).unwrap()];
    let key = PrivateKeyDer::from_pem_slice(pki.server().key_pem().as_bytes()).unwrap();
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
    TlsAcceptor::from(Arc::new(config))
}

fn wrapper(pki: &TestPki) -> TlsStreamWrapper {
    let loaded = TlsConfig::new()
        .ca_bytes(pki.ca_pem())
        .load_with(|_| None, None)
        .unwrap();
    TlsStreamWrapper::new(TlsConnector::new(&loaded).unwrap())
}

#[tokio::test]
async fn truncated_close_is_end_of_stream() {
    let pki = TestPki::generate().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = acceptor(&pki);
    let server = tokio::spawn(async move {
        // Two clients: each gets a greeting and then a plain TCP close, without close_notify
        // (as FoundationDB's `closeSocket` does).
        for _ in 0..2 {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            tls.write_all(b"hello").await.unwrap();
            tls.flush().await.unwrap();
            drop(tls);
        }
    });

    let rt = TokioRuntime::new();
    let wrapper = wrapper(&pki);
    let peer = NetworkAddress::new(addr.ip().into(), addr.port(), true, true);

    // The wrapper's stream: the bytes, then end of stream, and again.
    let tcp = rt.connect(addr).await.unwrap();
    let mut stream = wrapper.wrap(tcp, peer).await.unwrap();
    let mut buf = [0u8; 16];
    let mut got = Vec::new();
    loop {
        match stream.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
            Err(e) => panic!("truncation must read as end of stream: {e}"),
        }
    }
    assert_eq!(got, b"hello");
    assert_eq!(stream.read(&mut buf).await.unwrap(), 0);
    assert!(stream.tls().protocol_version().is_some());

    // The plain TlsStream reports the same close as an error: the reason for the adapter.
    let tcp = rt.connect(addr).await.unwrap();
    let connector = wrapper.connector().clone();
    let mut raw = connector.connect(tcp).await.unwrap();
    let mut got = Vec::new();
    let error = loop {
        match raw.read(&mut buf).await {
            Ok(0) => panic!("expected UnexpectedEof, got end of stream"),
            Ok(n) => got.extend_from_slice(&buf[..n]),
            Err(e) => break e,
        }
    };
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    server.await.unwrap();
}

/// A peer that accepts the TCP connection and then says nothing: the handshake is part of the
/// manager's single connect timeout and fails as `TlsTimeout`, not as a connect timeout.
#[tokio::test]
async fn silent_server_times_out_in_the_handshake() {
    let pki = TestPki::generate().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let silent = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(30)).await;
        drop(tcp);
    });

    let mut config = PeerManagerConfig::default();
    config.connect_timeout = Duration::from_millis(300);
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(TokioRuntime::new(), config, wrapper(&pki), sink);
    let peer = mgr.peer(&NetworkAddress::new(
        addr.ip().into(),
        addr.port(),
        true,
        true,
    ));

    let started = Instant::now();
    let error = tokio::time::timeout(Duration::from_secs(10), peer.connect())
        .await
        .expect("connect finishes")
        .unwrap_err();
    assert!(
        matches!(error, PeerError::TlsTimeout { after } if after == Duration::from_millis(300)),
        "{error:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    mgr.shutdown().await;
    silent.abort();
}
