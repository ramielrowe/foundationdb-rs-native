//! Test machinery for the cluster monitor: an in-memory [`Runtime`] on tokio's clock (so the
//! tests run under paused time with no real I/O), with a hostname table, and scripted fake
//! coordinators that speak the ConnectPacket handshake and frames with the transport's own
//! types and hand every `OpenDatabaseCoordRequest` to the test.

#![allow(dead_code, unreachable_pub, missing_debug_implementations)]

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use fdb_native_schema::domain::{
    ClientDbInfo, CommitProxy, GrvProxy, OpenDatabaseCoord, TenantMode,
};
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::connection::{PING_REPLY_OK, PingRequest, well_known};
use fdb_native_transport::runtime::{Instant, Runtime, TokioRuntime, TokioTask};
use fdb_native_transport::{Checksum, ConnectPacket, FrameConfig, FrameDecoder};
use fdb_native_wire::{
    Endpoint, FdbError, NetworkAddress, NetworkAddressList, ProtocolVersion, Uid,
};
use futures_io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::mpsc;

pub mod process;

pub fn sock(last: u8, port: u16) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, last], port))
}

// ---- the in-memory network -----------------------------------------------------------------

#[derive(Default)]
pub struct Net {
    listeners: Mutex<HashMap<SocketAddr, mpsc::UnboundedSender<DuplexStream>>>,
    hosts: Mutex<HashMap<String, Vec<std::net::IpAddr>>>,
    connects: Mutex<HashMap<SocketAddr, usize>>,
    resolves: Mutex<Vec<String>>,
}

impl Net {
    pub fn new() -> Arc<Net> {
        Arc::new(Net::default())
    }

    /// Makes `host` resolve to `ips` (any port).
    pub fn set_host(&self, host: &str, ips: Vec<std::net::IpAddr>) {
        self.hosts.lock().unwrap().insert(host.to_owned(), ips);
    }

    /// How many times `addr` was dialed.
    pub fn connects(&self, addr: SocketAddr) -> usize {
        self.connects
            .lock()
            .unwrap()
            .get(&addr)
            .copied()
            .unwrap_or(0)
    }

    /// The host names resolved so far.
    pub fn resolves(&self) -> Vec<String> {
        self.resolves.lock().unwrap().clone()
    }

    fn unlisten(&self, addr: SocketAddr) {
        self.listeners.lock().unwrap().remove(&addr);
    }
}

/// A [`DuplexStream`] with `futures_io`'s traits.
#[derive(Debug)]
pub struct MemStream(DuplexStream);

impl AsyncRead for MemStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut read_buf = tokio::io::ReadBuf::new(buf);
        ready!(tokio::io::AsyncRead::poll_read(
            Pin::new(&mut self.0),
            cx,
            &mut read_buf
        ))?;
        Poll::Ready(Ok(read_buf.filled().len()))
    }
}

impl AsyncWrite for MemStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.0), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(Pin::new(&mut self.0), cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(Pin::new(&mut self.0), cx)
    }
}

/// Time and tasks from tokio, connections and names from a [`Net`].
#[derive(Clone)]
pub struct MemRuntime {
    tokio: TokioRuntime,
    net: Arc<Net>,
}

impl MemRuntime {
    pub fn new(net: &Arc<Net>) -> MemRuntime {
        MemRuntime {
            tokio: TokioRuntime::new(),
            net: net.clone(),
        }
    }
}

impl Runtime for MemRuntime {
    type Stream = MemStream;
    type Task<T: Send + 'static> = TokioTask<T>;

    fn now(&self) -> Instant {
        self.tokio.now()
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.tokio.sleep(duration)
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send {
        self.tokio.sleep_until(deadline)
    }

    fn spawn<T, F>(&self, future: F) -> TokioTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.tokio.spawn(future)
    }

    async fn connect(&self, addr: SocketAddr) -> io::Result<MemStream> {
        *self.net.connects.lock().unwrap().entry(addr).or_insert(0) += 1;
        let listener = self.net.listeners.lock().unwrap().get(&addr).cloned();
        let Some(tx) = listener else {
            return Err(io::ErrorKind::ConnectionRefused.into());
        };
        let (client, server) = tokio::io::duplex(64 * 1024);
        tx.send(server)
            .map_err(|_| io::Error::from(io::ErrorKind::ConnectionRefused))?;
        Ok(MemStream(client))
    }

    async fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        self.net.resolves.lock().unwrap().push(host.to_owned());
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        match self.net.hosts.lock().unwrap().get(host) {
            Some(ips) => Ok(ips.iter().map(|ip| SocketAddr::new(*ip, port)).collect()),
            None => Err(io::Error::new(io::ErrorKind::NotFound, "unknown host")),
        }
    }
}

// ---- fake coordinators ---------------------------------------------------------------------

enum Cmd {
    Send(Uid, Vec<u8>),
    Close,
}

/// One `OpenDatabaseCoordRequest` a coordinator received. Dropping it without answering
/// leaves the request pending (a long poll).
pub struct Call {
    pub request: OpenDatabaseCoord,
    pub reply_token: Uid,
    /// The schema set the coordinator decoded it with (its version's).
    pub schema: SchemaVersion,
    /// The coordinator it reached.
    pub coordinator: SocketAddr,
    conn: mpsc::UnboundedSender<Cmd>,
}

impl Call {
    /// Answers with `info`, encoded as the coordinator's version writes it.
    pub fn reply(&self, info: &ClientDbInfo) {
        let payload = OpenDatabaseCoord::encode_reply(self.schema, &Ok(info.clone())).unwrap();
        let _ = self.conn.send(Cmd::Send(self.reply_token, payload));
    }

    /// Answers with an FDB error.
    pub fn reply_error(&self, code: u16) {
        let payload =
            OpenDatabaseCoord::encode_reply(self.schema, &Err(FdbError::new(code))).unwrap();
        let _ = self.conn.send(Cmd::Send(self.reply_token, payload));
    }

    /// Hangs up the connection the request came on.
    pub fn close_connection(&self) {
        let _ = self.conn.send(Cmd::Close);
    }
}

/// A coordinator at one address. Every request it receives is handed to the test through
/// [`next_call`](FakeCoordinator::next_call).
pub struct FakeCoordinator {
    pub addr: SocketAddr,
    net: Arc<Net>,
    version: Arc<AtomicU64>,
    calls: mpsc::UnboundedReceiver<Call>,
    conns: Arc<Mutex<Vec<mpsc::UnboundedSender<Cmd>>>>,
    announced: Arc<Mutex<Vec<ProtocolVersion>>>,
}

impl FakeCoordinator {
    /// Listens at `addr`, speaking `version`. Call it inside the tokio runtime.
    pub fn start(net: &Arc<Net>, addr: SocketAddr, version: ProtocolVersion) -> FakeCoordinator {
        let (tx, mut rx) = mpsc::unbounded_channel::<DuplexStream>();
        net.listeners.lock().unwrap().insert(addr, tx);
        let version = Arc::new(AtomicU64::new(version.0));
        let (calls_tx, calls) = mpsc::unbounded_channel();
        let conns = Arc::new(Mutex::new(Vec::new()));
        let announced = Arc::new(Mutex::new(Vec::new()));
        {
            let version = version.clone();
            let conns = conns.clone();
            let announced = announced.clone();
            tokio::spawn(async move {
                while let Some(stream) = rx.recv().await {
                    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
                    conns.lock().unwrap().push(cmd_tx.clone());
                    let v = ProtocolVersion(version.load(Ordering::SeqCst));
                    tokio::spawn(serve(
                        stream,
                        addr,
                        v,
                        calls_tx.clone(),
                        cmd_tx,
                        cmd_rx,
                        announced.clone(),
                    ));
                }
            });
        }
        FakeCoordinator {
            addr,
            net: net.clone(),
            version,
            calls,
            conns,
            announced,
        }
    }

    pub fn address(&self) -> NetworkAddress {
        NetworkAddress::new(self.addr.ip().into(), self.addr.port(), true, false)
    }

    /// The next request, waiting up to 60 s of (virtual) time.
    pub async fn next_call(&mut self) -> Call {
        tokio::time::timeout(Duration::from_secs(60), self.calls.recv())
            .await
            .unwrap_or_else(|_| panic!("no request reached {} within 60 s", self.addr))
            .expect("coordinator running")
    }

    /// A request already received, if any.
    pub fn try_call(&mut self) -> Option<Call> {
        self.calls.try_recv().ok()
    }

    /// The versions clients announced, one per connection.
    pub fn announced(&self) -> Vec<ProtocolVersion> {
        self.announced.lock().unwrap().clone()
    }

    /// Makes the coordinator speak `version` on its next connections (an upgrade); the
    /// current ones keep theirs until closed.
    pub fn set_version(&self, version: ProtocolVersion) {
        self.version.store(version.0, Ordering::SeqCst);
    }

    /// Hangs up every connection, still listening (a restart).
    pub fn close_connections(&self) {
        for conn in self.conns.lock().unwrap().drain(..) {
            let _ = conn.send(Cmd::Close);
        }
    }

    /// Stops listening and hangs up every connection: the coordinator is down.
    pub fn stop(&self) {
        self.net.unlisten(self.addr);
        for conn in self.conns.lock().unwrap().drain(..) {
            let _ = conn.send(Cmd::Close);
        }
    }
}

async fn serve(
    mut stream: DuplexStream,
    listen: SocketAddr,
    version: ProtocolVersion,
    calls: mpsc::UnboundedSender<Call>,
    cmd_tx: mpsc::UnboundedSender<Cmd>,
    mut cmds: mpsc::UnboundedReceiver<Cmd>,
    announced: Arc<Mutex<Vec<ProtocolVersion>>>,
) {
    let mut hello = [0u8; ConnectPacket::SIZE];
    if stream.read_exact(&mut hello).await.is_err() {
        return;
    }
    let (client, _) = ConnectPacket::decode(&hello).expect("client's ConnectPacket");
    announced
        .lock()
        .unwrap()
        .push(client.protocol_version.without_flags());
    let packet = ConnectPacket::server(version, listen);
    if stream.write_all(&packet.encode().unwrap()).await.is_err() {
        return;
    }
    if !client.protocol_version.is_compatible(version) {
        // As fdbserver does: say who we are, then hang up.
        return;
    }
    let schema = SchemaVersion::for_protocol_version(version).expect("a supported version");
    let config = FrameConfig::new(Checksum::for_tls(false));
    let mut decoder = FrameDecoder::new(config);
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        tokio::select! {
            read = stream.read(&mut buf) => {
                let Ok(n) = read else { return };
                if n == 0 {
                    return;
                }
                decoder.push(&buf[..n]);
                while let Some(frame) = decoder.next_frame().expect("client frames") {
                    let frame = frame.into_owned();
                    if frame.token == well_known::PING_PACKET {
                        let ping: PingRequest =
                            fdb_native_wire::decode(&frame.payload).expect("ping");
                        let payload = fdb_native_wire::encode(&PING_REPLY_OK).unwrap();
                        let token = ping.reply.token();
                        if write_frame(&mut stream, config, token, &payload).await.is_err() {
                            return;
                        }
                    } else if frame.token == well_known::CLIENTLEADERREG_OPENDATABASE {
                        let (request, reply_token) =
                            OpenDatabaseCoord::decode_request(schema, &frame.payload)
                                .expect("request");
                        let _ = calls.send(Call {
                            request,
                            reply_token,
                            schema,
                            coordinator: listen,
                            conn: cmd_tx.clone(),
                        });
                    }
                }
            }
            cmd = cmds.recv() => match cmd {
                None | Some(Cmd::Close) => return,
                Some(Cmd::Send(token, payload)) => {
                    if write_frame(&mut stream, config, token, &payload).await.is_err() {
                        return;
                    }
                }
            },
        }
    }
}

async fn write_frame(
    stream: &mut DuplexStream,
    config: FrameConfig,
    token: Uid,
    payload: &[u8],
) -> io::Result<()> {
    let mut out = Vec::new();
    config
        .encode_frame(token, payload, &mut out)
        .expect("frame");
    stream.write_all(&out).await
}

// ---- infos -------------------------------------------------------------------------------------

pub fn endpoint(ip_last: u8, port: u16, token: Uid) -> Endpoint {
    Endpoint::new(
        NetworkAddressList {
            address: NetworkAddress::new(sock(ip_last, port).ip().into(), port, true, false),
            secondary_address: None,
        },
        token,
    )
}

/// An info with `grv` GRV proxies and `commit` commit proxies, decoded as `schema`.
pub fn info(id: u64, grv: usize, commit: usize, schema: SchemaVersion) -> ClientDbInfo {
    ClientDbInfo {
        id: Uid::new(0xdb, id),
        grv_proxies: (0..grv)
            .map(|i| GrvProxy {
                process_id: Some(format!("grv-{i}").into_bytes()),
                provisional: false,
                base: endpoint(
                    100 + i as u8,
                    4500,
                    Uid::new(0x1000 + id, 0x10 + i as u64 * 0x100),
                ),
            })
            .collect(),
        commit_proxies: (0..commit)
            .map(|i| CommitProxy {
                process_id: Some(format!("cp-{i}").into_bytes()),
                provisional: false,
                base: endpoint(
                    150 + i as u8,
                    4500,
                    Uid::new(0x2000 + id, 0x10 + i as u64 * 0x100),
                ),
                schema,
            })
            .collect(),
        forward: None,
        history: Vec::new(),
        cluster_id: Uid::new(0xc1, 0xd),
        cluster_type: 0,
        tenant_mode: match schema {
            SchemaVersion::V7_4 => Some(TenantMode::DISABLED),
            SchemaVersion::V8_0 => None,
        },
    }
}

/// What `leaderServer` sends when the cluster moved.
pub fn forward(id: u64, connection_string: &str) -> ClientDbInfo {
    ClientDbInfo {
        id: Uid::new(0xf0, id),
        grv_proxies: Vec::new(),
        commit_proxies: Vec::new(),
        forward: Some(connection_string.as_bytes().to_vec()),
        history: Vec::new(),
        cluster_id: Uid::default(),
        cluster_type: 0,
        tenant_mode: None,
    }
}

/// Polls `cond` every 10 ms of (virtual) time for up to 60 s.
pub async fn until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..6000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}
