//! Shared test machinery of the peer manager tests: an in-memory [`Runtime`] on tokio's clock
//! (so tests run under paused time without any real I/O racing the virtual clock) and a
//! scripted FDB-like server that speaks the ConnectPacket handshake and frames with this
//! crate's own types.

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

use fdb_native_transport::connection::{PING_REPLY_OK, PingRequest, well_known};
use fdb_native_transport::runtime::{Instant, Runtime, TokioRuntime, TokioTask};
use fdb_native_transport::{Checksum, ConnectPacket, FrameConfig, FrameDecoder, OwnedFrame};
use fdb_native_wire::{NetworkAddress, ProtocolVersion, Uid};
use futures_io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::mpsc;

pub fn na(addr: SocketAddr, tls: bool) -> NetworkAddress {
    NetworkAddress::new(addr.ip().into(), addr.port(), true, tls)
}

pub fn sock(port: u16) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, 1], port))
}

// ---- the in-memory network -----------------------------------------------------------------

/// What connecting to an address does.
pub enum Behavior {
    /// Connection refused at once.
    Refuse,
    /// The connect never completes.
    Hang,
    /// Hands the server half of an in-memory pipe (with this buffer size) to a listener.
    Accept {
        tx: mpsc::UnboundedSender<DuplexStream>,
        capacity: usize,
    },
}

#[derive(Default)]
pub struct Net {
    behaviors: Mutex<HashMap<SocketAddr, Behavior>>,
    connects: Mutex<HashMap<SocketAddr, usize>>,
}

impl Net {
    pub fn new() -> Arc<Net> {
        Arc::new(Net::default())
    }

    pub fn set(&self, addr: SocketAddr, behavior: Behavior) {
        self.behaviors.lock().unwrap().insert(addr, behavior);
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
}

/// A [`DuplexStream`] with `futures_io`'s traits.
#[derive(Debug)]
pub struct MemStream(pub DuplexStream);

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

/// Time and tasks from tokio, connections from a [`Net`].
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
        enum Plan {
            Refuse,
            Hang,
            Accept(mpsc::UnboundedSender<DuplexStream>, usize),
        }
        let plan = match self.net.behaviors.lock().unwrap().get(&addr) {
            None | Some(Behavior::Refuse) => Plan::Refuse,
            Some(Behavior::Hang) => Plan::Hang,
            Some(Behavior::Accept { tx, capacity }) => Plan::Accept(tx.clone(), *capacity),
        };
        match plan {
            Plan::Refuse => Err(io::ErrorKind::ConnectionRefused.into()),
            Plan::Hang => std::future::pending().await,
            Plan::Accept(tx, capacity) => {
                let (client, server) = tokio::io::duplex(capacity);
                tx.send(server)
                    .map_err(|_| io::Error::from(io::ErrorKind::ConnectionRefused))?;
                Ok(MemStream(client))
            }
        }
    }

    async fn resolve(&self, _host: &str, _port: u16) -> io::Result<Vec<SocketAddr>> {
        Ok(Vec::new())
    }
}

// ---- the scripted server -------------------------------------------------------------------

pub type Responder = Arc<dyn Fn(&OwnedFrame) -> Vec<(Uid, Vec<u8>)> + Send + Sync>;

/// How the server behaves on one connection.
#[derive(Clone)]
pub struct Script {
    /// The version it speaks (shared, so a test can "upgrade" the cluster).
    pub version: Arc<AtomicU64>,
    /// Whether it sends its ConnectPacket (after reading the client's).
    pub hello: bool,
    /// Hang up right after reading the client's ConnectPacket, without answering.
    pub close_before_hello: bool,
    /// Hang up this long after the handshake.
    pub close_after: Option<Duration>,
    /// Whether it answers pings.
    pub answer_pings: bool,
    /// Whether it reads at all after the handshake.
    pub read: bool,
    /// Whether the connection is TLS (frames carry no checksum).
    pub tls: bool,
    /// Frames to send back for a frame received.
    pub responder: Option<Responder>,
}

impl Script {
    pub fn new(version: ProtocolVersion) -> Script {
        Script {
            version: Arc::new(AtomicU64::new(version.0)),
            hello: true,
            close_before_hello: false,
            close_after: None,
            answer_pings: true,
            read: true,
            tls: false,
            responder: None,
        }
    }

    pub fn current_version(&self) -> ProtocolVersion {
        ProtocolVersion(self.version.load(Ordering::SeqCst))
    }
}

/// What a server saw of one connection.
#[derive(Clone, Debug)]
pub struct ConnRecord {
    pub at: tokio::time::Instant,
    pub announced: Option<ProtocolVersion>,
    pub connection_id: u64,
    pub frames: Vec<OwnedFrame>,
    pub pings: usize,
    pub closed_at: Option<tokio::time::Instant>,
}

pub enum ServerCmd {
    Send(Uid, Vec<u8>),
    Close,
    StartReading,
}

pub struct ServerConn {
    pub index: usize,
    pub tx: mpsc::UnboundedSender<ServerCmd>,
}

impl ServerConn {
    pub fn send(&self, token: Uid, payload: Vec<u8>) {
        let _ = self.tx.send(ServerCmd::Send(token, payload));
    }

    pub fn close(&self) {
        let _ = self.tx.send(ServerCmd::Close);
    }

    pub fn start_reading(&self) {
        let _ = self.tx.send(ServerCmd::StartReading);
    }
}

pub struct Server {
    pub addr: SocketAddr,
    pub address: NetworkAddress,
    pub records: Arc<Mutex<Vec<ConnRecord>>>,
    pub conns: mpsc::UnboundedReceiver<ServerConn>,
}

impl Server {
    /// The control handle of the next accepted connection not handed out yet.
    pub async fn conns_next(&mut self) -> ServerConn {
        self.conns.recv().await.expect("a connection")
    }

    pub fn connections(&self) -> usize {
        self.records.lock().unwrap().len()
    }

    pub fn record(&self, index: usize) -> ConnRecord {
        self.records.lock().unwrap()[index].clone()
    }

    /// The versions announced by each connection, in order.
    pub fn announced(&self) -> Vec<Option<ProtocolVersion>> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.announced)
            .collect()
    }

    pub fn frames(&self, index: usize) -> usize {
        self.records.lock().unwrap()[index].frames.len()
    }
}

/// Starts a server at `addr`: connection `i` follows `scripts[min(i, len - 1)]`.
pub fn listen(net: &Arc<Net>, addr: SocketAddr, scripts: Vec<Script>, capacity: usize) -> Server {
    assert!(!scripts.is_empty());
    let tls = scripts[0].tls;
    let (tx, mut rx) = mpsc::unbounded_channel::<DuplexStream>();
    net.set(addr, Behavior::Accept { tx, capacity });
    let records = Arc::new(Mutex::new(Vec::<ConnRecord>::new()));
    let (conn_tx, conns) = mpsc::unbounded_channel();
    let accepted = records.clone();
    tokio::spawn(async move {
        while let Some(stream) = rx.recv().await {
            let index = {
                let mut r = accepted.lock().unwrap();
                r.push(ConnRecord {
                    at: tokio::time::Instant::now(),
                    announced: None,
                    connection_id: 0,
                    frames: Vec::new(),
                    pings: 0,
                    closed_at: None,
                });
                r.len() - 1
            };
            let script = scripts[index.min(scripts.len() - 1)].clone();
            let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
            let _ = conn_tx.send(ServerConn { index, tx: cmd_tx });
            tokio::spawn(serve(stream, script, accepted.clone(), index, cmd_rx, addr));
        }
    });
    Server {
        addr,
        address: na(addr, tls),
        records,
        conns,
    }
}

/// A server with one script for every connection.
pub fn listen_one(net: &Arc<Net>, addr: SocketAddr, script: Script) -> Server {
    listen(net, addr, vec![script], 64 * 1024)
}

pub async fn serve<S>(
    mut stream: S,
    script: Script,
    records: Arc<Mutex<Vec<ConnRecord>>>,
    index: usize,
    mut cmds: mpsc::UnboundedReceiver<ServerCmd>,
    listen: SocketAddr,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let frame_config = FrameConfig::new(Checksum::for_tls(script.tls));
    let result = serve_inner(
        &mut stream,
        &script,
        &records,
        index,
        &mut cmds,
        listen,
        frame_config,
    )
    .await;
    let _ = result;
    records.lock().unwrap()[index].closed_at = Some(tokio::time::Instant::now());
}

async fn serve_inner<S>(
    stream: &mut S,
    script: &Script,
    records: &Arc<Mutex<Vec<ConnRecord>>>,
    index: usize,
    cmds: &mut mpsc::UnboundedReceiver<ServerCmd>,
    listen: SocketAddr,
    frame_config: FrameConfig,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let mut hello = [0u8; ConnectPacket::SIZE];
    stream.read_exact(&mut hello).await?;
    let (client, _) = ConnectPacket::decode(&hello).expect("client's ConnectPacket");
    {
        let mut r = records.lock().unwrap();
        r[index].announced = Some(client.protocol_version.without_flags());
        r[index].connection_id = client.connection_id;
    }
    if script.close_before_hello {
        return Ok(());
    }
    if script.hello {
        let packet = ConnectPacket::server(script.current_version(), listen);
        stream.write_all(&packet.encode().unwrap()).await?;
    }
    let deadline = script.close_after.map(|d| tokio::time::Instant::now() + d);
    let mut reading = script.read;
    let mut cmds_open = true;
    let mut decoder = FrameDecoder::new(frame_config);
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        tokio::select! {
            read = stream.read(&mut buf), if reading => {
                let n = read?;
                if n == 0 {
                    return Ok(());
                }
                decoder.push(&buf[..n]);
                while let Some(frame) = decoder.next_frame().expect("client frames") {
                    let frame = frame.into_owned();
                    if frame.token == well_known::PING_PACKET {
                        records.lock().unwrap()[index].pings += 1;
                        if script.answer_pings {
                            let request: PingRequest =
                                fdb_native_wire::decode(&frame.payload).expect("ping");
                            let payload = fdb_native_wire::encode(&PING_REPLY_OK).unwrap();
                            write_frame(stream, frame_config, request.reply.token(), &payload).await?;
                        }
                        continue;
                    }
                    let replies = script.responder.as_ref().map(|f| f(&frame)).unwrap_or_default();
                    records.lock().unwrap()[index].frames.push(frame);
                    for (token, payload) in replies {
                        write_frame(stream, frame_config, token, &payload).await?;
                    }
                }
            }
            cmd = cmds.recv(), if cmds_open => match cmd {
                None => cmds_open = false,
                Some(ServerCmd::Close) => return Ok(()),
                Some(ServerCmd::Send(token, payload)) => {
                    write_frame(stream, frame_config, token, &payload).await?;
                }
                Some(ServerCmd::StartReading) => reading = true,
            },
            _ = async { tokio::time::sleep_until(deadline.unwrap()).await }, if deadline.is_some() => {
                return Ok(());
            }
        }
    }
}

async fn write_frame<S: tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
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

// ---- helpers -------------------------------------------------------------------------------

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
