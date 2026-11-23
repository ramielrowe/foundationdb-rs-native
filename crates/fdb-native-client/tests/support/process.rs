//! A scripted fake FDB process on the in-memory network: it speaks the ConnectPacket
//! handshake and frames with the transport's own types, answers pings, and hands every other
//! frame to the handler registered for its token (GRV proxies, commit proxies, storage
//! servers, ...). Frames to tokens without a handler are dropped.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_schema::SchemaVersion;
use fdb_native_transport::connection::{PING_REPLY_OK, PingRequest, well_known};
use fdb_native_transport::{Checksum, ConnectPacket, FrameConfig, FrameDecoder};
use fdb_native_wire::{Endpoint, NetworkAddress, NetworkAddressList, ProtocolVersion, Uid};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::mpsc;

use super::{Net, write_frame};

/// A request frame a process received.
pub struct Incoming {
    /// The token it was sent to.
    pub token: Uid,
    /// The message.
    pub payload: Vec<u8>,
    /// The process's schema set.
    pub schema: SchemaVersion,
}

/// What a handler does with a request.
pub enum Action {
    /// Sends `payload` (an `ErrorOr` reply envelope) to `token` after `delay`.
    Reply {
        token: Uid,
        payload: Vec<u8>,
        delay: Duration,
    },
    /// Nothing (the request stays unanswered).
    Ignore,
    /// Hangs up the connection.
    Close,
}

impl Action {
    pub fn reply(token: Uid, payload: Vec<u8>) -> Action {
        Action::Reply {
            token,
            payload,
            delay: Duration::ZERO,
        }
    }
}

pub type Handler = Arc<dyn Fn(Incoming) -> Action + Send + Sync>;

enum Cmd {
    Send(Uid, Vec<u8>),
    Close,
}

/// A process at one address.
#[derive(Clone)]
pub struct FakeProcess {
    pub addr: SocketAddr,
    net: Arc<Net>,
    handlers: Arc<Mutex<HashMap<Uid, Handler>>>,
    conns: Arc<Mutex<Vec<mpsc::UnboundedSender<Cmd>>>>,
    requests: Arc<AtomicUsize>,
    running: Arc<AtomicBool>,
    version: ProtocolVersion,
}

impl FakeProcess {
    /// Listens at `addr`, speaking `version`. Call it inside the tokio runtime.
    pub fn start(net: &Arc<Net>, addr: SocketAddr, version: ProtocolVersion) -> FakeProcess {
        let process = FakeProcess {
            addr,
            net: net.clone(),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            conns: Arc::new(Mutex::new(Vec::new())),
            requests: Arc::new(AtomicUsize::new(0)),
            running: Arc::new(AtomicBool::new(false)),
            version,
        };
        process.listen();
        process
    }

    fn listen(&self) {
        let (tx, mut rx) = mpsc::unbounded_channel::<DuplexStream>();
        self.net.listeners.lock().unwrap().insert(self.addr, tx);
        self.running.store(true, Ordering::SeqCst);
        let me = self.clone();
        tokio::spawn(async move {
            while let Some(stream) = rx.recv().await {
                let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
                me.conns.lock().unwrap().push(cmd_tx.clone());
                tokio::spawn(me.clone().serve(stream, cmd_tx, cmd_rx));
            }
        });
    }

    /// The process's address, as endpoints carry it.
    pub fn address(&self) -> NetworkAddress {
        NetworkAddress::new(self.addr.ip().into(), self.addr.port(), true, false)
    }

    /// The endpoint `token` of this process.
    pub fn endpoint(&self, token: Uid) -> Endpoint {
        Endpoint::new(
            NetworkAddressList {
                address: self.address(),
                secondary_address: None,
            },
            token,
        )
    }

    /// Handles requests to `token` with `handler`.
    pub fn handle(&self, token: Uid, handler: impl Fn(Incoming) -> Action + Send + Sync + 'static) {
        self.handlers
            .lock()
            .unwrap()
            .insert(token, Arc::new(handler));
    }

    /// How many requests (not pings) reached a handler.
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// Stops listening and hangs up every connection: the process is down.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.net.unlisten(self.addr);
        for conn in self.conns.lock().unwrap().drain(..) {
            let _ = conn.send(Cmd::Close);
        }
    }

    /// Listens again after [`stop`](Self::stop), with the same handlers.
    pub fn restart(&self) {
        self.listen();
    }

    async fn serve(
        self,
        mut stream: DuplexStream,
        cmd_tx: mpsc::UnboundedSender<Cmd>,
        mut cmds: mpsc::UnboundedReceiver<Cmd>,
    ) {
        let mut hello = [0u8; ConnectPacket::SIZE];
        if stream.read_exact(&mut hello).await.is_err() {
            return;
        }
        let (client, _) = ConnectPacket::decode(&hello).expect("client's ConnectPacket");
        let packet = ConnectPacket::server(self.version, self.addr);
        if stream.write_all(&packet.encode().unwrap()).await.is_err() {
            return;
        }
        if !client.protocol_version.is_compatible(self.version) {
            return;
        }
        let schema = SchemaVersion::for_protocol_version(self.version).expect("supported");
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
                            if write_frame(&mut stream, config, ping.reply.token(), &payload)
                                .await
                                .is_err()
                            {
                                return;
                            }
                            continue;
                        }
                        let handler = self.handlers.lock().unwrap().get(&frame.token).cloned();
                        let Some(handler) = handler else { continue };
                        self.requests.fetch_add(1, Ordering::SeqCst);
                        let action = handler(Incoming {
                            token: frame.token,
                            payload: frame.payload.to_vec(),
                            schema,
                        });
                        match action {
                            Action::Ignore => {}
                            Action::Close => return,
                            Action::Reply { token, payload, delay } => {
                                if delay.is_zero() {
                                    if write_frame(&mut stream, config, token, &payload)
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                } else {
                                    let tx = cmd_tx.clone();
                                    tokio::spawn(async move {
                                        tokio::time::sleep(delay).await;
                                        let _ = tx.send(Cmd::Send(token, payload));
                                    });
                                }
                            }
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
}
