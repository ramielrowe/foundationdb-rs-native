//! A simulated FDB process: it listens on its node, speaks the ConnectPacket handshake and
//! FlowTransport frames with the transport's own types (as the fakes of the client's tests
//! do), answers pings, sends `ENDPOINT_NOT_FOUND` for stream tokens it has no handler for,
//! and hands every other frame to the handler registered for its token. Handlers answer
//! through a [`Reply`], which (like FDB's `ReplyPromise`) answers `broken_promise` if it is
//! dropped unanswered: a role retired by a recovery breaks its pending requests' promises.

use std::collections::BTreeMap;
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::connection::{PING_REPLY_OK, PingRequest, TOKEN_STREAM_FLAG, well_known};
use fdb_native_transport::{Checksum, ConnectPacket, FrameConfig, FrameDecoder};
use fdb_native_wire::{FdbError, ProtocolVersion, Uid};
use futures_channel::mpsc;
use futures_io::AsyncRead;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use futures_lite::stream::StreamExt;

use crate::executor::lock;
use crate::net::KillMode;
use crate::trace::{TraceLevel, hash_bytes};
use crate::{NodeId, Sim, SimStream};

/// `broken_promise`.
pub(crate) const BROKEN_PROMISE: u16 = 1100;

/// Frames to send on one connection.
#[derive(Clone, Debug)]
pub(crate) struct Outbox(mpsc::UnboundedSender<(Uid, Vec<u8>)>);

impl Outbox {
    fn send(&self, token: Uid, payload: Vec<u8>) {
        let _ = self.0.unbounded_send((token, payload));
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// A request frame a process received.
pub(crate) struct Incoming {
    pub(crate) token: Uid,
    pub(crate) payload: Vec<u8>,
    pub(crate) schema: SchemaVersion,
    pub(crate) out: Outbox,
}

impl Incoming {
    /// Decodes the request as `Q`, with a [`Reply`] for it.
    pub(crate) fn decode<Q: Request>(&self) -> Result<(Q, Reply), String> {
        let (request, token) = Q::decode_request(self.schema, &self.payload)
            .map_err(|e| format!("undecodable request on {:?}: {e}", self.token))?;
        Ok((
            request,
            Reply {
                out: self.out.clone(),
                token,
                schema: self.schema,
                error_payload: error_payload::<Q>,
                done: false,
            },
        ))
    }
}

fn error_payload<Q: Request>(schema: SchemaVersion, code: u16) -> Vec<u8> {
    Q::encode_reply(schema, &Err(FdbError::new(code))).expect("an error reply encodes")
}

/// The reply promise of one request. Dropping it unanswered answers `broken_promise`.
pub(crate) struct Reply {
    out: Outbox,
    token: Uid,
    schema: SchemaVersion,
    error_payload: fn(SchemaVersion, u16) -> Vec<u8>,
    done: bool,
}

impl Reply {
    pub(crate) fn schema(&self) -> SchemaVersion {
        self.schema
    }

    /// Answers with an encoded reply envelope.
    pub(crate) fn send(mut self, payload: Vec<u8>) {
        self.done = true;
        self.out.send(self.token, payload);
    }

    /// Answers with an FDB error.
    pub(crate) fn error(mut self, code: u16) {
        self.done = true;
        let payload = (self.error_payload)(self.schema, code);
        self.out.send(self.token, payload);
    }

    /// Never answers (as a process that died would not), without breaking the promise.
    pub(crate) fn forget(mut self) {
        self.done = true;
    }

    /// Whether the connection the request came on is gone.
    pub(crate) fn is_closed(&self) -> bool {
        self.out.is_closed()
    }
}

impl Drop for Reply {
    fn drop(&mut self) {
        if !self.done {
            let payload = (self.error_payload)(self.schema, BROKEN_PROMISE);
            self.out.send(self.token, payload);
        }
    }
}

pub(crate) type Handler = Arc<dyn Fn(Incoming) + Send + Sync>;

/// A process at one address of one node.
pub(crate) struct Process {
    pub(crate) sim: Sim,
    pub(crate) node: NodeId,
    pub(crate) addr: SocketAddr,
    version: ProtocolVersion,
    handlers: Mutex<BTreeMap<Uid, Handler>>,
    /// Requests that reached a handler.
    pub(crate) requests: AtomicU64,
    alive: AtomicBool,
    /// Protocol breaches seen by the server side (undecodable frames and the like).
    violations: Arc<Mutex<Vec<String>>>,
}

impl Process {
    pub(crate) fn new(
        sim: &Sim,
        node: NodeId,
        addr: SocketAddr,
        version: ProtocolVersion,
        violations: Arc<Mutex<Vec<String>>>,
    ) -> Arc<Process> {
        Arc::new(Process {
            sim: sim.clone(),
            node,
            addr,
            version,
            handlers: Mutex::new(BTreeMap::new()),
            requests: AtomicU64::new(0),
            alive: AtomicBool::new(false),
            violations,
        })
    }

    pub(crate) fn name(&self) -> String {
        self.sim.node_name(self.node)
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    /// Listens and serves connections.
    pub(crate) fn start(self: &Arc<Self>) {
        self.alive.store(true, Ordering::SeqCst);
        self.sim.power_on(self.node);
        let listener = self.sim.listen(self.node, self.addr);
        let me = self.clone();
        self.sim.spawn_on(self.node, async move {
            while let Some(stream) = listener.accept().await {
                let conn = me.clone();
                me.sim
                    .spawn_on(me.node, async move { conn.serve(stream).await });
            }
        });
    }

    /// Dies (its handlers are forgotten; its roles must be registered again after a
    /// [`start`](Self::start)).
    pub(crate) fn kill(&self, mode: KillMode) {
        self.alive.store(false, Ordering::SeqCst);
        // Dropped outside the lock: a handler may own replies.
        let handlers = std::mem::take(&mut *lock(&self.handlers));
        self.sim.kill_node(self.node, mode);
        drop(handlers);
    }

    pub(crate) fn handle(&self, token: Uid, handler: Handler) {
        lock(&self.handlers).insert(token, handler);
    }

    pub(crate) fn unhandle(&self, token: Uid) {
        let handler = lock(&self.handlers).remove(&token);
        drop(handler);
    }

    fn violation(&self, what: String) {
        self.sim
            .trace(TraceLevel::Events, "server-violation", &[], || what.clone());
        lock(&self.violations).push(format!("{}: {what}", self.name()));
    }

    async fn serve(self: Arc<Self>, mut stream: SimStream) {
        let mut hello = [0u8; ConnectPacket::SIZE];
        if stream.read_exact(&mut hello).await.is_err() {
            return;
        }
        let client = match ConnectPacket::decode(&hello) {
            Ok((client, _)) => client,
            Err(e) => {
                self.violation(format!("undecodable ConnectPacket: {e}"));
                return;
            }
        };
        let packet = ConnectPacket::server(self.version, self.addr);
        if stream
            .write_all(&packet.encode().expect("a ConnectPacket encodes"))
            .await
            .is_err()
        {
            return;
        }
        if !client.protocol_version.is_compatible(self.version) {
            // As fdbserver does: say who we are, then hang up.
            return;
        }
        let schema = SchemaVersion::for_protocol_version(self.version).expect("supported");
        let config = FrameConfig::new(Checksum::for_tls(false));
        let mut decoder = FrameDecoder::new(config);
        let (tx, mut rx) = mpsc::unbounded::<(Uid, Vec<u8>)>();
        let outbox = Outbox(tx);
        let mut buf = vec![0u8; 16 * 1024];
        enum Event {
            Read(io::Result<usize>),
            Send(Option<(Uid, Vec<u8>)>),
        }
        loop {
            let event = poll_fn(|cx| {
                if let Poll::Ready(frame) = rx.poll_next(cx) {
                    return Poll::Ready(Event::Send(frame));
                }
                match Pin::new(&mut stream).poll_read(cx, &mut buf) {
                    Poll::Ready(r) => Poll::Ready(Event::Read(r)),
                    Poll::Pending => Poll::Pending,
                }
            })
            .await;
            match event {
                Event::Send(None) => return,
                Event::Send(Some((token, payload))) => {
                    if write_frame(&mut stream, config, token, &payload)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Event::Read(Err(_)) | Event::Read(Ok(0)) => return,
                Event::Read(Ok(n)) => {
                    decoder.push(&buf[..n]);
                    loop {
                        let frame = match decoder.next_frame() {
                            Ok(Some(frame)) => frame.into_owned(),
                            Ok(None) => break,
                            Err(e) => {
                                self.violation(format!("bad frame from the client: {e}"));
                                return;
                            }
                        };
                        let reply =
                            self.dispatch(frame.token, frame.payload.to_vec(), schema, &outbox);
                        if let Some((token, payload)) = reply {
                            if write_frame(&mut stream, config, token, &payload)
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Handles one frame; returns a frame to send at once (a ping's reply, an
    /// `ENDPOINT_NOT_FOUND`).
    fn dispatch(
        &self,
        token: Uid,
        payload: Vec<u8>,
        schema: SchemaVersion,
        outbox: &Outbox,
    ) -> Option<(Uid, Vec<u8>)> {
        if token == well_known::PING_PACKET {
            return match fdb_native_wire::decode::<PingRequest>(&payload) {
                Ok(ping) => Some((
                    ping.reply.token(),
                    fdb_native_wire::encode(&PING_REPLY_OK).expect("a ping reply encodes"),
                )),
                Err(e) => {
                    self.violation(format!("undecodable ping: {e}"));
                    None
                }
            };
        }
        let handler = lock(&self.handlers).get(&token).cloned();
        let node = u64::from(self.node.0);
        let Some(handler) = handler else {
            self.sim.trace(
                TraceLevel::Events,
                "no-endpoint",
                &[node, token.first, token.second],
                || format!("{}: no handler for {token:?}", self.name()),
            );
            let stream = token.first & TOKEN_STREAM_FLAG != 0 && !well_known::is_well_known(token);
            return stream.then(|| {
                (
                    well_known::ENDPOINT_NOT_FOUND,
                    fdb_native_wire::encode(&token).expect("a token encodes"),
                )
            });
        };
        self.requests.fetch_add(1, Ordering::SeqCst);
        let hash = hash_bytes(&payload);
        self.sim.trace(
            TraceLevel::Verbose,
            "request",
            &[node, token.first, token.second, hash],
            || format!("{} <- {token:?} ({} bytes)", self.name(), payload.len()),
        );
        handler(Incoming {
            token,
            payload,
            schema,
            out: outbox.clone(),
        });
        None
    }
}

async fn write_frame(
    stream: &mut SimStream,
    config: FrameConfig,
    token: Uid,
    payload: &[u8],
) -> io::Result<()> {
    let mut out = Vec::new();
    config
        .encode_frame(token, payload, &mut out)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    stream.write_all(&out).await
}
