//! A scripted fake proxy (GRV or commit) on the in-memory network: it speaks the handshake,
//! answers pings, and hands every other request to the test, which replies to it.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_schema::domain::{Commit, CommitId, GetReadVersion, ReadVersion};
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::connection::{PING_REPLY_OK, PingRequest, well_known};
use fdb_native_transport::{Checksum, ConnectPacket, FrameConfig, FrameDecoder};
use fdb_native_wire::{FdbError, ProtocolVersion, Uid};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::mpsc;

use super::Net;

pub enum Cmd {
    Send(Uid, Vec<u8>),
    Close,
}

/// A request that reached a fake proxy, as raw bytes.
pub struct Incoming {
    pub token: Uid,
    pub payload: Vec<u8>,
    pub schema: SchemaVersion,
    pub conn: mpsc::UnboundedSender<Cmd>,
}

impl Incoming {
    pub fn commit(&self) -> (Commit, Uid) {
        Commit::decode_request(self.schema, &self.payload).expect("a CommitTransactionRequest")
    }

    pub fn grv(&self) -> (GetReadVersion, Uid) {
        GetReadVersion::decode_request(self.schema, &self.payload).expect("a GetReadVersion")
    }

    pub fn reply_commit(&self, reply: Result<CommitId, u16>) {
        let (_, token) = self.commit();
        let reply = reply.map_err(FdbError::new);
        let payload = Commit::encode_reply(self.schema, &reply).unwrap();
        let _ = self.conn.send(Cmd::Send(token, payload));
    }

    pub fn reply_grv(&self, version: i64) {
        self.reply_grv_with(version, false, None);
    }

    /// A read version reply of a database that is `locked` or not, with the value of
    /// `\xff/metadataVersion` at the version.
    pub fn reply_grv_with(&self, version: i64, locked: bool, metadata_version: Option<&[u8]>) {
        let (_, token) = self.grv();
        let rv = ReadVersion {
            process_busy_time: 0,
            version,
            locked,
            metadata_version: metadata_version.map(<[u8]>::to_vec),
            tag_throttle_info: Default::default(),
            mid_shard_size: 0,
            rk_default_throttled: false,
            rk_batch_throttled: false,
            ss_version_vector_delta: Default::default(),
            proxy_id: Uid::new(9, 9),
            proxy_tag_throttled_duration: 0.0,
        };
        let payload = GetReadVersion::encode_reply(self.schema, &Ok(rv)).unwrap();
        let _ = self.conn.send(Cmd::Send(token, payload));
    }

    /// Hangs up the connection the request came on.
    pub fn close(&self) {
        let _ = self.conn.send(Cmd::Close);
    }
}

pub struct FakeProxy {
    pub addr: SocketAddr,
    pub rx: mpsc::UnboundedReceiver<Incoming>,
    /// Hang up on every new connection before the handshake.
    hang_up: Arc<Mutex<bool>>,
}

impl FakeProxy {
    pub fn start(net: &Arc<Net>, addr: SocketAddr, version: ProtocolVersion) -> FakeProxy {
        let mut listener = net.listen(addr);
        let (tx, rx) = mpsc::unbounded_channel();
        let hang_up = Arc::new(Mutex::new(false));
        let hang = hang_up.clone();
        tokio::spawn(async move {
            while let Some(stream) = listener.recv().await {
                if *hang.lock().unwrap() {
                    drop(stream);
                    continue;
                }
                tokio::spawn(serve(stream, addr, version, tx.clone()));
            }
        });
        FakeProxy { addr, rx, hang_up }
    }

    pub fn hang_up(&self, on: bool) {
        *self.hang_up.lock().unwrap() = on;
    }

    pub async fn next(&mut self) -> Incoming {
        tokio::time::timeout(Duration::from_secs(60), self.rx.recv())
            .await
            .unwrap_or_else(|_| panic!("no request reached {} within 60 s", self.addr))
            .expect("proxy running")
    }

    pub fn try_next(&mut self) -> Option<Incoming> {
        self.rx.try_recv().ok()
    }
}

pub async fn serve(
    mut stream: DuplexStream,
    listen: SocketAddr,
    version: ProtocolVersion,
    incoming: mpsc::UnboundedSender<Incoming>,
) {
    let mut hello = [0u8; ConnectPacket::SIZE];
    if stream.read_exact(&mut hello).await.is_err() {
        return;
    }
    let (client, _) = ConnectPacket::decode(&hello).expect("client's ConnectPacket");
    let packet = ConnectPacket::server(version, listen);
    if stream.write_all(&packet.encode().unwrap()).await.is_err() {
        return;
    }
    if !client.protocol_version.is_compatible(version) {
        return;
    }
    let schema = SchemaVersion::for_protocol_version(version).expect("a supported version");
    let config = FrameConfig::new(Checksum::for_tls(false));
    let mut decoder = FrameDecoder::new(config);
    let (cmd_tx, mut cmds) = mpsc::unbounded_channel();
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
                        if write_frame(&mut stream, config, ping.reply.token(), &payload).await.is_err() {
                            return;
                        }
                    } else {
                        let _ = incoming.send(Incoming {
                            token: frame.token,
                            payload: frame.payload.to_vec(),
                            schema,
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

pub async fn write_frame(
    stream: &mut DuplexStream,
    config: FrameConfig,
    token: Uid,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut out = Vec::new();
    config
        .encode_frame(token, payload, &mut out)
        .expect("frame");
    stream.write_all(&out).await
}
