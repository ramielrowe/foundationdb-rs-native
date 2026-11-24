//! Reads a pcap (Ethernet, as an AF_PACKET socket captures) of a disposable FDB
//! server's traffic, reassembles every client->server TCP stream to port 4500, splits it into
//! the ConnectPacket and FlowTransport frames, and decodes every frame that is a
//! `GetReadVersionRequest` (7.4 schema). Prints the connect packets and a histogram of the GRV
//! requests' fields.
use std::collections::{BTreeMap, HashMap};

use fdb_native_schema::domain::GetReadVersion;
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::ConnectPacket;
use fdb_native_transport::framing::{Checksum, FrameConfig, parse_frame};

#[derive(Default)]
struct Stream {
    next_seq: Option<u32>,
    pending: BTreeMap<u32, Vec<u8>>,
    buf: Vec<u8>,
    handshaken: bool,
    broken: bool,
    gaps: u64,
}

fn main() {
    let path = std::env::args().nth(1).expect("pcap path");
    let data = std::fs::read(&path).expect("read pcap");
    let mut off = 24; // global header
    let mut streams: HashMap<([u8; 4], u16), Stream> = HashMap::new();
    let mut packets = 0u64;
    let config = FrameConfig::new(Checksum::for_tls(false));
    let mut grv_hist: BTreeMap<String, u64> = BTreeMap::new();
    let mut grv_requests = 0u64;
    let mut grv_tx = 0u64;
    let mut frames = 0u64;
    while off + 16 <= data.len() {
        let incl = u32::from_le_bytes(data[off + 8..off + 12].try_into().unwrap()) as usize;
        let pkt = &data[off + 16..off + 16 + incl];
        off += 16 + incl;
        packets += 1;
        if pkt.len() < 14 + 20 || pkt[12] != 0x08 || pkt[13] != 0x00 {
            continue;
        }
        let ip = &pkt[14..];
        if ip[9] != 6 {
            continue;
        }
        let ihl = ((ip[0] & 0x0f) as usize) * 4;
        let total = u16::from_be_bytes([ip[2], ip[3]]) as usize;
        let src: [u8; 4] = ip[12..16].try_into().unwrap();
        let tcp = &ip[ihl..total.min(ip.len())];
        let sport = u16::from_be_bytes([tcp[0], tcp[1]]);
        let dport = u16::from_be_bytes([tcp[2], tcp[3]]);
        if dport != 4500 {
            continue;
        }
        let seq = u32::from_be_bytes(tcp[4..8].try_into().unwrap());
        let doff = ((tcp[12] >> 4) as usize) * 4;
        let flags = tcp[13];
        let payload = &tcp[doff..];
        let st = streams.entry((src, sport)).or_default();
        if flags & 0x02 != 0 {
            // SYN
            *st = Stream::default();
            st.next_seq = Some(seq.wrapping_add(1));
            continue;
        }
        if payload.is_empty() || st.broken {
            continue;
        }
        let Some(next) = st.next_seq else {
            // Stream started before the capture: skip it.
            st.broken = true;
            continue;
        };
        if seq == next {
            st.buf.extend_from_slice(payload);
            let mut n = next.wrapping_add(payload.len() as u32);
            while let Some(p) = st.pending.remove(&n) {
                n = n.wrapping_add(p.len() as u32);
                st.buf.extend_from_slice(&p);
            }
            st.next_seq = Some(n);
        } else if (seq.wrapping_sub(next) as i32) > 0 {
            st.pending.insert(seq, payload.to_vec());
            if st.pending.len() > 10_000 {
                st.gaps += 1;
                st.broken = true;
            }
            continue;
        } else {
            continue; // retransmission
        }
        // Parse what is complete.
        let mut consumed = 0;
        if !st.handshaken {
            match ConnectPacket::decode(&st.buf) {
                Ok((cp, n)) => {
                    println!(
                        "connect from {}.{}.{}.{}:{}: protocol {:x} connection_id {} canonical_port {}",
                        src[0], src[1], src[2], src[3], sport, cp.protocol_version.0, cp.connection_id, cp.canonical_remote_port
                    );
                    consumed = n;
                    st.handshaken = true;
                }
                Err(_) => continue,
            }
        }
        loop {
            match parse_frame(&config, &st.buf[consumed..]) {
                Ok(Some((frame, n))) => {
                    frames += 1;
                    if let Ok((req, _reply)) = GetReadVersion::decode_request(SchemaVersion::V7_4, frame.payload) {
                        grv_requests += 1;
                        grv_tx += u64::from(req.transaction_count);
                        let key = format!(
                            "flags=0x{:08x} (priority byte {} causal_read_risky={} provisional={} min_known={}) tags={} debug_id={} max_version={} span_sampled={:?}",
                            req.flags,
                            req.flags >> 24,
                            req.flags & 1 != 0,
                            req.flags & 2 != 0,
                            req.flags & 4 != 0,
                            req.tags.len(),
                            req.debug_id.is_some(),
                            req.max_version,
                            req.span_context.flags,
                        );
                        *grv_hist.entry(key).or_default() += 1;
                    }
                    consumed += n;
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("frame error on {:?}:{}: {e}", src, sport);
                    st.broken = true;
                    break;
                }
            }
        }
        st.buf.drain(..consumed);
    }
    let broken = streams.values().filter(|s| s.broken).count();
    println!("packets {packets}, client streams {}, broken/skipped {broken}, frames {frames}", streams.len());
    println!("GetReadVersionRequests {grv_requests}, transactions in them {grv_tx}, tx/request {:.2}", grv_tx as f64 / grv_requests.max(1) as f64);
    for (k, v) in grv_hist {
        println!("  {v:>8}  {k}");
    }
}
