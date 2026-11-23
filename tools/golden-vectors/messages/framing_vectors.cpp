// Golden vectors for FlowTransport's packet framing: the bytes between the ConnectPacket and the
// end of a connection, [u32 length][u64 XXH3-64 checksum, non-TLS only][16-byte token][payload].
// Built inside one release branch's CMake build (targets.cmake) and run by ../regen_messages.sh.
// See ../README.md ("Framing vectors").
//
// FlowTransport's sendPacket() is a static function of a .cpp file and needs a whole transport, so
// it cannot be called. The generator instead runs the framing core of sendPacket() verbatim
// (messages/check_mirrors.py compares the two regions marked MIRROR below with the branch's source,
// line by line and without comments) over the real PacketBuffer / PacketWriter / SplitBuffer types
// and flow's own xxhash.h, so the multi-buffer behaviour (a header, a token or a payload that
// straddles a PacketBuffer boundary; the XXH3 streaming API versus the one-shot call) is exercised
// for real. Every frame is then read back the way scanPackets() reads it (length, checksum,
// ArenaReader >> UID) and compared with what was put in, and every negative vector must be refused
// by the same checks.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "json.h"

#include "flow/flow.h"
#include "flow/Knobs.h"
#include "flow/network.h"
#include "flow/serialize.h"
#include "flow/Net2Packet.h"
#define XXH_INLINE_ALL
#include "flow/xxhash.h"
#if __has_include("fdbclient/NativeAPI.h")
#include "fdbclient/NativeAPI.h"
#else
#include "fdbclient/NativeAPI.actor.h"
#endif

using namespace std::string_literals;

namespace {

constexpr int kFormatVersion = 1;
constexpr int PACKET_LEN_WIDTH = sizeof(uint32_t); // FlowTransport: PACKET_LEN_WIDTH

// ---------------------------------------------------------------------------------------
// Bytes and JSON.

std::string hex(const uint8_t* p, size_t n) {
	static const char* d = "0123456789abcdef";
	std::string s;
	s.reserve(2 * n);
	for (size_t i = 0; i < n; ++i) {
		s += d[p[i] >> 4];
		s += d[p[i] & 15];
	}
	return s;
}
std::string hex(const std::string& s) {
	return hex(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

// ---------------------------------------------------------------------------------------
// Payload sources: what `what.serializePacketWriter(wr)` writes after the token.

// A payload of raw bytes (byte i is i * mul + add). `chunked` writes it with serializeBytes() in
// pieces, as a payload built from many small items is written, so that it may straddle PacketBuffer
// boundaries; otherwise it is allocated in one piece with writeBytes(), which is how
// MakeSerializeSource::serializePacketWriter() hands the whole flatbuffers message to the
// ObjectWriter (PacketWriter::packetWriterAlloc), so it always lies in one PacketBuffer.
struct RawSource : ISerializeSource {
	size_t n;
	uint8_t mul, add;
	bool chunked;
	size_t chunk;

	RawSource(size_t n, uint8_t mul, uint8_t add, bool chunked, size_t chunk = 1000)
	  : n(n), mul(mul), add(add), chunked(chunked), chunk(chunk) {}

	std::string bytes() const {
		std::string b(n, '\0');
		for (size_t i = 0; i < n; ++i) {
			b[i] = static_cast<char>(static_cast<uint8_t>(i * mul + add));
		}
		return b;
	}

	void serializePacketWriter(PacketWriter& w) const override {
		std::string b = bytes();
		if (n == 0) {
			return;
		}
		if (!chunked) {
			memcpy(w.writeBytes(n), b.data(), n);
		} else {
			for (size_t off = 0; off < n; off += chunk) {
				w.serializeBytes(b.data() + off, static_cast<int>(std::min(chunk, n - off)));
			}
		}
	}
	void serializeObjectWriter(ObjectWriter&) const override { throw internal_error(); }
};

// ---------------------------------------------------------------------------------------
// The write path.

// What the frame writer needs from `Endpoint destination`.
struct Destination {
	UID token;
};

// A chain of PacketBuffers like Peer::unsent: frames are appended at the tail.
struct Chain {
	PacketBuffer* first;
	PacketBuffer* tail;
	int startOffset; // bytes of the first buffer that precede the first frame (not part of the output)
	Chain(int pad) {
		first = tail = PacketBuffer::create();
		first->bytes_written = pad; // the bytes of earlier packets; their content is never read
		startOffset = pad;
	}
	Chain(const Chain&) = delete;
	~Chain() {
		for (PacketBuffer* b = first; b;) {
			PacketBuffer* next = b->nextPacketBuffer();
			b->delref();
			b = next;
		}
	}
	size_t capacity() const { return first->size(); }
	// The bytes written to the chain after startOffset, and the size of the piece in each buffer.
	std::string bytes(std::vector<int>* segments = nullptr) const {
		std::string out;
		int skip = startOffset;
		for (PacketBuffer* b = first; b; b = b->nextPacketBuffer()) {
			int n = b->bytes_written - skip;
			out.append(reinterpret_cast<const char*>(b->data()) + skip, n);
			if (segments) {
				segments->push_back(n);
			}
			skip = 0;
		}
		return out;
	}
};

// Appends one frame the way sendPacket() does and returns the number of bytes it added.
// `len` is what sendPacket() calls len: the token and the payload.
size_t sendFrame(Chain& chain, const UID& token, const ISerializeSource& what, bool checksumEnabled) {
	// sendPacket(): `PacketBuffer* pb = peer->unsent.getWriteBuffer(); ReliablePacket* rp = reliable ? ... : 0;`
	PacketBuffer* pb = chain.tail;
	ReliablePacket* rp = nullptr;
	const Destination destination{ token };
	int prevBytesWritten = pb->bytes_written;
	PacketBuffer* checksumPb = pb;
	size_t before = 0;
	for (PacketBuffer* b = chain.first; b; b = b->nextPacketBuffer()) {
		before += b->bytes_written;
	}

	// BEGIN MIRROR A (sendPacket: serialize the packet)
	PacketWriter wr(pb,
	                rp,
	                AssumeVersion(g_network->protocolVersion())); // SOMEDAY: Can we downgrade to talk to older peers?

	// Reserve some space for packet length and checksum, write them after serializing data
	SplitBuffer packetInfoBuffer;
	uint32_t len;

	// This is technically abstraction breaking but avoids XXH3_createState() and XXH3_freeState() which are just
	// malloc/free
	XXH3_state_t checksumState;
	// Checksum will be calculated with buffer API if contiguous, else using stream API.  Mode is tracked here.
	bool checksumStream = false;
	XXH64_hash_t checksum;

	int packetInfoSize = PACKET_LEN_WIDTH;
	if (checksumEnabled) {
		packetInfoSize += sizeof(checksum);
	}

	wr.writeAhead(packetInfoSize, &packetInfoBuffer);
	wr << destination.token;
	what.serializePacketWriter(wr);
	pb = wr.finish();
	// END MIRROR A
	// release-7.4: `len = wr.size() - packetInfoSize;`
	// release-8.0: the same, after rejecting a payload above maxPacketPayloadSize() (not reachable here).
	len = wr.size() - packetInfoSize;

	// BEGIN MIRROR B (sendPacket: checksum, then write the length and the checksum)
	if (checksumEnabled) {
		// Find the correct place to start calculating checksum
		uint32_t checksumUnprocessedLength = len;
		prevBytesWritten += packetInfoSize;
		if (prevBytesWritten >= checksumPb->bytes_written) {
			prevBytesWritten -= checksumPb->bytes_written;
			checksumPb = checksumPb->nextPacketBuffer();
		}

		// Checksum calculation
		while (checksumUnprocessedLength > 0) {
			uint32_t processLength =
			    std::min(checksumUnprocessedLength, (uint32_t)(checksumPb->bytes_written - prevBytesWritten));

			// If not in checksum stream mode yet
			if (!checksumStream) {
				// If there is nothing left to process then calculate checksum directly
				if (processLength == checksumUnprocessedLength) {
					checksum = XXH3_64bits(checksumPb->data() + prevBytesWritten, processLength);
				} else {
					// Otherwise, initialize checksum state and switch to stream mode
					if (XXH3_64bits_reset(&checksumState) != XXH_OK) {
						throw internal_error();
					}
					checksumStream = true;
				}
			}

			// If in checksum stream mode, update the checksum state
			if (checksumStream) {
				if (XXH3_64bits_update(&checksumState, checksumPb->data() + prevBytesWritten, processLength) !=
				    XXH_OK) {
					throw internal_error();
				}
			}

			checksumUnprocessedLength -= processLength;
			checksumPb = checksumPb->nextPacketBuffer();
			prevBytesWritten = 0;
		}

		// If in checksum stream mode, get the final checksum
		if (checksumStream) {
			checksum = XXH3_64bits_digest(&checksumState);
		}
	}

	// Write packet length and checksum into packet buffer
	packetInfoBuffer.write(&len, sizeof(len));
	if (checksumEnabled) {
		packetInfoBuffer.write(&checksum, sizeof(checksum), sizeof(len));
	}
	// END MIRROR B

	// sendPacket(): `peer->send(pb, rp, firstUnsent)`, which makes pb the new write buffer.
	chain.tail = pb;
	size_t after = 0;
	for (PacketBuffer* b = chain.first; b; b = b->nextPacketBuffer()) {
		after += b->bytes_written;
	}
	return after - before;
}

// ---------------------------------------------------------------------------------------
// The read path: what scanPackets() checks, on one complete or incomplete buffer.

enum class Scan { Frame, NeedMore, Oversize, TooShort, ChecksumMismatch };

struct Scanned {
	Scan result = Scan::NeedMore;
	uint32_t len = 0;
	bool hasChecksum = false;
	uint64_t storedChecksum = 0;
	uint64_t computedChecksum = 0;
	UID token;
	std::string payload;
	size_t consumed = 0;
};

// maxPacketPayloadSize(): release-8.0 clamps the knob so that a frame fits in an int; release-7.4
// uses the knob as it is (the clamp is irrelevant at the default).
int64_t packetLimit(bool checksumEnabled) {
	int64_t framing = 2 * PACKET_LEN_WIDTH + (checksumEnabled ? static_cast<int64_t>(sizeof(XXH64_hash_t)) : 0);
	return std::min<int64_t>(FLOW_KNOBS->PACKET_LIMIT, std::numeric_limits<int>::max() - framing);
}

Scanned scanOne(const std::string& bytes, bool checksumEnabled) {
	Scanned s;
	const uint8_t* p = reinterpret_cast<const uint8_t*>(bytes.data());
	const uint8_t* e = p + bytes.size();
	const uint8_t* start = p;
	if (e - p < PACKET_LEN_WIDTH) {
		return s;
	}
	memcpy(&s.len, p, sizeof(s.len));
	p += PACKET_LEN_WIDTH;
	if (checksumEnabled) {
		if (e - p < static_cast<ptrdiff_t>(sizeof(uint64_t))) {
			return s;
		}
		memcpy(&s.storedChecksum, p, sizeof(uint64_t));
		s.hasChecksum = true;
		p += sizeof(uint64_t);
	}
	if (s.len > packetLimit(checksumEnabled)) {
		s.result = Scan::Oversize;
		return s;
	}
	if (e - p < static_cast<ptrdiff_t>(s.len)) {
		return s;
	}
	if (s.len < sizeof(UID)) {
		s.result = Scan::TooShort;
		return s;
	}
	if (checksumEnabled) {
		s.computedChecksum = XXH3_64bits(p, s.len);
		if (s.computedChecksum != s.storedChecksum) {
			s.result = Scan::ChecksumMismatch;
			return s;
		}
	}
	// remove object serializer flag to account for flat buffer
	ProtocolVersion peerProtocolVersion = g_network->protocolVersion();
	peerProtocolVersion.removeObjectSerializerFlag();
	Arena arena;
	ArenaReader reader(arena, StringRef(p, s.len), AssumeVersion(peerProtocolVersion));
	reader >> s.token;
	StringRef rest = reader.arenaReadAll();
	s.payload.assign(reinterpret_cast<const char*>(rest.begin()), rest.size());
	s.consumed = (p - start) + s.len;
	s.result = Scan::Frame;
	return s;
}

// ---------------------------------------------------------------------------------------
// Vectors.

struct Record {
	std::string name;
	Json json;
};
std::vector<Record>& positives() {
	static std::vector<Record> v;
	return v;
}
std::vector<Record>& negatives() {
	static std::vector<Record> v;
	return v;
}

// One frame of a positive vector, as the generator wrote it.
struct FrameSpec {
	UID token;
	std::string payload; // the bytes the source wrote
	std::string payloadHex; // for small payloads
	// Set when the payload is RawSource's pattern.
	bool pattern = false;
	uint8_t mul = 0, add = 0;
	const ISerializeSource* source = nullptr;
};

Json tokenJson(const UID& t) {
	return Json::object().set("first", Json::uinteger(t.first())).set("second", Json::uinteger(t.second()));
}

[[noreturn]] void fail(const std::string& what) {
	throw std::runtime_error(what);
}

// Writes `frames` (with their sources) into one chain starting `pad` bytes into its first buffer,
// reads the result back with scanOne() and records it.
void addVector(const std::string& name,
               const std::string& group,
               const std::string& description,
               bool checksumEnabled,
               int pad,
               std::vector<FrameSpec> frames) {
	Chain chain(pad);
	std::vector<size_t> sizes;
	for (auto& f : frames) {
		sizes.push_back(sendFrame(chain, f.token, *f.source, checksumEnabled));
	}
	std::vector<int> segments;
	std::string bytes = chain.bytes(&segments);

	// Read it back the way connectionReader/scanPackets does.
	Json framesJson = Json::array();
	size_t off = 0;
	for (size_t i = 0; i < frames.size(); ++i) {
		Scanned s = scanOne(bytes.substr(off), checksumEnabled);
		if (s.result != Scan::Frame || s.consumed != sizes[i] || !(s.token == frames[i].token) ||
		    s.payload != frames[i].payload || s.len != 16 + frames[i].payload.size()) {
			fail("vector " + name + ": frame " + std::to_string(i) + " does not read back");
		}
		Json fj = Json::object();
		fj.set("offset", Json::uinteger(off));
		fj.set("length", Json::uinteger(s.len));
		fj.set("checksum", s.hasChecksum ? Json::uinteger(s.storedChecksum) : Json::null());
		fj.set("token", tokenJson(s.token));
		fj.set("payload_len", Json::uinteger(s.payload.size()));
		if (frames[i].pattern) {
			fj.set("payload_pattern",
			       Json::object()
			           .set("mul", Json::uinteger(frames[i].mul))
			           .set("add", Json::uinteger(frames[i].add)));
		} else {
			fj.set("payload_hex", Json::string(hex(s.payload)));
		}
		framesJson.push(fj);
		off += s.consumed;
	}
	if (off != bytes.size()) {
		fail("vector " + name + ": trailing bytes");
	}
	size_t total = 0;
	for (int s : segments) {
		total += s;
	}
	if (total != bytes.size()) {
		fail("vector " + name + ": segments do not add up");
	}
	Json segs = Json::array();
	for (int s : segments) {
		segs.push(Json::uinteger(s));
	}
	Json j = Json::object();
	j.set("format_version", Json::integer(kFormatVersion));
	j.set("name", Json::string(name));
	j.set("group", Json::string(group));
	j.set("kind", Json::string("positive"));
	j.set("description", Json::string(description));
	j.set("checksum_enabled", Json::boolean(checksumEnabled));
	j.set("start_offset", Json::integer(pad));
	j.set("segments", segs);
	j.set("size", Json::uinteger(bytes.size()));
	j.set("bytes", Json::string(hex(bytes)));
	j.set("frames", framesJson);
	positives().push_back({ name, j });
}

// Convenience: a frame holding a RawSource.
struct RawFrame {
	RawSource source;
	FrameSpec spec;
	RawFrame(UID token, size_t n, uint8_t mul, uint8_t add, bool chunked, size_t chunk = 1000)
	  : source(n, mul, add, chunked, chunk) {
		spec.token = token;
		spec.payload = source.bytes();
		spec.pattern = true;
		spec.mul = mul;
		spec.add = add;
		spec.source = &source;
	}
	RawFrame(const RawFrame&) = delete;
};

const UID kToken(0x0123456789abcdefULL, 0xfedcba9876543210ULL);
const UID kWellKnown(std::numeric_limits<uint64_t>::max(), 1); // Endpoint::wellKnownToken(WLTOKEN_PING_PACKET)

// Both checksum modes of one case. `make` builds the frames (called once per mode).
template <class F>
void bothModes(const std::string& name, const std::string& group, const std::string& description, int pad, F&& make) {
	for (bool checksum : { true, false }) {
		make(name + (checksum ? "_checksum" : "_nochecksum"), group, description, checksum, pad);
	}
}

void rawVectors() {
	auto one = [&](const std::string& name, const std::string& description, UID token, size_t n, bool chunked) {
		bothModes(name, "raw", description, 0, [&](auto&& nm, auto&& g, auto&& d, bool checksum, int pad) {
			RawFrame f(token, n, 31, 7, chunked);
			addVector(nm, g, d, checksum, pad, { f.spec });
		});
	};
	one("raw_empty", "A frame whose payload is empty (length 16: just the token).", kToken, 0, false);
	one("raw_1_byte", "A one-byte payload.", kToken, 1, false);
	one("raw_100_bytes", "A 100-byte payload.", kToken, 100, false);
	one("raw_well_known_token",
	    "The well-known ping token (-1, 1): the first half of the UID is the first 8 bytes on the wire.",
	    kWellKnown,
	    64,
	    false);
	one("raw_large_contiguous",
	    "A 40000-byte payload allocated in one piece (as an ObjectWriter message is), which does not fit in a "
	    "16 KiB PacketBuffer and so lies in a buffer of its own after the token.",
	    kToken,
	    40000,
	    false);
	one("raw_large_chunked",
	    "A 40000-byte payload written in 1000-byte pieces, which straddles three PacketBuffers: the checksum is "
	    "computed with XXH3's streaming API.",
	    kToken,
	    40000,
	    true);
}

void flatbufferVectors() {
	auto add = [&](const std::string& name, const std::string& description, const UID& token, auto&& source,
	               const std::string& payload) {
		for (bool checksum : { true, false }) {
			FrameSpec spec;
			spec.token = token;
			spec.payload = payload;
			spec.source = &source;
			addVector(name + (checksum ? "_checksum" : "_nochecksum"), "flatbuffer", description, checksum, 0, { spec });
		}
	};
	auto payloadOf = [&](const ISerializeSource& source) {
		// The payload is what the source writes after a token: read it back from a frame.
		Chain chain(0);
		sendFrame(chain, UID(), source, false);
		std::string frame = chain.bytes();
		return frame.substr(PACKET_LEN_WIDTH + sizeof(UID));
	};
	StringRef ok = "ok"_sr;
	SerializeSource<StringRef> okSource(ok);
	add("flatbuffer_string_ok",
	    "A real flatbuffers message (ObjectWriter through PacketWriter): the StringRef \"ok\".",
	    kToken,
	    okSource,
	    payloadOf(okSource));
	UID id(1, 2);
	SerializeSource<UID> idSource(id);
	add("flatbuffer_uid",
	    "A real flatbuffers message: the UID (1, 2).",
	    kWellKnown,
	    idSource,
	    payloadOf(idSource));
	std::string longString(300, 'x');
	for (size_t i = 0; i < longString.size(); ++i) {
		longString[i] = static_cast<char>('a' + i % 26);
	}
	StringRef longRef(reinterpret_cast<const uint8_t*>(longString.data()), longString.size());
	SerializeSource<StringRef> longSource(longRef);
	add("flatbuffer_string_300",
	    "A real flatbuffers message: a 300-byte StringRef.",
	    kToken,
	    longSource,
	    payloadOf(longSource));
}

// A frame near the end of a PacketBuffer. cap is the capacity of a buffer; `delta` is how far the
// whole frame (header, token and payload) reaches past the end of the first buffer.
void boundaryVectors(size_t cap) {
	for (bool chunked : { false, true }) {
		for (int delta : { -1, 0, 1 }) {
			if (chunked && delta == -1) {
				continue; // the contiguous pair of this one is enough
			}
			for (bool checksum : { true, false }) {
				size_t header = checksum ? 12 : 4;
				size_t payload = cap - header - sizeof(UID) + delta;
				RawFrame f(kToken, payload, 29, 3, chunked, 4000);
				std::string name = std::string("boundary_") + (chunked ? "chunked" : "contiguous") + "_" +
				                   (delta < 0 ? "minus1" : delta == 0 ? "exact" : "plus1") +
				                   (checksum ? "_checksum" : "_nochecksum");
				addVector(name,
				          "boundary",
				          "A frame that ends " +
				              (delta == 0 ? std::string("exactly at")
				                          : std::to_string(std::abs(delta)) + " byte " + (delta < 0 ? "before" : "after")) +
				              " the end of the first " + std::to_string(cap) + "-byte PacketBuffer; the payload is " +
				              (chunked ? "written in pieces." : "allocated in one piece (moved to a new buffer when it does not fit)."),
				          checksum,
				          0,
				          { f.spec });
			}
		}
	}
	// The header (length, checksum) and the token straddle a buffer boundary: the first frame starts
	// k bytes before the end of the buffer.
	for (bool chunked : { false, true }) {
		for (int k : { 1, 2, 3, 4, 5, 8, 11, 12, 13, 20, 28, 29 }) {
			for (bool checksum : { true, false }) {
				RawFrame f(kToken, 100, 17, 5, chunked, 7);
				std::string name = std::string("straddle_") + (chunked ? "chunked" : "contiguous") + "_k" +
				                   std::to_string(k) + (checksum ? "_checksum" : "_nochecksum");
				addVector(name,
				          "boundary",
				          "A 100-byte frame that starts " + std::to_string(k) + " bytes before the end of a " +
				              std::to_string(cap) + "-byte PacketBuffer (the output starts at the frame). Payload " +
				              (chunked ? "written in 7-byte pieces." : "allocated in one piece."),
				          checksum,
				          static_cast<int>(cap) - k,
				          { f.spec });
			}
		}
	}
}

void streamVectors() {
	for (bool checksum : { true, false }) {
		RawFrame a(kWellKnown, 40, 3, 1, false);
		RawFrame b(UID(5, 6), 0, 0, 0, false);
		RawFrame c(kToken, 9000, 7, 2, true, 2500);
		RawFrame d(UID(0, 0), 1, 1, 1, false);
		RawFrame e(UID(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max()), 16000, 13, 11, true, 3000);
		RawFrame f(kToken, 17, 5, 9, false);
		addVector(std::string("stream_mixed") + (checksum ? "_checksum" : "_nochecksum"),
		          "stream",
		          "Six frames written back to back into one buffer chain, as a peer's unsent queue holds them: an "
		          "empty payload, a zero token, a frame that straddles a buffer boundary, and a frame that ends in "
		          "the next buffer.",
		          checksum,
		          0,
		          { a.spec, b.spec, c.spec, d.spec, e.spec, f.spec });
	}
}

// ---------------------------------------------------------------------------------------
// Negative vectors: bytes the receiver refuses.

void addNegative(const std::string& name,
                 const std::string& description,
                 bool checksumEnabled,
                 const std::string& bytes,
                 Scan expect,
                 const char* error) {
	Scanned s = scanOne(bytes, checksumEnabled);
	if (s.result != expect) {
		fail("negative vector " + name + " is not refused as expected");
	}
	Json j = Json::object();
	j.set("format_version", Json::integer(kFormatVersion));
	j.set("name", Json::string(name));
	j.set("group", Json::string("negative"));
	j.set("kind", Json::string("negative"));
	j.set("description", Json::string(description));
	j.set("checksum_enabled", Json::boolean(checksumEnabled));
	j.set("size", Json::uinteger(bytes.size()));
	j.set("bytes", Json::string(hex(bytes)));
	j.set("length", Json::uinteger(s.len));
	j.set("error", Json::string(error));
	j.set("packet_limit", Json::integer(packetLimit(checksumEnabled)));
	negatives().push_back({ name, j });
}

std::string frameBytes(const UID& token, size_t n, bool checksum) {
	RawFrame f(token, n, 31, 7, false);
	Chain chain(0);
	sendFrame(chain, token, f.source, checksum);
	return chain.bytes();
}

std::string u32le(uint32_t v) {
	std::string s(4, '\0');
	memcpy(&s[0], &v, 4);
	return s;
}
std::string u64le(uint64_t v) {
	std::string s(8, '\0');
	memcpy(&s[0], &v, 8);
	return s;
}

void negativeVectors() {
	const size_t headerLen = 12;
	{
		std::string b = frameBytes(kToken, 64, true);
		std::string x = b;
		x[headerLen + 16 + 10] ^= 0x04;
		addNegative("checksum_payload_bit_flipped", "A valid 64-byte frame with one payload bit flipped.", true, x,
		            Scan::ChecksumMismatch, "checksum_mismatch");
		x = b;
		x[headerLen + 3] ^= 0x80;
		addNegative("checksum_token_bit_flipped", "A valid frame with one token bit flipped.", true, x,
		            Scan::ChecksumMismatch, "checksum_mismatch");
		x = b;
		x[4] ^= 0x01;
		addNegative("checksum_stored_bit_flipped", "A valid frame with one bit of the stored checksum flipped.", true,
		            x, Scan::ChecksumMismatch, "checksum_mismatch");
		x = b;
		x.replace(4, 8, u64le(0));
		addNegative("checksum_zero", "A valid frame whose checksum field is zero.", true, x,
		            Scan::ChecksumMismatch, "checksum_mismatch");
	}
	// A length too small to hold the token, with the correct checksum for those bytes: only the
	// length is wrong.
	for (uint32_t len : { 0u, 1u, 15u }) {
		std::string body(len, '\x5a');
		for (bool checksum : { true, false }) {
			std::string b = u32le(len);
			if (checksum) {
				b += u64le(XXH3_64bits(body.data(), body.size()));
			}
			b += body;
			addNegative("length_" + std::to_string(len) + (checksum ? "_checksum" : "_nochecksum"),
			            "A frame whose length (" + std::to_string(len) + ") is smaller than a token (16).",
			            checksum, b, Scan::TooShort, "too_short");
		}
	}
	// A length above the limit: refused as soon as the header is read, without the body.
	for (bool checksum : { true, false }) {
		int64_t limit = packetLimit(checksum);
		for (int64_t len : { limit + 1, static_cast<int64_t>(0xffffffffu) }) {
			std::string b = u32le(static_cast<uint32_t>(len));
			if (checksum) {
				b += u64le(0x1122334455667788ULL);
			}
			b += std::string(32, '\x11');
			addNegative("length_" + std::to_string(len) + (checksum ? "_checksum" : "_nochecksum"),
			            "A frame whose length (" + std::to_string(len) + ") is above PACKET_LIMIT (" +
			                std::to_string(limit) + "); the body is not read.",
			            checksum, b, Scan::Oversize, "oversize");
		}
	}
}

// ---------------------------------------------------------------------------------------
// Output.

void writeFile(const std::string& path, const std::string& content) {
	std::ofstream f(path, std::ios::binary);
	if (!f) {
		throw std::runtime_error("cannot write " + path);
	}
	f << content;
}

int generate(const std::string& out, const std::string& fdbRev) {
	size_t cap;
	{
		Chain probe(0);
		cap = probe.capacity();
	}
	rawVectors();
	flatbufferVectors();
	boundaryVectors(cap);
	streamVectors();
	negativeVectors();

	const std::string dir = out + "/framing-" GV_RELEASE_NAME;
	std::filesystem::create_directories(dir + "/negative");
	Json index = Json::object();
	index.set("format_version", Json::integer(kFormatVersion));
	index.set("generator", Json::string("tools/golden-vectors"));
	index.set("fdb_source", Json::string(fdbRev));
	index.set("fdb_version", Json::string(GV_FDB_VERSION));
	const char* image = getenv("GV_BUILD_IMAGE");
	index.set("build_image", Json::string(image ? image : "unknown"));
	index.set("packet_buffer_capacity", Json::uinteger(cap));
	index.set("packet_limit", Json::integer(FLOW_KNOBS->PACKET_LIMIT));
	index.set("packet_warning", Json::integer(FLOW_KNOBS->PACKET_WARNING));
	index.set("min_packet_buffer_bytes", Json::integer(FLOW_KNOBS->MIN_PACKET_BUFFER_BYTES));
	index.set("note",
	          Json::string("Generated by tools/golden-vectors/regen_messages.sh. Every frame was written by the framing "
	                       "core of FlowTransport's sendPacket() (mirrored verbatim, see messages/framing_vectors.cpp) "
	                       "over this release's PacketBuffer/PacketWriter/SplitBuffer and xxhash.h, and read back with "
	                       "scanPackets()'s checks. Integers are exact: parse 64-bit values as u64. Do not edit."));
	Json pos = Json::array();
	for (const auto& r : positives()) {
		writeFile(dir + "/" + r.name + ".json", r.json.dump() + "\n");
		pos.push(Json::string(r.name + ".json"));
	}
	Json neg = Json::array();
	for (const auto& r : negatives()) {
		writeFile(dir + "/negative/" + r.name + ".json", r.json.dump() + "\n");
		neg.push(Json::string("negative/" + r.name + ".json"));
	}
	index.set("positive", pos);
	index.set("negative", neg);
	writeFile(dir + "/index.json", index.dump() + "\n");
	fprintf(stderr, "wrote %zu positive and %zu negative framing vectors to %s\n", positives().size(),
	        negatives().size(), dir.c_str());
	return 0;
}

} // namespace

int main(int argc, char** argv) {
	std::string out, fdbRev = "unknown";
	bool generateMode = false;
	for (int i = 1; i < argc; ++i) {
		std::string a = argv[i];
		if (a == "generate") {
			generateMode = true;
		} else if (a == "--out" && i + 1 < argc) {
			out = argv[++i];
		} else if (a == "--fdb-rev" && i + 1 < argc) {
			fdbRev = argv[++i];
		} else {
			generateMode = false;
			break;
		}
	}
	if (!generateMode || out.empty()) {
		fprintf(stderr, "usage: golden_vectors_framing generate --out DIR [--fdb-rev REV]\n");
		return 2;
	}
	try {
		setupNetwork(0, UseMetrics::False);
		return generate(out, fdbRev);
	} catch (Error& e) {
		fprintf(stderr, "FDB error %d %s\n", e.code(), e.name());
		return 1;
	} catch (std::exception& e) {
		fprintf(stderr, "error: %s\n", e.what());
		return 1;
	}
}
