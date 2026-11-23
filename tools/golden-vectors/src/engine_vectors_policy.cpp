// Engine-level vectors for the corpus policy (README.md, "Corpus accept classes"): the aliasing bombs,
// and mutation corpora over the Void, ErrorOr and EnsureTable messages of the catalog.
//
// This is a separate translation unit on purpose. The C++ writer orders a message's vtable block by the
// addresses of per-shape statics (`gen_vtable3`), which the compiler lays out in the order it instantiates
// them. Instantiating new types in engine_vectors.cpp changed the bytes of unrelated existing vectors, so
// everything that is new lives here and works from the bytes of the vectors engine_vectors.cpp already
// produced (Fixture::bytes). The Makefile links this object after engine_vectors.o.
#include "fixtures.h"
#include "mutation.h"

namespace {

using namespace gvm;

// The message of the positive vector |name|.
const std::vector<uint8_t>& positiveBytes(const std::string& name) {
	for (const auto& f : fixtures().positives) {
		if (f.name == name) {
			return f.bytes;
		}
	}
	throw std::runtime_error("no positive vector " + name);
}

struct MutationSource {
	std::string name; // prefix of the entry names
	std::string type; // cpp_type
	std::vector<uint8_t> bytes; // a valid message
	int flips; // iterations of the random-damage loop
	bool exhaustive = true; // also every truncation and the hostile words
};

// The generators of the untrusted_* corpora (every truncation, hostile words at every aligned position,
// seeded random byte damage) applied to each source, into one corpus file.
void mutationCorpus(const std::string& file,
                    const std::string& description,
                    const std::vector<MutationSource>& sources,
                    uint64_t seed) {
	std::vector<Json> lines;
	for (size_t s = 0; s < sources.size(); ++s) {
		const auto& src = sources[s];
		const auto& good = src.bytes;
		const std::string prefix = src.name + "/";
		Json sourceRoot = decode_registered(src.type, good);
		for (size_t n = 0; src.exhaustive && n < good.size(); ++n) {
			std::vector<uint8_t> g(good.begin(), good.begin() + n);
			lines.push_back(
			    corpus_entry(prefix + "truncate_" + std::to_string(n), src.type, g, mutation_truncate(n), sourceRoot));
		}
		for (uint32_t hostile : { 0u, 1u, 0xffffffffu, 0x7ffffff0u, 0x80000000u, 0xfffffff0u, 0x10000u }) {
			if (!src.exhaustive) {
				break;
			}
			for (size_t pos = 0; pos + 4 <= good.size(); pos += 4) {
				auto bad = good;
				write32(bad, pos, hostile);
				if (bad == good) {
					continue; // the word already held this value
				}
				lines.push_back(corpus_entry(prefix + "word_" + std::to_string(pos) + "_" + hexstr(hostile),
				                             src.type,
				                             bad,
				                             mutation_u32(pos, hostile),
				                             sourceRoot));
			}
		}
		SplitMix rng{ seed + s };
		for (int iter = 0; iter < src.flips; ++iter) {
			auto bad = good;
			Json bytesSet = Json::array();
			int flips = 1 + rng.below(3);
			for (int i = 0; i < flips; ++i) {
				uint32_t pos = rng.below(bad.size());
				uint8_t value = rng.below(256);
				bad[pos] = value;
				bytesSet.push(Json::object().set("pos", Json::uinteger(pos)).set("value", Json::uinteger(value)));
			}
			if (bad == good) {
				continue;
			}
			lines.push_back(corpus_entry(prefix + "flip_" + std::to_string(iter),
			                             src.type,
			                             bad,
			                             Json::object().set("op", Json::string("set_bytes")).set("bytes", bytesSet),
			                             sourceRoot));
		}
	}
	fixtures().corpora.push_back({ file, lines });
	fixtures().corpusDescriptions.push_back({ file, description });
}

// A message assembled byte by byte, for layouts the C++ writer never produces.
struct Hand {
	std::vector<uint8_t> b;
	size_t at() const { return b.size(); }
	void align(size_t a) {
		while (b.size() % a) {
			b.push_back(0);
		}
	}
	size_t put8(uint8_t x) {
		b.push_back(x);
		return b.size() - 1;
	}
	size_t put16(uint16_t x) {
		size_t p = b.size();
		b.resize(p + 2);
		write16(b, p, x);
		return p;
	}
	size_t put32(uint32_t x) {
		size_t p = b.size();
		b.resize(p + 4);
		write32(b, p, x);
		return p;
	}
	size_t put64(uint64_t x) {
		size_t p = put32(uint32_t(x));
		put32(uint32_t(x >> 32));
		return p;
	}
	// The uoffset at |field| points at |target|, which must lie after it (uoffsets are unsigned).
	void ref(size_t field, size_t target) {
		if (target < field) {
			throw std::runtime_error("backward uoffset");
		}
		write32(b, field, uint32_t(target - field));
	}
	// The table at |table| uses the vtable at |vtable|.
	void vtable(size_t table, size_t vtable) { write32(b, table, uint32_t(int32_t(table) - int32_t(vtable))); }
};

constexpr uint32_t kUntrustedFid = 4242; // Untrusted::file_identifier
constexpr uint32_t kRootFid = 0x0fdb08; // Root::file_identifier

// An `Untrusted` (slots: s, v, o tag, o offset, vo tags, vo offsets, b, strs) holding s = "x", v = [1],
// o = 5, vo = [1], b = [true], and |aliases| offsets in strs that all point at one string of |length|
// 'a' bytes. aliasedUntrusted(20, 600) is 824 bytes.
std::vector<uint8_t> aliasedUntrusted(size_t aliases, size_t length) {
	Hand h;
	h.put32(0); // root offset, patched below
	h.put32(kUntrustedFid);
	size_t vtWrapper = h.put16(6);
	h.put16(8);
	h.put16(4);
	size_t vtUntrusted = h.put16(20);
	h.put16(33);
	for (uint16_t entry : { 4, 8, 32, 12, 16, 20, 24, 28 }) { // s v o.tag o.offset vo.tags vo.offsets b strs
		h.put16(entry);
	}
	h.align(4);
	size_t wrapper = h.put32(0);
	size_t wrapperSlot = h.put32(0);
	size_t t = h.put32(0); // soffset
	for (int i = 0; i < 7; ++i) {
		h.put32(0); // offsets, patched below
	}
	h.put8(1); // o's tag: alternative 0 present
	h.align(4);
	h.ref(0, wrapper);
	h.vtable(wrapper, vtWrapper);
	h.ref(wrapperSlot, t);
	h.vtable(t, vtUntrusted);

	h.ref(t + 4, h.put32(1)); // s
	h.put8('x');
	h.align(4);
	while ((h.at() + 4) % 8) { // v's elements are 8-aligned
		h.put8(0);
	}
	h.ref(t + 8, h.put32(1));
	h.put64(1);
	h.ref(t + 12, h.put32(5)); // o's payload, unaligned and unpadded
	h.ref(t + 16, h.put32(1)); // vo's tags
	h.put8(1);
	h.align(4);
	size_t voOffsets = h.put32(1);
	h.ref(t + 20, voOffsets);
	size_t voEntry = h.put32(0);
	h.ref(voEntry, h.put32(1)); // the payload
	h.ref(t + 24, h.put32(1)); // b
	h.put8(1);
	h.align(4);
	h.ref(t + 28, h.put32(aliases)); // strs
	std::vector<size_t> slots;
	for (size_t i = 0; i < aliases; ++i) {
		slots.push_back(h.put32(0));
	}
	size_t str = h.put32(length);
	for (size_t i = 0; i < length; ++i) {
		h.put8('a');
	}
	h.align(4);
	for (size_t slot : slots) {
		h.ref(slot, str);
	}
	return h.b;
}

// A `Root` whose b holds |outer| offsets to one Nested2 table, whose b holds |inner| offsets to one
// string of |length| bytes: outer * inner visits of the string.
std::vector<uint8_t> aliasedNestedRoot(size_t outer, size_t inner, size_t length) {
	Hand h;
	h.put32(0);
	h.put32(kRootFid);
	size_t vtWrapper = h.put16(6);
	h.put16(8);
	h.put16(4);
	size_t vtTable = h.put16(8); // Root and Nested2 both: slot 0 absent, slot 1 (b) at +4, the rest absent
	h.put16(8);
	h.put16(0);
	h.put16(4);
	h.align(4);
	size_t wrapper = h.put32(0);
	size_t wrapperSlot = h.put32(0);
	size_t root = h.put32(0);
	size_t rootB = h.put32(0);
	h.ref(0, wrapper);
	h.vtable(wrapper, vtWrapper);
	h.ref(wrapperSlot, root);
	h.vtable(root, vtTable);

	h.ref(rootB, h.put32(outer));
	std::vector<size_t> slots;
	for (size_t i = 0; i < outer; ++i) {
		slots.push_back(h.put32(0));
	}
	size_t nested = h.put32(0);
	size_t nestedB = h.put32(0);
	h.vtable(nested, vtTable);
	for (size_t slot : slots) {
		h.ref(slot, nested);
	}
	h.ref(nestedB, h.put32(inner));
	slots.clear();
	for (size_t i = 0; i < inner; ++i) {
		slots.push_back(h.put32(0));
	}
	size_t str = h.put32(length);
	for (size_t i = 0; i < length; ++i) {
		h.put8("0123456789abcdef"[i % 16]);
	}
	h.align(4);
	for (size_t slot : slots) {
		h.ref(slot, str);
	}
	return h.b;
}

Json mutation_hand_built() {
	return Json::object().set("op", Json::string("hand_built"));
}

// Work units one decode of |bytes| as |type| charges (strict.h), and the limit.
size_t workOf(const std::string& type, const std::vector<uint8_t>& bytes, size_t* limit) {
	return registry().at(type).work(bytes.data(), bytes.size(), limit);
}

void aliasingBombs() {
	std::vector<Json> lines;
	Json untrustedRoot = decode_registered("Untrusted", positiveBytes("untrusted_good"));
	Json rootRoot = decode_registered("Root", positiveBytes("nested_root"));

	// The README's example: 20 offsets to one 600-byte string.
	auto bomb = aliasedUntrusted(20, 600);
	if (bomb.size() != 824) {
		throw std::runtime_error("aliased Untrusted is not 824 bytes");
	}
	lines.push_back(corpus_entry("untrusted_alias_20x600", "Untrusted", bomb, mutation_hand_built(), untrustedRoot));

	// The limit exactly: the same shape sized so that one decode charges exactly 4 * length + 1024
	// units (accepted), and one byte more string (over by a few units).
	bool found = false;
	for (size_t m = 8; m < 40 && !found; ++m) {
		for (size_t len = 0; len < 3000 && !found; ++len) {
			size_t limit, overLimit;
			auto at = aliasedUntrusted(m, len);
			if (workOf("Untrusted", at, &limit) != limit || !try_decode("Untrusted", at, nullptr).empty()) {
				continue;
			}
			auto next = aliasedUntrusted(m, len + 1);
			if (workOf("Untrusted", next, &overLimit) <= overLimit || !try_decode("Untrusted", next, nullptr).empty()) {
				continue;
			}
			lines.push_back(corpus_entry("untrusted_alias_" + std::to_string(m) + "x" + std::to_string(len) + "_at_limit",
			                             "Untrusted",
			                             at,
			                             mutation_hand_built(),
			                             untrustedRoot));
			lines.push_back(corpus_entry(
			    "untrusted_alias_" + std::to_string(m) + "x" + std::to_string(len + 1) + "_over_limit",
			    "Untrusted",
			    next,
			    mutation_hand_built(),
			    untrustedRoot));
			found = true;
		}
	}
	if (!found) {
		throw std::runtime_error("no aliased Untrusted sits exactly at the work limit");
	}

	// Nested aliasing: a vector of offsets to one table whose vector of offsets points at one string.
	lines.push_back(corpus_entry("root_alias_3x2x16", "Root", aliasedNestedRoot(3, 2, 16), mutation_hand_built(), rootRoot));
	lines.push_back(
	    corpus_entry("root_alias_40x40x16", "Root", aliasedNestedRoot(40, 40, 16), mutation_hand_built(), rootRoot));

	fixtures().corpora.push_back({ "aliasing_bombs", lines });
	fixtures().corpusDescriptions.push_back(
	    { "aliasing_bombs",
	      "Hand-built messages whose offsets reference the same objects many times, which the C++ reader accepts: "
	      "the 824-byte Untrusted with 20 offsets to one 600-byte string; an Untrusted sized to charge exactly "
	      "4 * length + 1024 work units (data), and the same with one more byte of string (work_limit); a Root "
	      "whose vector holds 40 offsets to one Nested2 table, whose vector holds 40 offsets to one string "
	      "(work_limit), and a 3 x 2 version well under the limit (data)." });
}

// Positive vectors for the types the policy corpora mutate and the catalog lacks.
void voidVectors() {
	positive("error_or",
	         "void_root",
	         "A bare Void as the root member: root slot 0 holds an offset to a zero-field table (just its 4-byte "
	         "soffset), which the reader never follows. File identifier 2010442.",
	         "Void",
	         Void::file_identifier,
	         Void());
	using EOVPlain = ErrorOr<Void>;
	constexpr FileIdentifier eovPlainFid = FileIdentifierFor<EOVPlain>::value;
	positive("error_or",
	         "error_or_plain_void_success",
	         "ErrorOr<Void> (no EnsureTable; Void is a table, so the wire form is the same as "
	         "ErrorOr<EnsureTable<Void>>) holding Void: tag 2, offset to the empty table.",
	         "ErrorOr<Void>",
	         eovPlainFid,
	         EOVPlain(Void()));
	positive("error_or",
	         "error_or_plain_void_error",
	         "ErrorOr<Void> carrying an Error (1020 not_committed): tag 1, offset to the Error table.",
	         "ErrorOr<Void>",
	         eovPlainFid,
	         EOVPlain(Error(1020)));
}

void policyCorpora() {
	const std::string how =
	    "The untrusted_* generators (every truncation; the hostile words 0, 1, 0xffffffff, 0x7ffffff0, 0x80000000, "
	    "0xfffffff0 and 0x10000 at every aligned position, no-op overwrites omitted; seeded random byte damage, "
	    "splitmix64, 1-3 bytes) applied to ";
	mutationCorpus("void_mutations",
	               how + "Void as the root member (void_root: a zero-field table, whose offset the C++ reader never "
	                     "follows), with the C++ (FDB main) reader's outcome. Names are <source>/<mutation>.",
	               { { "void_root", "Void", positiveBytes("void_root"), 300 } },
	               0x0fdb5eed + 100);
	mutationCorpus("error_or_void_mutations",
	               how + "ErrorOr<Void> holding Void (error_or_plain_void_success) and an Error "
	                     "(error_or_plain_void_error). Names are <source>/<mutation>.",
	               { { "plain_void_success", "ErrorOr<Void>", positiveBytes("error_or_plain_void_success"), 200 },
	                 { "plain_void_error", "ErrorOr<Void>", positiveBytes("error_or_plain_void_error"), 200 } },
	               0x0fdb5eed + 200);
	mutationCorpus("error_or_ensure_table_void_mutations",
	               how + "ErrorOr<EnsureTable<Void>>, the reply to a ReplyPromise<Void>: success "
	                     "(error_or_void_success) and broken_promise (error_or_void_broken_promise). Names are "
	                     "<source>/<mutation>.",
	               { { "void_success", "ErrorOr<EnsureTable<Void>>", positiveBytes("error_or_void_success"), 200 },
	                 { "void_broken_promise",
	                   "ErrorOr<EnsureTable<Void>>",
	                   positiveBytes("error_or_void_broken_promise"),
	                   200 } },
	               0x0fdb5eed + 300);
	mutationCorpus("error_or_payload_mutations",
	               how + "replies carrying a table: ErrorOr<EnsureTable<Payload>> holding a Payload (error_or_success) "
	                     "and an Error (error_or_error), and, with the random damage only, ErrorOr<Payload> (no "
	                     "EnsureTable; the same bytes as the first) holding the Payload "
	                     "(error_or_without_ensure_table). Names are <source>/<mutation>.",
	               { { "success", "ErrorOr<EnsureTable<Payload>>", positiveBytes("error_or_success"), 60 },
	                 { "error", "ErrorOr<EnsureTable<Payload>>", positiveBytes("error_or_error"), 60 },
	                 { "plain_success",
	                   "ErrorOr<Payload>",
	                   positiveBytes("error_or_without_ensure_table"),
	                   100,
	                   false } },
	               0x0fdb5eed + 400);
	mutationCorpus("ensure_table_mutations",
	               how + "the EnsureTables message (ensure_table: EnsureTable around a scalar, a table, a string and a "
	                     "vector, and inside a variant). Names are <source>/<mutation>.",
	               { { "ensure_tables", "EnsureTables", positiveBytes("ensure_table"), 60 } },
	               0x0fdb5eed + 500);
	mutationCorpus("error_or_ensure_table_optional_mutations",
	               how + "ErrorOr<EnsureTable<Optional<Inner>>> holding an Optional "
	                     "(fid_error_or_ensure_table_optional: EnsureTable around a union, wrapped in a one-slot "
	                     "table). Names are <source>/<mutation>.",
	               { { "optional_success",
	                   "ErrorOr<EnsureTable<Optional<Inner>>>",
	                   positiveBytes("fid_error_or_ensure_table_optional"),
	                   150 } },
	               0x0fdb5eed + 600);
}

} // namespace

// Called after build_engine_vectors() has registered and produced everything else.
void build_policy_vectors() {
	voidVectors();
	if (!g_positiveOnly) {
		aliasingBombs();
		policyCorpora();
	}
}
