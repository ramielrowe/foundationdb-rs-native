// Encode/decode entry points mirroring ObjectWriter / ArenaObjectReader
// (flow/include/flow/ObjectSerializer.h) without the rest of flow.
#pragma once

#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "dump.h"
#include "schema.h"
#include "strict.h"
#include "stubs.h"

// The save context ObjectWriter passes to save_members.
struct SaveContext {
	std::vector<uint8_t>* out;
	uint8_t fill;
	uint8_t* allocate(size_t n) {
		out->assign(n, fill);
		return out->data();
	}
	void addArena(Arena&) {}
	SaveContext& context() { return *this; }
};

// The load context, mirroring LoadContext<ArenaObjectReader> on FDB main,
// including its bounds checks (_ObjectReader::checkRange).
struct LoadContext {
	const uint8_t* begin;
	const uint8_t* end;
	Arena* arenaPtr;

	Arena& arena() { return *arenaPtr; }
	const uint8_t* tryReadZeroCopy(const uint8_t* ptr, unsigned) { return ptr; }
	void addArena(Arena& a) { a = *arenaPtr; }
	void checkRange(const uint8_t* ptr, size_t len) const {
		if (len == 0) {
			return;
		}
		auto p = reinterpret_cast<uintptr_t>(ptr);
		auto b = reinterpret_cast<uintptr_t>(begin);
		auto e = reinterpret_cast<uintptr_t>(end);
		if (p < b || p > e || len > e - p) {
			throw DecodeError("read outside of buffer");
		}
	}
	[[noreturn]] void loadFailed(const char* reason) const { throw DecodeError(reason); }
	LoadContext& context() { return *this; }
};

// ObjectWriter::serialize(file_identifier, items...). Encodes twice with
// different fill bytes so any byte the writer leaves uninitialized shows up.
template <class... Items>
std::vector<uint8_t> encode(FileIdentifier fid, const Items&... items) {
	std::vector<uint8_t> a, b;
	SaveContext ca{ &a, 0x00 };
	save_members(ca, fid, items...);
	SaveContext cb{ &b, 0xff };
	save_members(cb, fid, items...);
	if (a != b) {
		throw std::runtime_error("writer left uninitialized bytes in the buffer");
	}
	return a;
}

struct DecodeOptions {
	bool checkFileIdentifier = true;
};

// _ObjectReader::deserialize(file_identifier, items...) on FDB main.
template <class... Items>
void decode_into(Arena& arena,
                 const uint8_t* data,
                 size_t size,
                 FileIdentifier fid,
                 const DecodeOptions& opts,
                 Items&... items) {
	if (size < 2 * sizeof(uint32_t)) {
		throw DecodeError("buffer too short for header");
	}
	if (opts.checkFileIdentifier && read_file_identifier(data) != fid) {
		throw DecodeError("file identifier mismatch");
	}
	LoadContext ctx{ data, data + size, &arena };
	load_members(data, ctx, items...);
}

// A type that `golden-vectors decode <name>` understands.
struct DecodableType {
	std::string name;
	FileIdentifier fid;
	// Decodes and returns the root node; throws DecodeError on rejection.
	std::function<Json(const uint8_t*, size_t, const DecodeOptions&)> decode;
	// The permitted strict checks (strict.h) that the bytes fail.
	std::function<std::set<std::string>(const uint8_t*, size_t)> strict;
	// The members of the root table; registers every reachable table in the collector.
	std::function<Json(SchemaCollector&)> schema;
};

inline std::map<std::string, DecodableType>& registry() {
	static std::map<std::string, DecodableType> r;
	return r;
}

template <class... Items>
Json decode_as(const uint8_t* data, size_t size, FileIdentifier fid, const DecodeOptions& opts) {
	// Copy into an exactly-sized heap block so sanitizers catch over-reads.
	std::unique_ptr<uint8_t[]> buf(new uint8_t[size ? size : 1]);
	if (size) {
		memcpy(buf.get(), data, size);
	}
	Arena arena;
	std::tuple<Items...> items{};
	std::apply([&](Items&... it) { decode_into(arena, buf.get(), size, fid, opts, it...); }, items);
	return std::apply([](const Items&... it) { return dump_root(it...); }, items);
}

template <class... Items>
void register_type(const std::string& name, FileIdentifier fid) {
	auto& r = registry();
	auto it = r.find(name);
	if (it != r.end()) {
		if (it->second.fid != fid) {
			throw std::runtime_error("type " + name + " registered with two file identifiers");
		}
		return;
	}
	r[name] = DecodableType{
		name,
		fid,
		[fid](const uint8_t* data, size_t size, const DecodeOptions& opts) {
		    return decode_as<Items...>(data, size, fid, opts);
		},
		[](const uint8_t* data, size_t size) {
		    StrictWalker w(data, size);
		    w.template root<Items...>();
		    return w.violations;
		},
		[](SchemaCollector& c) { return root_schema<Items...>(c); },
	};
}
