// Byte-level helpers shared by the engine catalog (engine_vectors.cpp) and the policy corpora
// (engine_vectors_policy.cpp): little-endian patching, the deterministic PRNG of the random-damage
// corpora, and the mutation records.
#pragma once

#include <cstring>
#include <string>
#include <vector>

#include "fixtures.h"

namespace gvm {

inline uint32_t read32(const std::vector<uint8_t>& b, size_t pos) {
	uint32_t x;
	memcpy(&x, b.data() + pos, 4);
	return x;
}
inline uint16_t read16(const std::vector<uint8_t>& b, size_t pos) {
	uint16_t x;
	memcpy(&x, b.data() + pos, 2);
	return x;
}
inline void write32(std::vector<uint8_t>& b, size_t pos, uint32_t x) {
	memcpy(b.data() + pos, &x, 4);
}
inline void write16(std::vector<uint8_t>& b, size_t pos, uint16_t x) {
	memcpy(b.data() + pos, &x, 2);
}

// A small deterministic PRNG (splitmix64), so the random corpus is identical
// on every platform and standard library.
struct SplitMix {
	uint64_t s;
	uint64_t next() {
		uint64_t z = (s += 0x9e3779b97f4a7c15ull);
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
		z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
		return z ^ (z >> 31);
	}
	uint32_t below(uint32_t n) { return static_cast<uint32_t>(next() % n); }
};

inline Json mutation_u32(size_t pos, uint32_t value) {
	return Json::object()
	    .set("op", Json::string("overwrite_u32"))
	    .set("pos", Json::uinteger(pos))
	    .set("value", Json::uinteger(value));
}
inline Json mutation_u16(size_t pos, uint16_t value) {
	return Json::object()
	    .set("op", Json::string("overwrite_u16"))
	    .set("pos", Json::uinteger(pos))
	    .set("value", Json::uinteger(value));
}
inline Json mutation_truncate(size_t len) {
	return Json::object().set("op", Json::string("truncate")).set("len", Json::uinteger(len));
}

inline std::string hexstr(uint32_t x) {
	return fid_hex(x);
}

} // namespace gvm
