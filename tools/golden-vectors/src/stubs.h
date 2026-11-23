// Minimal stand-ins for the FoundationDB flow types that the flatbuffers engine
// serializes. Each stub reproduces exactly the serialization traits and file
// identifier composition of the real type (cited next to it) and nothing else,
// so the engine headers (flat_buffers.h, FileIdentifier.h,
// ObjectSerializerTraits.h) can be compiled without the rest of flow.
//
// Only wire-relevant behaviour matters here. If an FDB release changes one of
// the cited trait specializations, update the stub to match.
#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "flow/flat_buffers.h"

// Thrown for any buffer the reader rejects. Mirrors serialization_failed.
struct DecodeError : std::runtime_error {
	using std::runtime_error::runtime_error;
};

// ---------------------------------------------------------------------------
// Arena (flow/include/flow/Arena.h: class Arena, scalar_traits<Arena>)
// A zero-size scalar: it takes no vtable slot and no bytes, and must be the
// last serializer() argument.
class Arena {
public:
	constexpr static auto fb_must_appear_last = true;
	Arena() : blocks(std::make_shared<std::vector<std::unique_ptr<uint8_t[]>>>()) {}
	uint8_t* allocate(size_t n) {
		blocks->emplace_back(new uint8_t[n ? n : 1]);
		return blocks->back().get();
	}
	void dependsOn(const Arena& other) {
		// Keep other's memory alive as long as ours.
		keepAlive.push_back(other.blocks);
	}

private:
	std::shared_ptr<std::vector<std::unique_ptr<uint8_t[]>>> blocks;
	std::vector<std::shared_ptr<std::vector<std::unique_ptr<uint8_t[]>>>> keepAlive;
};

template <>
struct scalar_traits<Arena> : std::true_type {
	constexpr static size_t size = 0;
	template <class Context>
	static void save(uint8_t*, const Arena&, Context&) {}
	template <class Context>
	static void load(const uint8_t*, Arena& arena, Context& context) {
		context.addArena(arena);
	}
};

// ---------------------------------------------------------------------------
// StringRef (flow/include/flow/Arena.h: dynamic_size_traits<StringRef>)
// Same wire form as std::string: offset -> u32 length, bytes, zero padding.
class StringRef {
public:
	constexpr static FileIdentifier file_identifier = 13300811;
	StringRef() = default;
	StringRef(const uint8_t* d, int n) : d(d), n(n) {}
	StringRef(Arena& arena, const std::string& s) : n(static_cast<int>(s.size())) {
		uint8_t* p = arena.allocate(s.size());
		memcpy(p, s.data(), s.size());
		d = p;
	}
	const uint8_t* begin() const { return d; }
	const uint8_t* end() const { return d + n; }
	int size() const { return n; }
	bool empty() const { return n == 0; }
	std::string toString() const { return std::string(reinterpret_cast<const char*>(d), n); }

private:
	const uint8_t* d = nullptr;
	int n = 0;
};

template <>
struct dynamic_size_traits<StringRef> : std::true_type {
	template <class Context>
	static size_t size(const StringRef& t, Context&) {
		return t.size();
	}
	template <class Context>
	static void save(uint8_t* out, const StringRef& t, Context&) {
		std::copy(t.begin(), t.end(), out);
	}
	template <class Context>
	static void load(const uint8_t* ptr, size_t sz, StringRef& str, Context& context) {
		str = StringRef(context.tryReadZeroCopy(ptr, sz), sz);
	}
};

// ---------------------------------------------------------------------------
// VectorRef<T> (flow/include/flow/Arena.h: class VectorRef : ComposedIdentifier<T, 3>,
// vector_like_traits<VectorRef<T, VecSerStrategy::FlatBuffers>>)
template <class T>
class VectorRef : public ComposedIdentifier<T, 3> {
public:
	using value_type = T;
	VectorRef() = default;
	void push_back(Arena& arena, const T& t) {
		resize(arena, n + 1);
		d[n - 1] = t;
	}
	void resize(Arena& arena, int size) {
		T* nd = reinterpret_cast<T*>(arena.allocate(sizeof(T) * size));
		for (int i = 0; i < size; ++i) {
			new (&nd[i]) T(i < n ? d[i] : T{});
		}
		d = nd;
		n = size;
	}
	T* begin() { return d; }
	const T* begin() const { return d; }
	const T* end() const { return d + n; }
	int size() const { return n; }
	const T& operator[](int i) const { return d[i]; }

private:
	T* d = nullptr;
	int n = 0;
};

template <class T>
struct vector_like_traits<VectorRef<T>> : std::true_type {
	using Vec = VectorRef<T>;
	using value_type = T;
	using iterator = const T*;
	using insert_iterator = T*;

	template <class Context>
	static size_t num_entries(const Vec& v, Context&) {
		return v.size();
	}
	template <class Context>
	static insert_iterator insert(Vec& v, size_t s, Context& context) {
		v.resize(context.arena(), s);
		return v.begin();
	}
	template <class Context>
	static iterator begin(const Vec& v, Context&) {
		return v.begin();
	}
};

// ---------------------------------------------------------------------------
// VectorRef<KeyValueRef, VecSerStrategy::String> (flow/include/flow/Arena.h:
// dynamic_size_traits<VectorRef<V, VecSerStrategy::String>>, and
// fdbclient/include/fdbclient/FDBTypes.h: string_serialized_traits<KeyValueRef>).
// A "pre-serialized" vector: one opaque blob (same framing as a string) whose
// payload is u32 count, then per item u32 key_len, key, u32 value_len, value.
// Used by GetKeyValuesReply::data and friends.
struct KeyValue {
	std::string key;
	std::string value;
	bool operator==(const KeyValue&) const = default;
};

struct PreserializedKVVector {
	std::vector<KeyValue> items;
	bool operator==(const PreserializedKVVector&) const = default;
};

template <>
struct dynamic_size_traits<PreserializedKVVector> : std::true_type {
	template <class Context>
	static size_t size(const PreserializedKVVector& t, Context&) {
		size_t s = sizeof(uint32_t);
		for (const auto& kv : t.items) {
			s += 2 * sizeof(uint32_t) + kv.key.size() + kv.value.size();
		}
		return s;
	}
	template <class Context>
	static void save(uint8_t* out, const PreserializedKVVector& t, Context&) {
		auto put32 = [&](uint32_t x) {
			memcpy(out, &x, 4);
			out += 4;
		};
		put32(t.items.size());
		for (const auto& kv : t.items) {
			put32(kv.key.size());
			memcpy(out, kv.key.data(), kv.key.size());
			out += kv.key.size();
			put32(kv.value.size());
			memcpy(out, kv.value.data(), kv.value.size());
			out += kv.value.size();
		}
	}
	// The real loader trusts the embedded lengths (it ASSERTs that they sum to the
	// blob size after the fact). This stub rejects inconsistent blobs instead.
	template <class Context>
	static void load(const uint8_t* data, size_t size, PreserializedKVVector& t, Context&) {
		size_t pos = 0;
		auto get32 = [&]() {
			if (size - pos < 4) {
				throw DecodeError("preserialized vector item overruns blob");
			}
			uint32_t x;
			memcpy(&x, data + pos, 4);
			pos += 4;
			return x;
		};
		auto getBytes = [&](uint32_t n) {
			if (size - pos < n) {
				throw DecodeError("preserialized vector item overruns blob");
			}
			std::string s(reinterpret_cast<const char*>(data + pos), n);
			pos += n;
			return s;
		};
		uint32_t count = get32();
		t.items.clear();
		for (uint32_t i = 0; i < count; ++i) {
			KeyValue kv;
			kv.key = getBytes(get32());
			kv.value = getBytes(get32());
			t.items.push_back(std::move(kv));
		}
		if (pos != size) {
			throw DecodeError("preserialized vector has trailing bytes");
		}
	}
};

// ---------------------------------------------------------------------------
// Optional<T> (flow/include/flow/Optional.h: class Optional : ComposedIdentifier<T, 4>;
// flow/include/flow/Arena.h: union_like_traits<Optional<T>>)
// A one-alternative union; absent is union tag 0.
template <class T>
class Optional : public ComposedIdentifier<T, 4> {
public:
	using ValueType = T;
	Optional() = default;
	Optional(const T& t) : impl(t) {}
	bool present() const { return impl.has_value(); }
	const T& get() const { return *impl; }
	bool operator==(const Optional& o) const { return impl == o.impl; }

private:
	std::optional<T> impl;
};

template <class T>
struct union_like_traits<Optional<T>> : std::true_type {
	using Member = Optional<T>;
	using alternatives = pack<T>;
	template <class Context>
	static uint8_t index(const Member&, Context&) {
		return 0;
	}
	template <class Context>
	static bool empty(const Member& variant, Context&) {
		return !variant.present();
	}
	template <int i, class Context>
	static const T& get(const Member& variant, Context&) {
		static_assert(i == 0);
		return variant.get();
	}
	template <size_t i, class U, class Context>
	static void assign(Member& member, const U& t, Context&) {
		member = t;
	}
};

// ---------------------------------------------------------------------------
// UID (flow/include/flow/IRandom.h: scalar_traits<UID>): a 16-byte scalar,
// first then second, each little-endian u64. Alignment 8.
class UID {
public:
	UID() = default;
	UID(uint64_t a, uint64_t b) : part{ a, b } {}
	uint64_t first() const { return part[0]; }
	uint64_t second() const { return part[1]; }
	bool operator==(const UID&) const = default;

private:
	uint64_t part[2] = { 0, 0 };
};

template <>
struct scalar_traits<UID> : std::true_type {
	constexpr static size_t size = sizeof(uint64_t[2]);
	template <class Context>
	static void save(uint8_t* out, const UID& uid, Context&) {
		uint64_t a = uid.first(), b = uid.second();
		memcpy(out, &a, 8);
		memcpy(out + 8, &b, 8);
	}
	template <class Context>
	static void load(const uint8_t* i, UID& out, Context&) {
		uint64_t a, b;
		memcpy(&a, i, 8);
		memcpy(&b, i + 8, 8);
		out = UID(a, b);
	}
};

// ---------------------------------------------------------------------------
// Error (flow/include/flow/Error.h): a table with one field, the u16 error code.
// The flags member is not serialized.
class Error {
public:
	constexpr static FileIdentifier file_identifier = 14065384;
	static constexpr const char* type_name = "Error";
	static constexpr const char* field_names = "error_code";
	Error() = default;
	explicit Error(uint16_t code) : error_code(code) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, error_code);
	}
	int code() const { return error_code; }
	bool operator==(const Error& o) const { return error_code == o.error_code; }
	uint16_t error_code = 0xffff; // invalid_error_code
	uint16_t flags = 0;
};

// Void (flow/include/flow/flow.h): an empty table.
class Void {
public:
	constexpr static FileIdentifier file_identifier = 2010442;
	static constexpr const char* type_name = "Void";
	static constexpr const char* field_names = "";
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
	bool operator==(const Void&) const { return true; }
};

// ---------------------------------------------------------------------------
// ErrorOr<T> (flow/include/flow/flow.h: class ErrorOr : ComposedIdentifier<T, 2>,
// union_like_traits<ErrorOr<T>>): a two-alternative union, Error is tag 1 and
// T is tag 2. Never empty on the wire.
template <class T>
class ErrorOr : public ComposedIdentifier<T, 2> {
public:
	using ValueType = T;
	ErrorOr() : value(std::in_place_type<Error>, Error(1035) /* default_error_or */) {}
	ErrorOr(const Error& e) : value(std::in_place_type<Error>, e) {}
	template <class U>
	    requires(!std::is_same_v<std::decay_t<U>, Error>)
	ErrorOr(const U& t) : value(std::in_place_type<T>, t) {}
	bool present() const { return std::holds_alternative<T>(value); }
	const T& get() const { return std::get<T>(value); }
	const Error& getError() const { return std::get<Error>(value); }
	bool operator==(const ErrorOr& o) const { return value == o.value; }

private:
	std::variant<Error, T> value;
};

template <class T>
struct union_like_traits<ErrorOr<T>> : std::true_type {
	using Member = ErrorOr<T>;
	using alternatives = pack<Error, T>;
	template <class Context>
	static uint8_t index(const Member& variant, Context&) {
		return variant.present() ? 1 : 0;
	}
	template <class Context>
	static bool empty(const Member&, Context&) {
		return false;
	}
	template <int i, class Context>
	static const index_t<i, alternatives>& get(const Member& m, Context&) {
		if constexpr (i == 0) {
			return m.getError();
		} else {
			static_assert(i == 1, "ErrorOr only has two members");
			return m.get();
		}
	}
	template <int i, class Alternative, class Context>
	static void assign(Member& m, const Alternative& a, Context&) {
		m = a;
	}
};

// ---------------------------------------------------------------------------
// ReplyPromise<T> (fdbrpc/include/fdbrpc/fdbrpc.h: class ReplyPromise : ComposedIdentifier<T, 1>,
// serializable_traits<ReplyPromise<T>>): a table holding the reply endpoint's
// UID token.
template <class T>
class ReplyPromise : public ComposedIdentifier<T, 1> {
public:
	ReplyPromise() = default;
	explicit ReplyPromise(UID token) : token(token) {}
	UID token;
	bool operator==(const ReplyPromise& o) const { return token == o.token; }
};

template <class T>
struct serializable_traits<ReplyPromise<T>> : std::true_type {
	static constexpr const char* field_names = "token";
	template <class Archiver>
	static void serialize(Archiver& ar, ReplyPromise<T>& p) {
		serializer(ar, p.token);
	}
};

// std::vector<T> and std::unordered_set composition (flow/include/flow/serialize.h)
template <class T, class Allocator>
struct FileIdentifierFor<std::vector<T, Allocator>> : ComposedIdentifierExternal<T, 5> {};
template <class T, class Allocator>
struct CompositionDepthFor<std::vector<T, Allocator>> : std::integral_constant<int, CompositionDepthFor<T>::value + 1> {
};
