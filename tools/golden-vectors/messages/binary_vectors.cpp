// Golden vectors for FDB's legacy binary format (BinaryWriter / BinaryReader, flow/serialize.h):
// real flow and fdbclient types of one release branch (release-7.4 or release-8.0), written by
// that branch's own BinaryWriter and read back by its own BinaryReader. Built inside the
// branch's CMake build (targets.cmake) and run by ../regen_messages.sh. See ../README.md
// ("Binary-format vectors").
//
// Every positive vector is checked here: the value is written, the bytes are read back with the
// matching BinaryReader (which must consume them all), and the decoded value must give the same
// JSON as the original and write the same bytes again.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <array>
#include <deque>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "json.h"

#if __has_include("fdbclient/NativeAPI.h")
#include "fdbclient/NativeAPI.h"
#else
#include "fdbclient/NativeAPI.actor.h"
#endif
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/GrvProxyInterface.h"
#if __has_include("fdbclient/IdempotencyId.h")
#include "fdbclient/IdempotencyId.h"
#else
#include "fdbclient/IdempotencyId.actor.h"
#endif
#include "fdbclient/SystemData.h"
#include "fdbclient/Tracing.h"
#include "flow/serialize.h"

#include "connect_packet.h"

using namespace std::string_literals;

namespace {

constexpr int kFormatVersion = 1;

// ---------------------------------------------------------------------------------------
// Version options.

struct Mode {
	enum class Kind { Unversioned, Include, Assume } kind = Kind::Unversioned;
	ProtocolVersion version;

	const char* name() const {
		switch (kind) {
		case Kind::Unversioned:
			return "unversioned";
		case Kind::Include:
			return "include";
		case Kind::Assume:
			return "assume";
		}
		return "";
	}
};

Mode unversioned() {
	return Mode{ Mode::Kind::Unversioned, ProtocolVersion() };
}
Mode include(ProtocolVersion v = currentProtocolVersion()) {
	return Mode{ Mode::Kind::Include, v };
}
Mode assume(ProtocolVersion v = currentProtocolVersion()) {
	return Mode{ Mode::Kind::Assume, v };
}

// Calls f with the C++ version option the mode stands for.
template <class F>
decltype(auto) withOption(const Mode& m, F&& f) {
	switch (m.kind) {
	case Mode::Kind::Include:
		return f(IncludeVersion(m.version));
	case Mode::Kind::Assume:
		return f(AssumeVersion(m.version));
	case Mode::Kind::Unversioned:
		break;
	}
	return f(Unversioned());
}

// ---------------------------------------------------------------------------------------
// Bytes and JSON.

std::string hex(const uint8_t* p, size_t n) {
	static const char* d = "0123456789abcdef";
	std::string s;
	for (size_t i = 0; i < n; ++i) {
		s += d[p[i] >> 4];
		s += d[p[i] & 15];
	}
	return s;
}
std::string hex(StringRef s) {
	return hex(s.begin(), s.size());
}
std::string hex(const std::string& s) {
	return hex(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

template <class T>
struct IsSpecialization : std::false_type {};
template <template <class...> class C, class... A>
struct IsSpecialization<C<A...>> : std::true_type {};
template <class T, template <class...> class C>
struct IsA : std::false_type {};
template <template <class...> class C, class... A>
struct IsA<C<A...>, C> : std::true_type {};
template <class T>
struct IsStdArray : std::false_type {};
template <class T, size_t N>
struct IsStdArray<std::array<T, N>> : std::true_type {};

template <class T>
struct IsStandalone : std::false_type {};
template <class T>
struct IsStandalone<Standalone<T>> : std::true_type {
	using Inner = T;
};
template <class T>
struct IsVectorRef : std::false_type {};
template <class T, VecSerStrategy S>
struct IsVectorRef<VectorRef<T, S>> : std::true_type {};

struct Enc {}; // base of the test structs that have a toJson() member

// The logical value of a vector as JSON. The Rust tests build the same JSON from the decoded
// Rust value (tests/binary_fixtures.rs, trait Logical), so the shapes are fixed here:
//   integers, bool, double: JSON numbers and booleans; enums: their underlying integer
//   std::string: a JSON string (ASCII only); StringRef, Key, Value: lowercase hex
//   UID: {"first": u64, "second": u64}; Error: its code
//   vector, deque, array, VectorRef, set: an array; map: an array of [key, value] pairs
//   Optional<T>: null or the value; pair: [first, second]
template <class T>
Json toJson(const T& v);

template <class T>
Json toJson(const T& v) {
	if constexpr (std::is_same_v<T, bool>) {
		return Json::boolean(v);
	} else if constexpr (std::is_enum_v<T>) {
		return toJson(static_cast<std::underlying_type_t<T>>(v));
	} else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
		return Json::integer(v);
	} else if constexpr (std::is_integral_v<T>) {
		return Json::uinteger(v);
	} else if constexpr (std::is_same_v<T, double>) {
		return Json::number(v);
	} else if constexpr (std::is_same_v<T, std::string>) {
		for (unsigned char c : v) {
			if (c < 0x20 || c >= 0x7f) {
				throw std::runtime_error("std::string vectors must be printable ASCII");
			}
		}
		return Json::string(v);
	} else if constexpr (std::is_same_v<T, StringRef> || std::is_same_v<T, Standalone<StringRef>>) {
		return Json::string(hex(StringRef(v)));
	} else if constexpr (std::is_same_v<T, UID>) {
		return Json::object().set("first", Json::uinteger(v.first())).set("second", Json::uinteger(v.second()));
	} else if constexpr (std::is_same_v<T, Error>) {
		return Json::uinteger(v.code());
	} else if constexpr (std::is_same_v<T, ProtocolVersion>) {
		return Json::uinteger(v.versionWithFlags());
	} else if constexpr (std::is_same_v<T, KeyValueRef>) {
		return Json::array().push(toJson(v.key)).push(toJson(v.value));
	} else if constexpr (std::is_same_v<T, KeyRangeRef> || std::is_same_v<T, Standalone<KeyRangeRef>>) {
		return Json::array().push(toJson(v.begin)).push(toJson(v.end));
	} else if constexpr (IsStandalone<T>::value) {
		return toJson(static_cast<const typename IsStandalone<T>::Inner&>(v));
	} else if constexpr (IsA<T, Optional>::value) {
		return v.present() ? toJson(v.get()) : Json::null();
	} else if constexpr (IsA<T, std::pair>::value) {
		return Json::array().push(toJson(v.first)).push(toJson(v.second));
	} else if constexpr (IsA<T, std::map>::value) {
		Json a = Json::array();
		for (const auto& [k, val] : v) {
			a.push(Json::array().push(toJson(k)).push(toJson(val)));
		}
		return a;
	} else if constexpr (IsA<T, std::variant>::value) {
		Json out = Json::object();
		out.set("index", Json::uinteger(v.index()));
		std::visit([&](const auto& inner) { out.set("value", toJson(inner)); }, v);
		return out;
	} else if constexpr (IsA<T, std::vector>::value || IsA<T, std::deque>::value || IsA<T, std::set>::value ||
	                     IsStdArray<T>::value || IsVectorRef<T>::value) {
		Json a = Json::array();
		for (const auto& e : v) {
			a.push(toJson(e));
		}
		return a;
	} else if constexpr (std::is_base_of_v<Enc, T>) {
		return v.toJson();
	} else {
		static_assert(!sizeof(T), "no toJson for this type");
	}
}

// ---------------------------------------------------------------------------------------
// Test types.

enum class Color : uint8_t { red = 1, green = 2, blue = 200 };
enum Level : int32_t { low = -5, mid = 0, high = 70000 };

// struct Point3 { int32_t x; int64_t y; std::string label; serialize: serializer(ar, x, y, label) }
struct Point3 : Enc {
	int32_t x = 0;
	int64_t y = 0;
	std::string label;
	Point3() = default;
	Point3(int32_t x, int64_t y, std::string label) : x(x), y(y), label(std::move(label)) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, x, y, label);
	}
	Json toJson() const {
		return Json::array().push(::toJson(x)).push(::toJson(y)).push(::toJson(label));
	}
};

using Variant3 = std::variant<int32_t, std::string, uint8_t>;

Arena& valueArena() {
	static Arena* a = new Arena();
	return *a;
}
StringRef sref(const std::string& s) {
	return StringRef(valueArena(), StringRef(s));
}
Key key(const std::string& s) {
	return Key(StringRef(s));
}
std::string bytes(std::initializer_list<int> b) {
	std::string s;
	for (int c : b) {
		s += static_cast<char>(c);
	}
	return s;
}

// ---------------------------------------------------------------------------------------
// Vector collection.

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

Json versioningJson(const Mode& m) {
	return Json::object()
	    .set("mode", Json::string(m.name()))
	    .set("protocol_version",
	         m.kind == Mode::Kind::Unversioned ? Json::null() : Json::uinteger(m.version.versionWithFlags()));
}

void addPositive(const std::string& name,
                 const std::string& group,
                 const std::string& description,
                 const std::string& cppType,
                 const std::string& type,
                 const Mode& mode,
                 const Standalone<StringRef>& value,
                 Json logical,
                 Json extra = Json::object()) {
	for (const auto& r : positives()) {
		if (r.name == name) {
			throw std::runtime_error("duplicate vector " + name);
		}
	}
	Json j = Json::object();
	j.set("format_version", Json::integer(kFormatVersion));
	j.set("name", Json::string(name));
	j.set("group", Json::string(group));
	j.set("kind", Json::string("positive"));
	j.set("description", Json::string(description));
	j.set("cpp_type", Json::string(cppType));
	j.set("type", Json::string(type));
	j.set("versioning", versioningJson(mode));
	j.set("size", Json::uinteger(value.size()));
	j.set("bytes", Json::string(hex(value)));
	j.set("value", std::move(logical));
	j.mergeFrom(extra);
	positives().push_back({ name, std::move(j) });
}

// Writes `value` with BinaryWriter, reads it back with BinaryReader (all bytes must be
// consumed), checks the round trip, and records the vector. `type` is the Rust-side type name
// (the tests' dispatch key).
template <class T>
void vec(const std::string& name,
         const std::string& group,
         const std::string& description,
         const std::string& cppType,
         const std::string& type,
         const Mode& mode,
         const T& value) {
	Standalone<StringRef> written = withOption(mode, [&](auto vo) {
		BinaryWriter wr(vo);
		wr << value;
		return wr.toValue();
	});
	T back;
	try {
		back = withOption(mode, [&](auto vo) {
			BinaryReader rd(written, vo);
			T t;
			rd >> t;
			rd.assertEnd();
			return t;
		});
	} catch (Error& e) {
		throw std::runtime_error("vector " + name + ": the reader failed with " + e.name() + " on " + hex(written));
	}
	Json before = toJson(value);
	Json after = toJson(back);
	if (!(before == after)) {
		throw std::runtime_error("vector " + name + " does not round trip: " + before.dump(-1) + " vs " +
		                         after.dump(-1));
	}
	Standalone<StringRef> again = withOption(mode, [&](auto vo) {
		BinaryWriter wr(vo);
		wr << back;
		return wr.toValue();
	});
	if (again != written) {
		throw std::runtime_error("vector " + name + " does not write the same bytes twice");
	}
	addPositive(name, group, description, cppType, type, mode, written, std::move(before));
}

// A reader that must reject `data`. `build` runs the reader (it throws on rejection).
template <class F>
void rejects(const std::string& name,
             const std::string& description,
             const std::string& type,
             const Mode& mode,
             const std::string& data,
             F&& run) {
	std::string reason;
	int code = 0;
	try {
		run(Standalone<StringRef>(StringRef(data)));
	} catch (Error& e) {
		reason = e.name();
		code = e.code();
	}
	if (reason.empty()) {
		throw std::runtime_error("the reader accepted negative vector " + name);
	}
	Json j = Json::object();
	j.set("format_version", Json::integer(kFormatVersion));
	j.set("name", Json::string(name));
	j.set("kind", Json::string("negative"));
	j.set("description", Json::string(description));
	j.set("type", Json::string(type));
	j.set("versioning", versioningJson(mode));
	j.set("bytes", Json::string(hex(data)));
	j.set("expect", Json::string("reject"));
	j.set("cpp_reason", Json::string(reason));
	j.set("cpp_error_code", Json::integer(code));
	negatives().push_back({ name, std::move(j) });
}

template <class T>
void rejectsAs(const std::string& name,
               const std::string& description,
               const std::string& type,
               const Mode& mode,
               const std::string& data) {
	rejects(name, description, type, mode, data, [&](const Standalone<StringRef>& s) {
		withOption(mode, [&](auto vo) {
			BinaryReader rd(s, vo);
			T t;
			rd >> t;
			return 0;
		});
	});
}

UID uid(uint64_t n) {
	return UID(0x1111000000000000ULL + n, 0x2222000000000000ULL + n);
}

// ---------------------------------------------------------------------------------------
// The vectors.

void primitiveVectors() {
	const std::string g = "primitives";
	const Mode u = unversioned();
	vec("u8", g, "uint8_t: one byte.", "uint8_t", "u8", u, uint8_t(0xab));
	vec("i8", g, "int8_t -2.", "int8_t", "i8", u, int8_t(-2));
	vec("u16", g, "uint16_t, little-endian.", "uint16_t", "u16", u, uint16_t(0xbeef));
	vec("i16", g, "int16_t -2.", "int16_t", "i16", u, int16_t(-2));
	vec("u32", g, "uint32_t, little-endian.", "uint32_t", "u32", u, uint32_t(0xdeadbeef));
	vec("i32", g, "int32_t -123456.", "int32_t", "i32", u, int32_t(-123456));
	vec("u64", g, "uint64_t, little-endian.", "uint64_t", "u64", u, uint64_t(0x0123456789abcdefULL));
	vec("i64_min", g, "int64_t minimum.", "int64_t", "i64", u, std::numeric_limits<int64_t>::min());
	vec("i64_neg1", g, "int64_t -1.", "int64_t", "i64", u, int64_t(-1));
	vec("f64", g, "double 3.141592653589793: the IEEE bits, little-endian.", "double", "f64", u, 3.141592653589793);
	vec("f64_neg_zero", g, "double -0.0.", "double", "f64", u, -0.0);
	vec("bool_true", g, "bool true: one byte, 1.", "bool", "bool", u, true);
	vec("bool_false", g, "bool false: one byte, 0.", "bool", "bool", u, false);
	vec("enum_u8", g, "enum class Color : uint8_t: the underlying value.", "Color", "enum_color", u, Color::blue);
	vec("enum_i32",
	    g,
	    "enum Level : int32_t with a negative value: the underlying value, little-endian.",
	    "Level",
	    "enum_level",
	    u,
	    Level::low);
	vec("uid", g, "UID: part[0] then part[1], two little-endian uint64_t.", "UID", "uid", u, uid(7));
	vec("protocol_version",
	    g,
	    "ProtocolVersion with the object-serializer flag: versionWithFlags() as a uint64_t.",
	    "ProtocolVersion",
	    "protocol_version",
	    u,
	    ProtocolVersion(0x0FDB00B074000000ULL | ProtocolVersion::objectSerializerFlag));
	vec("error", g, "Error: its code, a uint16_t. (Error::serialize needs a valid protocol version, so AssumeVersion.)", "Error", "error", assume(), Error::fromCode(1007));
}

void containerVectors() {
	const std::string g = "containers";
	// Containers and structs assert that the archive's protocol version is valid, so these are
	// written with AssumeVersion (nothing is added to the bytes).
	const Mode a = assume();
	vec("string_empty", g, "std::string, empty: an int32_t length of 0.", "std::string", "string", a, ""s);
	vec("string_hello", g, "std::string: an int32_t length, then the bytes.", "std::string", "string", a, "hello"s);
	vec("string_ref",
	    g,
	    "StringRef holding NUL and high bytes: a uint32_t length, then the bytes.",
	    "Standalone<StringRef>",
	    "bytes",
	    a,
	    Standalone<StringRef>(sref(bytes({ 0x00, 0xff, 0x61, 0x00, 0x80 }))));
	vec("key", g, "Key (Standalone<StringRef>): the same bytes as a StringRef.", "Key", "bytes", a, key("a/key"));
	vec("vec_i32", g, "std::vector<int32_t>: an int count, then the elements.", "std::vector<int32_t>", "vec<i32>", a, std::vector<int32_t>{ 1, -2, 300000 });
	vec("vec_i32_empty", g, "Empty std::vector<int32_t>.", "std::vector<int32_t>", "vec<i32>", a, std::vector<int32_t>{});
	vec("vec_u8", g, "std::vector<uint8_t>: a count and the bytes.", "std::vector<uint8_t>", "vec<u8>", a, std::vector<uint8_t>{ 0, 1, 255 });
	vec("vec_string", g, "std::vector<std::string>.", "std::vector<std::string>", "vec<string>", a, std::vector<std::string>{ "a", "", "bcd" });
	vec("vec_vec_u16", g, "Nested std::vector<std::vector<uint16_t>>.", "std::vector<std::vector<uint16_t>>", "vec<vec<u16>>", a, std::vector<std::vector<uint16_t>>{ { 1, 2 }, {}, { 0xffff } });
	vec("array_u16_3", g, "std::array<uint16_t, 3>: the elements, no count.", "std::array<uint16_t, 3>", "array<u16;3>", a, std::array<uint16_t, 3>{ 1, 2, 3 });
	vec("array_u8_4", g, "std::array<uint8_t, 4>.", "std::array<uint8_t, 4>", "array<u8;4>", a, std::array<uint8_t, 4>{ 9, 8, 7, 6 });
	vec("set_i32", g, "std::set<int32_t>: a count, then the elements in order.", "std::set<int32_t>", "set<i32>", a, std::set<int32_t>{ 5, -1, 3 });
	vec("map_string_i64", g, "std::map<std::string, int64_t>: a count, then each key and its value.", "std::map<std::string, int64_t>", "map<string,i64>", a, std::map<std::string, int64_t>{ { "x", 1 }, { "yy", -2 } });
	vec("map_uid_i64", g, "std::map<UID, int64_t>.", "std::map<UID, int64_t>", "map<uid,i64>", a, std::map<UID, int64_t>{ { uid(2), 20 }, { uid(1), 10 } });
	vec("optional_present", g, "Optional<int32_t> with a value: a bool, then the value.", "Optional<int32_t>", "optional<i32>", a, Optional<int32_t>(42));
	vec("optional_absent", g, "Optional<int32_t> without a value: a bool, 0.", "Optional<int32_t>", "optional<i32>", a, Optional<int32_t>());
	vec("optional_string", g, "Optional<std::string>.", "Optional<std::string>", "optional<string>", a, Optional<std::string>("opt"s));
	vec("pair", g, "std::pair<uint8_t, std::string>: first, then second.", "std::pair<uint8_t, std::string>", "pair<u8,string>", a, std::make_pair(uint8_t(7), "seven"s));
	vec("struct_point3", g, "A struct whose serialize() is serializer(ar, x, y, label): its members back to back.", "Point3", "point3", a, Point3(-1, 1LL << 40, "p"));
	vec("variant_int", g, "std::variant<int32_t, std::string, uint8_t> holding alternative 0: a uint8_t index, then the value.", "std::variant<int32_t, std::string, uint8_t>", "variant3", a, Variant3(int32_t(-9)));
	vec("variant_string", g, "The same variant holding alternative 1.", "std::variant<int32_t, std::string, uint8_t>", "variant3", a, Variant3(std::string("v")));
	vec("variant_u8", g, "The same variant holding alternative 2.", "std::variant<int32_t, std::string, uint8_t>", "variant3", a, Variant3(uint8_t(5)));
	vec("key_range", g, "KeyRange (Standalone<KeyRangeRef>): begin then end, each a StringRef.", "KeyRange", "key_range", a, KeyRange(KeyRangeRef(sref("a"), sref("b\x00"s))));
	{
		Standalone<VectorRef<KeyValueRef>> kvs;
		kvs.push_back_deep(kvs.arena(), KeyValueRef(sref("k1"), sref("v1")));
		kvs.push_back_deep(kvs.arena(), KeyValueRef(sref("k2"), sref("")));
		vec("vector_ref_key_value", g, "VectorRef<KeyValueRef>: a uint32_t count, then each key and value.", "Standalone<VectorRef<KeyValueRef>>", "vec<key_value>", a, kvs);
	}
	{
		Standalone<VectorRef<StringRef>> names;
		names.push_back_deep(names.arena(), sref("one"));
		names.push_back_deep(names.arena(), sref("two"));
		vec("vector_ref_string_ref", g, "VectorRef<StringRef>.", "Standalone<VectorRef<StringRef>>", "vec<bytes>", a, names);
	}
}

void versionVectors() {
	const std::string g = "versioning";
	// The same values under each version option: the bytes only differ by the prefix.
	vec("include_version_u32", g, "IncludeVersion(): the current protocol version as a uint64_t, then the value.", "uint32_t", "u32", include(), uint32_t(0x01020304));
	vec("include_version_vec", g, "IncludeVersion() before a std::vector<int32_t>.", "std::vector<int32_t>", "vec<i32>", include(), std::vector<int32_t>{ 1, 2 });
	vec("include_version_flagged_struct", g, "IncludeVersion(v) with the object-serializer flag set on v, before a struct of two integers; the flag does not change how a type without a file identifier is read.", "Point3", "point3", include(ProtocolVersion(currentProtocolVersion().versionWithFlags() | ProtocolVersion::objectSerializerFlag)), Point3(1, 2, "f"));
	vec("assume_version_vec", g, "AssumeVersion(): the same vector with no prefix.", "std::vector<int32_t>", "vec<i32>", assume(), std::vector<int32_t>{ 1, 2 });
	vec("unversioned_u32", g, "Unversioned(): the same integer with no prefix.", "uint32_t", "u32", unversioned(), uint32_t(0x01020304));
}

// SWVersion is the smallest FDB type that is stored behind an IncludeVersion prefix with the
// object-serializer flag: the prefix is binary, the rest a flatbuffers-dialect message. (A
// \xff/serverList value is the same wrapper around a StorageServerInterface.)
void objectPayloadVectors() {
	const ProtocolVersion v = currentProtocolVersion();
	SWVersion sw(v, ProtocolVersion(v.version() - 0x1000000), ProtocolVersion(0x0FDB00B073000000ULL));
	Value value = swVersionValue(sw);
	SWVersion back = decodeSWVersionValue(value);
	if (back.newestProtocolVersion() != sw.newestProtocolVersion() ||
	    back.lastRunProtocolVersion() != sw.lastRunProtocolVersion() ||
	    back.lowestCompatibleProtocolVersion() != sw.lowestCompatibleProtocolVersion()) {
		throw std::runtime_error("swVersionValue does not round trip");
	}
	// Read the prefix with BinaryReader, as decodeServerListValue does.
	BinaryReader rd(value, IncludeVersion());
	if (!rd.protocolVersion().hasObjectSerializerFlag()) {
		throw std::runtime_error("swVersionValue has no object-serializer flag");
	}
	Json logical = Json::object()
	                   .set("newest_protocol_version", Json::uinteger(sw.newestProtocolVersion()))
	                   .set("last_run_protocol_version", Json::uinteger(sw.lastRunProtocolVersion()))
	                   .set("lowest_compatible_protocol_version", Json::uinteger(sw.lowestCompatibleProtocolVersion()))
	                   .set("file_identifier", Json::uinteger(SWVersion::file_identifier))
	                   .set("prefix", Json::uinteger(rd.protocolVersion().versionWithFlags()))
	                   .set("payload", Json::string(hex(StringRef(value).substr(sizeof(uint64_t)))));
	addPositive("sw_version_value",
	            "usage",
	            "swVersionValue(SWVersion): ObjectWriter::toValue(swVersion, IncludeVersion(version with the "
	            "object-serializer flag)): an 8-byte binary prefix, then a flatbuffers-dialect SWVersion table "
	            "(the bytes of the payload depend on the C++ compiler; compare decoded fields).",
	            "SWVersion",
	            "sw_version_value",
	            include(ProtocolVersion(v.versionWithFlags() | ProtocolVersion::objectSerializerFlag)),
	            value,
	            std::move(logical));
}

void connectPacketVectors() {
	const ProtocolVersion v = [] {
		ProtocolVersion p = currentProtocolVersion();
		p.addObjectSerializerFlag();
		return p;
	}();
	auto add = [&](const std::string& name, const std::string& description, ConnectPacket pkt) {
		pkt.connectPacketLength = sizeof(pkt) - sizeof(pkt.connectPacketLength); // prependConnectPacket()
		pkt.protocolVersion = v;
		// Written the way prependConnectPacket() does: a PacketWriter over a PacketBuffer.
		PacketBuffer* first = PacketBuffer::create();
		PacketWriter wr(first, nullptr, Unversioned());
		pkt.serialize(wr);
		PacketBuffer* end = wr.finish();
		if (end != first) {
			throw std::runtime_error("a connect packet does not fit in one PacketBuffer");
		}
		Standalone<StringRef> written{ StringRef(first->data(), first->bytes_written) };
		first->delref();
		// Read it the way connectionReader does: BinaryReader(AssumeVersion(protocolVersion)).
		auto protocolVersion = ((ConnectPacket*)written.begin())->protocolVersion;
		BinaryReader pktReader(written.begin(), written.size(), AssumeVersion(protocolVersion));
		ConnectPacket back;
		serializer(pktReader, back);
		pktReader.assertEnd();
		if (written.size() != 44 || back.totalPacketSize() != 44 || memcmp(&back, &pkt, sizeof(pkt)) != 0) {
			throw std::runtime_error("connect packet " + name + " does not round trip");
		}
		Json logical =
		    Json::object()
		        .set("connect_packet_length", Json::uinteger(pkt.connectPacketLength))
		        .set("protocol_version", Json::uinteger(pkt.protocolVersion.versionWithFlags()))
		        .set("canonical_remote_port", Json::uinteger(pkt.canonicalRemotePort))
		        .set("connection_id", Json::uinteger(pkt.connectionId))
		        .set("canonical_remote_ip4", Json::uinteger(pkt.canonicalRemoteIp4))
		        .set("flags", Json::uinteger(pkt.flags))
		        .set("canonical_remote_ip6", Json::string(hex(pkt.canonicalRemoteIp6, sizeof(pkt.canonicalRemoteIp6))))
		        .set("canonical_remote_ip", pkt.canonicalRemotePort ? Json::string(pkt.canonicalRemoteIp().toString()) : Json::null())
		        .set("total_packet_size", Json::uinteger(pkt.totalPacketSize()));
		addPositive(name, "usage", description, "ConnectPacket", "connect_packet", unversioned(), written, std::move(logical));
	};
	ConnectPacket client;
	client.connectionId = 1;
	client.setCanonicalRemoteIp(IPAddress(0));
	add("connect_packet_client",
	    "A client's ConnectPacket (Peer::prependConnectPacket for a mixed TLS/non-TLS connection): port 0, address 0, "
	    "connectionId 1.",
	    client);
	ConnectPacket v4;
	v4.canonicalRemotePort = 4500;
	v4.connectionId = 0x0123456789abcdefULL;
	v4.setCanonicalRemoteIp(IPAddress(0x0a000102));
	add("connect_packet_ipv4", "A server's ConnectPacket listening on 10.0.1.2:4500.", v4);
	ConnectPacket v6;
	v6.canonicalRemotePort = 4501;
	IPAddress::IPAddressStore a6{};
	a6[0] = 0xfd;
	a6[1] = 0xdb;
	a6[15] = 1;
	v6.setCanonicalRemoteIp(IPAddress(a6));
	add("connect_packet_ipv6", "A ConnectPacket listening on an IPv6 address: the FLAG_IPV6 bit and 16 address bytes.", v6);

	// ConnectPacket::serialize rejects a length above sizeof(ConnectPacket) - 4 before reading the rest.
	std::string tooLong = bytes({ 41, 0, 0, 0 }) + std::string(41, '\0');
	rejects("connect_packet_length_41",
	        "A ConnectPacket whose connectPacketLength is 41: serialize() throws serialization_failed.",
	        "connect_packet",
	        unversioned(),
	        tooLong,
	        [&](const Standalone<StringRef>& s) {
		        BinaryReader r(s.begin(), s.size(), AssumeVersion(currentProtocolVersion()));
		        ConnectPacket p;
		        serializer(r, p);
	        });
	std::string truncated(44 - 3, '\0');
	truncated[0] = 40;
	rejects("connect_packet_truncated",
	        "A 41-byte ConnectPacket that declares a length of 40: the reader runs out of bytes.",
	        "connect_packet",
	        unversioned(),
	        truncated,
	        [&](const Standalone<StringRef>& s) {
		        BinaryReader r(s.begin(), s.size(), AssumeVersion(currentProtocolVersion()));
		        ConnectPacket p;
		        serializer(r, p);
	        });
}

void conflictKeyVectors() {
	// Transaction::makeSelfConflicting (NativeAPI), with a fixed UID instead of a random one.
	BinaryWriter wr(Unversioned());
	wr.serializeBytes("\xFF/SC/"_sr);
	wr << uid(0x5c);
	Standalone<StringRef> k = wr.toValue();
	addPositive("self_conflict_key",
	            "usage",
	            "Transaction::makeSelfConflicting(): \\xff/SC/ followed by a UID, written with BinaryWriter(Unversioned()).",
	            "Key",
	            "self_conflict_key",
	            unversioned(),
	            k,
	            Json::object().set("unique_id", toJson(uid(0x5c))));
}

void spanVectors() {
	auto add = [&](const std::string& name, const std::string& description, SpanContext span) {
		Standalone<StringRef> value = BinaryWriter::toValue(span, IncludeVersion());
		if (value.size() != 33) {
			throw std::runtime_error("a SPAN_PARENT value is not 33 bytes");
		}
		// FDBTransactionOptions::SPAN_PARENT reads it like this.
		SpanContext back = BinaryReader::fromStringRef<SpanContext>(value, IncludeVersion());
		if (back.traceID != span.traceID || back.spanID != span.spanID || back.m_Flags != span.m_Flags) {
			throw std::runtime_error("span context does not round trip");
		}
		Json logical = Json::object()
		                   .set("trace_id", toJson(span.traceID))
		                   .set("span_id", Json::uinteger(span.spanID))
		                   .set("flags", Json::uinteger(static_cast<uint8_t>(span.m_Flags)));
		addPositive(name, "usage", description, "SpanContext", "span_context", include(), value, std::move(logical));
	};
	add("span_context_sampled",
	    "The value of the SPAN_PARENT transaction option: BinaryWriter::toValue(SpanContext, IncludeVersion()), 33 bytes.",
	    SpanContext(UID(0x7ace000000000001ULL, 0x7ace100000000002ULL), 0x5ba0000000000003ULL, TraceFlags::sampled));
	add("span_context_unsampled",
	    "An unsampled SpanContext.",
	    SpanContext(UID(1, 2), 3, TraceFlags::unsampled));
}

void healthMetricsVectors() {
	auto add = [&](const std::string& name, const std::string& description, const HealthMetrics& hm) {
		// GetHealthMetricsReply's constructor serializes the metrics into `serialized` with
		// BinaryWriter(IncludeVersion()); its serialize() reads them back with BinaryReader.
		GetHealthMetricsReply reply(hm);
		Standalone<StringRef> blob = reply.serialized;
		HealthMetrics back = BinaryReader::fromStringRef<HealthMetrics>(blob, IncludeVersion());
		if (!(back == reply.healthMetrics) || back.tpsLimit != hm.tpsLimit) {
			throw std::runtime_error("health metrics do not round trip");
		}
		Json stats = Json::array();
		for (const auto& [id, s] : hm.storageStats) {
			stats.push(Json::array().push(toJson(id)).push(Json::object()
			                                                  .set("storage_queue", Json::integer(s.storageQueue))
			                                                  .set("storage_durability_lag", Json::integer(s.storageDurabilityLag))
			                                                  .set("disk_usage", Json::number(s.diskUsage))
			                                                  .set("cpu_usage", Json::number(s.cpuUsage))));
		}
		Json logical = Json::object()
		                   .set("worst_storage_queue", Json::integer(hm.worstStorageQueue))
		                   .set("limiting_storage_queue", Json::integer(hm.limitingStorageQueue))
		                   .set("worst_storage_durability_lag", Json::integer(hm.worstStorageDurabilityLag))
		                   .set("limiting_storage_durability_lag", Json::integer(hm.limitingStorageDurabilityLag))
		                   .set("worst_t_log_queue", Json::integer(hm.worstTLogQueue))
		                   .set("tps_limit", Json::number(hm.tpsLimit))
		                   .set("batch_limited", Json::boolean(hm.batchLimited))
		                   .set("storage_stats", stats)
		                   .set("t_log_queue", toJson(hm.tLogQueue));
		addPositive(name, "usage", description, "HealthMetrics", "health_metrics", include(), blob, std::move(logical));
	};
	HealthMetrics summary;
	summary.worstStorageQueue = 1000;
	summary.limitingStorageQueue = 2000;
	summary.worstStorageDurabilityLag = 3000;
	summary.limitingStorageDurabilityLag = 4000;
	summary.worstTLogQueue = 5000;
	summary.tpsLimit = 123456.5;
	summary.batchLimited = true;
	add("health_metrics_summary",
	    "GetHealthMetricsReply::serialized without per-server detail: HealthMetrics behind an IncludeVersion() prefix, "
	    "carried as a string inside the flatbuffers reply.",
	    summary);
	HealthMetrics detailed = summary;
	HealthMetrics::StorageStats s1;
	s1.storageQueue = 11;
	s1.storageDurabilityLag = 12;
	s1.diskUsage = 0.25;
	s1.cpuUsage = 0.5;
	HealthMetrics::StorageStats s2;
	s2.storageQueue = -1;
	s2.diskUsage = 1.0;
	detailed.storageStats[uid(2)] = s2;
	detailed.storageStats[uid(1)] = s1;
	detailed.tLogQueue[uid(9)] = 99;
	add("health_metrics_detailed", "HealthMetrics with storage and log server details.", detailed);
}

void idempotencyVectors() {
	// IdempotencyIdKVBuilder::add writes: a timestamp (int64_t(now()), here fixed so that the
	// bytes are reproducible) once, then for each id a uint8_t length, the id and the low byte of
	// the batch index. buildAndClear() stores it under makeIdempotencySingleKeyRange(...).begin.
	auto add = [&](const std::string& name,
	               const std::string& description,
	               Version commitVersion,
	               int64_t timestamp,
	               const std::vector<std::pair<std::string, uint16_t>>& ids) {
		BinaryWriter value{ IncludeVersion() };
		uint8_t high = ids[0].second >> 8;
		value << timestamp;
		for (const auto& [id, batchIndex] : ids) {
			if ((batchIndex >> 8) != high) {
				throw std::runtime_error("ids of one record share the batch index high byte");
			}
			value << uint8_t(id.size());
			value.serializeBytes(StringRef(id));
			value << uint8_t(batchIndex);
		}
		Arena arena;
		Value stored = value.toValue();
		KeyValueRef kv(makeIdempotencySingleKeyRange(arena, commitVersion, high).begin, stored);
		Version decodedVersion;
		uint8_t decodedHigh;
		decodeIdempotencyKey(kv.key, decodedVersion, decodedHigh);
		if (decodedVersion != commitVersion || decodedHigh != high) {
			throw std::runtime_error("idempotency key does not round trip");
		}
		Json entries = Json::array();
		Json found = Json::array();
		for (const auto& [id, batchIndex] : ids) {
			entries.push(Json::object().set("id", Json::string(hex(id))).set("low_order_batch_index", Json::uinteger(batchIndex & 0xff)));
			// kvContainsIdempotencyId is what determineCommitStatus calls on each record.
			Optional<CommitResult> r = kvContainsIdempotencyId(kv, IdempotencyIdRef(StringRef(id)));
			if (!r.present() || r.get().commitVersion != commitVersion || r.get().batchIndex != batchIndex) {
				throw std::runtime_error("kvContainsIdempotencyId does not find its id");
			}
			found.push(Json::object()
			               .set("id", Json::string(hex(id)))
			               .set("commit_version", Json::integer(r.get().commitVersion))
			               .set("batch_index", Json::uinteger(r.get().batchIndex)));
		}
		Json logical = Json::object()
		                   .set("protocol_version", Json::uinteger(currentProtocolVersion().versionWithFlags()))
		                   .set("timestamp", Json::integer(timestamp))
		                   .set("entries", entries)
		                   .set("key", Json::string(hex(kv.key)))
		                   .set("commit_version", Json::integer(commitVersion))
		                   .set("high_order_batch_index", Json::uinteger(high))
		                   .set("found", found);
		addPositive(name, "usage", description, "IdempotencyIdKVBuilder record", "idempotency_value", include(),
		            stored, std::move(logical));
	};
	std::string id1(16, '\x11');
	std::string id2 = std::string(20, '\x22');
	add("idempotency_record_single",
	    "A record with one 16-byte id: IncludeVersion(), the timestamp, then length, id and batch index.",
	    6000000000500LL,
	    1700000000,
	    { { id1, 0x0105 } });
	add("idempotency_record_multi",
	    "A record with two ids of different lengths in one commit batch.",
	    7000000000123LL,
	    1700000123,
	    { { id1, 0x0201 }, { id2, 0x02ff } });

	// determineCommitStatus: the key range to scan for the possible commit versions.
	Version minV = 6000000000000LL, maxV = 6000000099999LL;
	Standalone<StringRef> begin = BinaryWriter::toValue(bigEndian64(minV), Unversioned()).withPrefix(idempotencyIdKeys.begin);
	Standalone<StringRef> end = BinaryWriter::toValue(bigEndian64(maxV + 1), Unversioned()).withPrefix(idempotencyIdKeys.begin);
	Json logical = Json::object()
	                   .set("min_commit_version", Json::integer(minV))
	                   .set("max_commit_version", Json::integer(maxV))
	                   .set("begin", Json::string(hex(begin)))
	                   .set("end", Json::string(hex(end)));
	addPositive("idempotency_scan_range",
	            "usage",
	            "The range determineCommitStatus reads: the idempotencyIdKeys prefix and bigEndian64(version) written "
	            "with BinaryWriter(Unversioned()). `bytes` is the begin key; the range's two keys are in `value`.",
	            "KeyRange",
	            "idempotency_scan_range",
	            unversioned(),
	            Standalone<StringRef>(begin),
	            std::move(logical));
}

void keyServersVectors() {
	auto add = [&](const std::string& name,
	               const std::string& description,
	               std::vector<UID> src,
	               std::vector<UID> dest,
	               UID srcId,
	               UID destId) {
		Value value = keyServersValue(src, dest, srcId, destId);
		std::vector<UID> src2, dest2;
		UID srcId2, destId2;
		decodeKeyServersValue(RangeResult(), value, src2, dest2, srcId2, destId2);
		if (src2 != src || dest2 != dest || srcId2 != srcId || destId2 != destId) {
			throw std::runtime_error("keyServersValue does not round trip");
		}
		BinaryReader rd(value, IncludeVersion());
		Json logical = Json::object()
		                   .set("protocol_version", Json::uinteger(rd.protocolVersion().versionWithFlags()))
		                   .set("src", toJson(src))
		                   .set("dest", toJson(dest))
		                   .set("src_id", toJson(srcId))
		                   .set("dest_id", toJson(destId));
		addPositive(name, "usage", description, "keyServersValue", "key_servers_value",
		            include(ProtocolVersion::withShardEncodeLocationMetaData()), value, std::move(logical));
	};
	add("key_servers_value_stable",
	    "A \\xff/keyServers value for a shard that is not moving: IncludeVersion(withShardEncodeLocationMetaData()), "
	    "vector<UID> src, vector<UID> dest (empty), UID srcId, and no destId.",
	    { uid(1), uid(2), uid(3) },
	    {},
	    uid(10),
	    UID());
	add("key_servers_value_moving",
	    "A value for a shard that is moving: both ids follow.",
	    { uid(1), uid(2) },
	    { uid(4), uid(5) },
	    uid(10),
	    uid(11));
}

void negativeVectors() {
	const Mode u = unversioned();
	const Mode a = assume();
	rejectsAs<uint32_t>("neg_u32_truncated", "Three bytes where a uint32_t needs four.", "u32", u, bytes({ 1, 2, 3 }));
	rejectsAs<std::string>("neg_string_body_truncated", "A std::string of length 5 with two bytes.", "string", a, bytes({ 5, 0, 0, 0, 'a', 'b' }));
	rejectsAs<std::vector<int32_t>>("neg_vec_elements_truncated", "A std::vector<int32_t> of count 3 with one element.", "vec<i32>", a, bytes({ 3, 0, 0, 0, 1, 0, 0, 0 }));
	rejectsAs<std::vector<int32_t>>("neg_vec_count_only_truncated", "A count with only 3 of its 4 bytes.", "vec<i32>", a, bytes({ 3, 0, 0 }));
	rejectsAs<Optional<int32_t>>("neg_optional_value_missing", "An Optional that says present and has no value.", "optional<i32>", a, bytes({ 1 }));
	rejectsAs<Variant3>("neg_variant_index_out_of_range", "A std::variant index of 3 for 3 alternatives: load() asserts.", "variant3", a, bytes({ 3, 0, 0, 0, 0 }));
	rejectsAs<Variant3>("neg_variant_value_truncated", "A variant holding an int32_t with two bytes.", "variant3", a, bytes({ 0, 1, 0 }));
	rejectsAs<UID>("neg_uid_truncated", "A UID with 15 bytes.", "uid", u, std::string(15, '\1'));
	// IncludeVersion: the reader checks the prefix.
	const Mode inc = include();
	rejectsAs<uint32_t>("neg_include_version_truncated_prefix", "An IncludeVersion() input shorter than the 8-byte prefix.", "u32", inc, bytes({ 0, 0, 0, 0xb0 }));
	rejectsAs<uint32_t>("neg_include_version_invalid",
	                    "An IncludeVersion() prefix below the minimum valid protocol version: incompatible_protocol_version.",
	                    "u32",
	                    inc,
	                    bytes({ 0x00, 0x00, 0x00, 0x00, 0xa1, 0x00, 0xdb, 0x0f, 1, 2, 3, 4 }));
	{
		// minInvalidProtocolVersion is the first version a reader of this release refuses.
		uint64_t minInvalid = minInvalidProtocolVersion.versionWithFlags();
		std::string prefix;
		for (int i = 0; i < 8; ++i) {
			prefix += static_cast<char>((minInvalid >> (8 * i)) & 0xff);
		}
		rejectsAs<uint32_t>("neg_include_version_future",
		                    "An IncludeVersion() prefix equal to this release's minInvalidProtocolVersion: "
		                    "incompatible_protocol_version.",
		                    "u32",
		                    inc,
		                    prefix + bytes({ 1, 2, 3, 4 }));
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
	primitiveVectors();
	containerVectors();
	versionVectors();
	objectPayloadVectors();
	connectPacketVectors();
	conflictKeyVectors();
	spanVectors();
	healthMetricsVectors();
	idempotencyVectors();
	keyServersVectors();
	negativeVectors();

	const std::string dir = out + "/binary-" GV_RELEASE_NAME;
	std::filesystem::create_directories(dir + "/negative");
	Json index = Json::object();
	index.set("format_version", Json::integer(kFormatVersion));
	index.set("generator", Json::string("tools/golden-vectors"));
	index.set("fdb_source", Json::string(fdbRev));
	index.set("fdb_version", Json::string(GV_FDB_VERSION));
	const char* image = getenv("GV_BUILD_IMAGE");
	index.set("build_image", Json::string(image ? image : "unknown"));
	index.set("current_protocol_version", Json::uinteger(currentProtocolVersion().versionWithFlags()));
	index.set("min_invalid_protocol_version", Json::uinteger(minInvalidProtocolVersion.versionWithFlags()));
	index.set("note",
	          Json::string("Generated by tools/golden-vectors/regen_messages.sh from this release's own BinaryWriter "
	                       "and types. Do not edit by hand. The legacy binary format has no padding or vtables, so the "
	                       "bytes are canonical; the one flatbuffers payload (sw_version_value) is compared by its "
	                       "decoded fields."));
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
	fprintf(stderr, "wrote %zu positive and %zu negative binary vectors to %s\n", positives().size(), negatives().size(), dir.c_str());
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
		fprintf(stderr, "usage: golden_vectors_binary generate --out DIR [--fdb-rev REV]\n");
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
