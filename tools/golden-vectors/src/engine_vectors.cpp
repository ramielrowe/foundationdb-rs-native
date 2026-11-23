// Engine-level golden vectors: test message types modelled on the unit tests in
// flow/flat_buffers.cpp, and the catalog of fixtures built from them.
#include <climits>
#include <limits>

#include "fixtures.h"

// Declares a table: its name in dumps and its file identifier.
#define TABLE(Name, FID)                                                                                               \
	static constexpr const char* type_name = #Name;                                                                    \
	constexpr static FileIdentifier file_identifier = FID;

// Declares serialize() and records the argument names for dumps. The argument
// order is the vtable slot order.
#define FIELDS(...)                                                                                                    \
	static constexpr const char* field_names = #__VA_ARGS__;                                                           \
	template <class Ar>                                                                                                \
	void serialize(Ar& ar) {                                                                                           \
		serializer(ar, __VA_ARGS__);                                                                                   \
	}

namespace {

// File identifiers for the test types: 0x0fdbNN, all below 2^24 as FDB requires.
constexpr FileIdentifier fid(int n) {
	return 0x0fdb00 + n;
}

// Memory for StringRef/VectorRef test values.
Arena& valueArena() {
	static Arena a;
	return a;
}
StringRef ref(const std::string& s) {
	return StringRef(valueArena(), s);
}

// --- scalars ---------------------------------------------------------------
struct Scalars {
	TABLE(Scalars, fid(1))
	bool b = false;
	uint8_t u8 = 0;
	int8_t i8 = 0;
	uint16_t u16 = 0;
	int16_t i16 = 0;
	uint32_t u32 = 0;
	int32_t i32 = 0;
	uint64_t u64 = 0;
	int64_t i64 = 0;
	float f32 = 0;
	double f64 = 0;
	FIELDS(b, u8, i8, u16, i16, u32, i32, u64, i64, f32, f64)
};

// --- strings ---------------------------------------------------------------
struct Strings {
	TABLE(Strings, fid(2))
	std::string s;
	std::string empty;
	StringRef ref;
	StringRef emptyRef;
	std::string binary;
	std::string len4;
	FIELDS(s, empty, ref, emptyRef, binary, len4)
};

// --- vectors ---------------------------------------------------------------
struct Small {
	TABLE(Small, fid(3))
	int32_t x = 0;
	std::string name;
	FIELDS(x, name)
	bool operator==(const Small&) const = default;
};

struct Vectors {
	TABLE(Vectors, fid(4))
	std::vector<uint8_t> v_u8;
	std::vector<uint16_t> v_u16;
	std::vector<int32_t> v_i32;
	std::vector<uint64_t> v_u64;
	std::vector<double> v_f64;
	std::vector<std::string> v_str;
	std::vector<Small> v_table;
	std::vector<uint32_t> v_empty_u32;
	std::vector<std::string> v_empty_str;
	std::vector<Small> v_empty_table;
	std::vector<bool> v_bool;
	std::array<uint8_t, 16> fixed16{};
	FIELDS(v_u8, v_u16, v_i32, v_u64, v_f64, v_str, v_table, v_empty_u32, v_empty_str, v_empty_table, v_bool, fixed16)
};

struct Containers {
	TABLE(Containers, fid(5))
	std::map<int32_t, std::string> m;
	std::set<uint32_t> s;
	std::vector<std::vector<uint16_t>> nested;
	FIELDS(m, s, nested)
};

// --- nested tables (flow/FlatBuffers/serializeDeserializeRoot) -------------
struct Nested2 {
	TABLE(Nested2, fid(6))
	uint8_t a = 0;
	std::vector<std::string> b;
	int c = 0;
	FIELDS(a, b, c)
	bool operator==(const Nested2&) const = default;
};

struct Nested {
	TABLE(Nested, fid(7))
	uint8_t a = 0;
	std::string b;
	Nested2 nested;
	std::vector<uint64_t> c;
	FIELDS(a, b, nested, c)
};

struct Root {
	TABLE(Root, fid(8))
	uint8_t a = 0;
	std::vector<Nested2> b;
	Nested c;
	FIELDS(a, b, c)
};

// --- structs (std::tuple is the only struct-like type in flow) -------------
using Struct3 = std::tuple<int16_t, bool, int64_t>;
using Struct2 = std::tuple<uint8_t, uint32_t>;

struct Structs {
	TABLE(Structs, fid(9))
	uint8_t tag = 0;
	Struct3 inline3{};
	Struct2 inline2{};
	std::vector<Struct3> many;
	uint16_t after = 0;
	FIELDS(tag, inline3, inline2, many, after)
};

// --- optionals --------------------------------------------------------------
struct Inner {
	TABLE(Inner, fid(10))
	uint32_t a = 0;
	std::string b;
	FIELDS(a, b)
	bool operator==(const Inner&) const = default;
};

struct Optionals {
	TABLE(Optionals, fid(11))
	Optional<int32_t> int_present;
	Optional<int32_t> int_absent;
	Optional<std::string> str_present;
	Optional<std::string> str_absent;
	Optional<Inner> table_present;
	Optional<Inner> table_absent;
	Optional<double> f64_present;
	Optional<UID> uid_present;
	std::vector<Optional<int32_t>> vec_of_opt;
	FIELDS(int_present, int_absent, str_present, str_absent, table_present, table_absent, f64_present, uid_present,
	       vec_of_opt)
};

// --- variants ---------------------------------------------------------------
using Variant5 = std::variant<int32_t, double, Nested2, std::string, uint8_t>;
struct VariantHolder {
	TABLE(VariantHolder, fid(12))
	uint8_t before = 0;
	Variant5 v;
	uint16_t after = 0;
	FIELDS(before, v, after)
};
using Variant3 = std::variant<int, double, Nested2>; // flow/FlatBuffers/variant

// --- ErrorOr / EnsureTable --------------------------------------------------
struct Payload {
	TABLE(Payload, fid(13))
	int64_t version = 0;
	std::string value;
	Optional<UID> id;
	FIELDS(version, value, id)
	bool operator==(const Payload&) const = default;
};

struct EnsureTables {
	TABLE(EnsureTables, fid(14))
	EnsureTable<int32_t> scalar;
	EnsureTable<Inner> table;
	EnsureTable<std::string> str;
	EnsureTable<std::vector<uint16_t>> vec;
	std::variant<EnsureTable<int32_t>, Inner> wrapped_alt;
	std::variant<int32_t, Inner> bare_alt;
	FIELDS(scalar, table, str, vec, wrapped_alt, bare_alt)
};

// --- request shape: ReplyPromise + Arena ------------------------------------
struct RequestLike {
	TABLE(RequestLike, fid(15))
	StringRef key;
	int64_t version = 0;
	Optional<UID> debugID;
	ReplyPromise<Inner> reply;
	Arena arena;
	FIELDS(key, version, debugID, reply, arena)
};

// --- serializer order != declaration order ----------------------------------
struct OrderDemo {
	TABLE(OrderDemo, fid(16))
	uint8_t a = 0;
	uint64_t b = 0;
	std::string c;
	uint16_t d = 0;
	FIELDS(d, c, a, b)
};

// --- Arena-carrying refs ------------------------------------------------------
struct ArenaHolder {
	TABLE(ArenaHolder, fid(17))
	VectorRef<StringRef> keys;
	StringRef single;
	VectorRef<int64_t> nums;
	Arena arena;
	FIELDS(keys, single, nums, arena)
};

// --- pre-serialized vector ----------------------------------------------------
struct KVHolder {
	TABLE(KVHolder, fid(18))
	PreserializedKVVector data;
	bool more = false;
	FIELDS(data, more)
};

// --- vtable dedup -------------------------------------------------------------
struct DedupA {
	TABLE(DedupA, fid(19))
	int32_t x = 0;
	FIELDS(x)
};
struct DedupB {
	TABLE(DedupB, fid(20))
	uint32_t y = 0;
	FIELDS(y)
};
struct DedupC {
	TABLE(DedupC, fid(21))
	float z = 0;
	FIELDS(z)
};
struct DedupHolder {
	TABLE(DedupHolder, fid(22))
	DedupA a;
	DedupB b;
	DedupC c;
	std::vector<DedupA> va;
	FIELDS(a, b, c, va)
};

// --- schema evolution ---------------------------------------------------------
constexpr FileIdentifier kSchemaFid = fid(23);
struct SchemaV1 {
	TABLE(SchemaV1, kSchemaFid)
	int32_t a = 0;
	std::string b;
	FIELDS(a, b)
};
struct SchemaV2 {
	TABLE(SchemaV2, kSchemaFid)
	int32_t a = 0;
	std::string b;
	uint64_t c = 0;
	Optional<std::string> d;
	std::vector<uint32_t> e;
	FIELDS(a, b, c, d, e)
};

// flow/FlatBuffers/nestedCompat
struct Y1 {
	TABLE(Y1, 338229)
	int a = 0;
	FIELDS(a)
};
struct Y2 {
	TABLE(Y2, fid(24))
	int a = 0;
	std::variant<int> b;
	FIELDS(a, b)
};
template <class Y>
struct X {
	static constexpr const char* type_name = "X";
	int a = 0;
	Y b;
	int c = 0;
	FIELDS(a, b, c)
};

// --- malformed-buffer tests (flow/FlatBuffers/Rejects*) -------------------------
struct Untrusted {
	TABLE(Untrusted, 4242)
	std::string s;
	std::vector<int64_t> v;
	Optional<int> o;
	std::vector<Optional<int>> vo;
	std::vector<bool> b;
	std::vector<std::string> strs;
	FIELDS(s, v, o, vo, b, strs)
};

struct WireBytes {
	TABLE(WireBytes, 4343)
	std::vector<uint8_t> v;
	FIELDS(v)
};
struct FixedBytes {
	TABLE(FixedBytes, 4343)
	std::array<uint8_t, 16> a{};
	FIELDS(a)
};

// ---------------------------------------------------------------------------
uint32_t read32(const std::vector<uint8_t>& b, size_t pos) {
	uint32_t x;
	memcpy(&x, b.data() + pos, 4);
	return x;
}
uint16_t read16(const std::vector<uint8_t>& b, size_t pos) {
	uint16_t x;
	memcpy(&x, b.data() + pos, 2);
	return x;
}
void write32(std::vector<uint8_t>& b, size_t pos, uint32_t x) {
	memcpy(b.data() + pos, &x, 4);
}
void write16(std::vector<uint8_t>& b, size_t pos, uint16_t x) {
	memcpy(b.data() + pos, &x, 2);
}

// Byte position of the vtable of the table that root slot 0 points to.
size_t first_member_vtable(const std::vector<uint8_t>& b) {
	size_t wrapper = read32(b, 0);
	size_t wrapperVt = wrapper - (int32_t)read32(b, wrapper);
	size_t slotPos = wrapper + read16(b, wrapperVt + 4);
	size_t table = slotPos + read32(b, slotPos);
	return table - (int32_t)read32(b, table);
}

Json mutation_u32(size_t pos, uint32_t value) {
	return Json::object()
	    .set("op", Json::string("overwrite_u32"))
	    .set("pos", Json::uinteger(pos))
	    .set("value", Json::uinteger(value));
}
Json mutation_u16(size_t pos, uint16_t value) {
	return Json::object()
	    .set("op", Json::string("overwrite_u16"))
	    .set("pos", Json::uinteger(pos))
	    .set("value", Json::uinteger(value));
}
Json mutation_truncate(size_t len) {
	return Json::object().set("op", Json::string("truncate")).set("len", Json::uinteger(len));
}

std::string hexstr(uint32_t x) {
	return fid_hex(x);
}

// ===========================================================================
void scalarVectors() {
	Scalars typical;
	typical.b = true;
	typical.u8 = 0xab;
	typical.i8 = -2;
	typical.u16 = 0xbeef;
	typical.i16 = -12345;
	typical.u32 = 0xdeadbeef;
	typical.i32 = -123456789;
	typical.u64 = 0xfedcba9876543210ull;
	typical.i64 = -1234567890123456789ll;
	typical.f32 = 1.5f;
	typical.f64 = -2.25;
	positive("scalars",
	         "scalars_typical",
	         "One field of every scalar width and signedness plus bool, f32, f64. Physical placement in the table is "
	         "a stable sort by size descending (u64, i64, f64, then the 4-byte, 2-byte and 1-byte fields), not slot "
	         "order.",
	         "Scalars",
	         Scalars::file_identifier,
	         typical);

	Scalars ext;
	ext.b = false;
	ext.u8 = 255;
	ext.i8 = -128;
	ext.u16 = 65535;
	ext.i16 = -32768;
	ext.u32 = UINT32_MAX;
	ext.i32 = INT32_MIN;
	ext.u64 = UINT64_MAX;
	ext.i64 = INT64_MIN;
	ext.f32 = -0.0f;
	ext.f64 = 1e308;
	positive("scalars",
	         "scalars_extremes",
	         "Extreme values: unsigned maxima, signed minima, f32 negative zero, f64 1e308.",
	         "Scalars",
	         Scalars::file_identifier,
	         ext);

	positive("scalars",
	         "scalars_zero",
	         "All-zero scalars. Default values are still written: FDB never omits a field from the vtable.",
	         "Scalars",
	         Scalars::file_identifier,
	         Scalars{});

	positive("scalars",
	         "root_scalar_i32",
	         "A bare int32 as the message root: the synthetic root table holds the scalar inline in slot 0 "
	         "(file identifier supplied explicitly, int has none).",
	         "int32_t",
	         fid(100),
	         int32_t(0x01020304));
}

void stringVectors() {
	Strings s;
	s.s = "hello";
	s.empty = "";
	s.ref = ref("abc");
	s.emptyRef = StringRef();
	s.binary = std::string("\x00\x01\xff\xfe\x7f", 5);
	s.len4 = "abcd";
	positive("strings",
	         "strings",
	         "std::string and StringRef share one wire form: u32 length, bytes, zero padding to 4, no NUL terminator. "
	         "All empty strings/vectors in a message share one encoded empty blob (the first one written).",
	         "Strings",
	         Strings::file_identifier,
	         s);
}

void vectorVectors() {
	Vectors v;
	v.v_u8 = { 1, 2, 3 };
	v.v_u16 = { 0x1111, 0x2222, 0x3333 };
	v.v_i32 = { -1, 0, 1, INT32_MAX };
	v.v_u64 = { 0x0102030405060708ull, 9 };
	v.v_f64 = { 0.5, -1.0 };
	v.v_str = { "a", "", "bcdef" };
	v.v_table = { Small{ 1, "one" }, Small{ -2, "" } };
	v.v_bool = { true, false, true, true, false };
	for (int i = 0; i < 16; ++i) {
		v.fixed16[i] = 0xa0 + i;
	}
	positive("vectors",
	         "vectors",
	         "Vectors of scalars of each width, strings, tables; empty vectors of scalars, strings and tables; "
	         "std::vector<bool> (one byte per element); std::array<u8,16> (same wire form as a vector; the reader "
	         "rejects a length other than 16). Vector element data is aligned to max(4, element alignment) with "
	         "the u32 count immediately before it.",
	         "Vectors",
	         Vectors::file_identifier,
	         v);

	Containers c;
	c.m = { { 3, "three" }, { -1, "minus one" } };
	c.s = { 30, 10, 20 };
	c.nested = { { 1, 2 }, {}, { 3 } };
	positive("vectors",
	         "containers",
	         "std::map is a vector of pair tables {first, second} in key order; std::set is a vector of its keys in "
	         "order; a vector of vectors is a vector of offsets.",
	         "Containers",
	         Containers::file_identifier,
	         c);

	std::vector<bool> bools = { true, false, true, false, true };
	positive("vectors",
	         "root_vector_bool",
	         "flow/FlatBuffers/vectorBool: std::vector<bool> as the root member.",
	         "std::vector<bool>",
	         fid(101),
	         bools);
}

Root unitTestRoot() {
	Root root;
	root.a = 1;
	root.b = { Nested2{ 13, { "ghi", "jkl" }, 15 }, Nested2{ 16, { "mnop", "qrstuv" }, 18 } };
	root.c.a = 3;
	root.c.b = "hello";
	root.c.nested = Nested2{ 6, { "abc", "def" }, 8 };
	root.c.c = { 10, 11, 12 };
	return root;
}

void nestedVectors() {
	Root root = unitTestRoot();
	positive("nested",
	         "nested_root",
	         "flow/FlatBuffers/serializeDeserializeRoot: tables nested three deep, a vector of tables, vectors of "
	         "strings and u64.",
	         "Root",
	         Root::file_identifier,
	         root);
	positive("nested",
	         "root_multiple_members",
	         "flow/FlatBuffers/serializeDeserializeMembers: save_members(a, b, c) with three root members. The "
	         "synthetic root table has one slot per member.",
	         "RootMembers",
	         fid(102),
	         root.a,
	         root.b,
	         root.c);

	Structs st;
	st.tag = 7;
	st.inline3 = { -5, true, 0x1122334455667788ll };
	st.inline2 = { 0xee, 0xaabbccdd };
	st.many = { { 1, true, 2 }, { 3, false, 4 } };
	st.after = 0x4242;
	positive("nested",
	         "structs",
	         "Structs (std::tuple) stored inline in a table and in a vector. Struct members are laid out in "
	         "declaration order, each aligned to its own size, total size rounded up to the largest member "
	         "alignment; struct<i16,bool,i64> is 16 bytes, struct<u8,u32> is 8 bytes.",
	         "Structs",
	         Structs::file_identifier,
	         st);

	std::vector<Struct3> rootStructs = { { 1, true, 2 }, { 3, false, 4 } };
	positive("nested",
	         "root_vector_of_structs",
	         "flow/FlatBuffers/struct: a vector of struct<i16,bool,i64> as the root member.",
	         "std::vector<std::tuple<int16_t,bool,int64_t>>",
	         fid(103),
	         rootStructs);
}

void optionalVectors() {
	Optionals o;
	o.int_present = 42;
	o.str_present = std::string("opt");
	o.table_present = Inner{ 9, "inner" };
	o.f64_present = 3.5;
	o.uid_present = UID(0x0123456789abcdefull, 0xfedcba9876543210ull);
	o.vec_of_opt = { Optional<int32_t>(1), Optional<int32_t>(), Optional<int32_t>(3) };
	positive("unions",
	         "optionals",
	         "Optional<T> is a one-alternative union: two slots (u8 tag, u32 offset). Absent is tag 0 with a zero "
	         "offset. Present is tag 1; scalar payloads (i32, f64, UID) are stored out of line and the offset "
	         "points at the raw scalar bytes; string/table payloads are pointed to directly. A vector of Optional "
	         "is a union vector: two slots holding a vector<u8> of tags and a vector of offsets.",
	         "Optionals",
	         Optionals::file_identifier,
	         o);
}

void variantVectors() {
	auto holder = [](Variant5 v) {
		VariantHolder h;
		h.before = 0x11;
		h.v = std::move(v);
		h.after = 0x2222;
		return h;
	};
	positive("unions",
	         "variant_i32",
	         "std::variant alternative 0 (i32): tag 1, offset to 4 out-of-line bytes.",
	         "VariantHolder",
	         VariantHolder::file_identifier,
	         holder(Variant5(std::in_place_index<0>, -7)));
	positive("unions",
	         "variant_f64",
	         "std::variant alternative 1 (f64): tag 2, offset to 8 out-of-line bytes. The out-of-line scalar is "
	         "not padded or aligned beyond what precedes it.",
	         "VariantHolder",
	         VariantHolder::file_identifier,
	         holder(Variant5(std::in_place_index<1>, 6.25)));
	positive("unions",
	         "variant_table",
	         "std::variant alternative 2 (table Nested2): tag 3, offset to the table.",
	         "VariantHolder",
	         VariantHolder::file_identifier,
	         holder(Variant5(std::in_place_index<2>, Nested2{ 1, { "abc", "def" }, 2 })));
	positive("unions",
	         "variant_blob",
	         "std::variant alternative 3 (std::string): tag 4, offset to the length-prefixed blob.",
	         "VariantHolder",
	         VariantHolder::file_identifier,
	         holder(Variant5(std::in_place_index<3>, std::string("blob!"))));
	positive("unions",
	         "variant_u8",
	         "std::variant alternative 4 (u8): tag 5, offset to a single out-of-line byte.",
	         "VariantHolder",
	         VariantHolder::file_identifier,
	         holder(Variant5(std::in_place_index<4>, uint8_t(0x99))));

	positive("unions",
	         "root_variant_i32",
	         "flow/FlatBuffers/variant: a std::variant<int,double,Nested2> as the root member; the root table has "
	         "two slots.",
	         "std::variant<int,double,Nested2>",
	         fid(104),
	         Variant3(1));
	positive("unions",
	         "root_variant_f64",
	         "flow/FlatBuffers/variant with the double alternative.",
	         "std::variant<int,double,Nested2>",
	         fid(104),
	         Variant3(1.0));
	positive("unions",
	         "root_variant_table",
	         "flow/FlatBuffers/variant with the Nested2 alternative.",
	         "std::variant<int,double,Nested2>",
	         fid(104),
	         Variant3(Nested2{ 1, { "abc", "def" }, 2 }));
}

void errorOrVectors() {
	Payload p{ 123456789, "value bytes", Optional<UID>(UID(1, 2)) };
	using EO = ErrorOr<EnsureTable<Payload>>;
	constexpr FileIdentifier eoFid = FileIdentifierFor<EO>::value;
	static_assert(eoFid == ((2u << 24) | Payload::file_identifier));
	positive("error_or",
	         "error_or_success",
	         "A reply as sent on the wire: ErrorOr<EnsureTable<T>> holding T. Union tag 2 (alternative T), offset "
	         "to the T table. File identifier is (2 << 24) | T::file_identifier.",
	         "ErrorOr<EnsureTable<Payload>>",
	         eoFid,
	         EO(EnsureTable<Payload>(p)));
	positive("error_or",
	         "error_or_error",
	         "ErrorOr<EnsureTable<T>> holding an Error (1020 not_committed): union tag 1, offset to an Error "
	         "table with one u16 field (error_code). The vtable block still contains T's vtable.",
	         "ErrorOr<EnsureTable<Payload>>",
	         eoFid,
	         EO(Error(1020)));

	using EOV = ErrorOr<EnsureTable<Void>>;
	constexpr FileIdentifier eovFid = FileIdentifierFor<EOV>::value;
	positive("error_or",
	         "error_or_void_success",
	         "ErrorOr<EnsureTable<Void>> success, the reply to a ReplyPromise<Void>: tag 2, offset to an empty "
	         "table (just its 4-byte vtable offset). File identifier (2 << 24) | 2010442.",
	         "ErrorOr<EnsureTable<Void>>",
	         eovFid,
	         EOV(EnsureTable<Void>(Void())));
	positive("error_or",
	         "error_or_void_broken_promise",
	         "ErrorOr<EnsureTable<Void>> carrying broken_promise (1100).",
	         "ErrorOr<EnsureTable<Void>>",
	         eovFid,
	         EOV(Error(1100)));

	using EOP = ErrorOr<Payload>;
	positive("error_or",
	         "error_or_without_ensure_table",
	         "ErrorOr<Payload> without EnsureTable: for a T that is already a table, EnsureTable changes nothing, so "
	         "the bytes equal error_or_success.",
	         "ErrorOr<Payload>",
	         FileIdentifierFor<EOP>::value,
	         EOP(p));

	EnsureTables et;
	et.scalar = EnsureTable<int32_t>(77);
	et.table = EnsureTable<Inner>(Inner{ 5, "five" });
	et.str = EnsureTable<std::string>(std::string("wrapped"));
	et.vec = EnsureTable<std::vector<uint16_t>>(std::vector<uint16_t>{ 1, 2, 3 });
	et.wrapped_alt = EnsureTable<int32_t>(-3);
	et.bare_alt = int32_t(-3);
	positive("error_or",
	         "ensure_table",
	         "EnsureTable<T>: if T is a table it is transparent; otherwise T is wrapped in a one-slot table "
	         "(the same shape as the root wrapper), so the field holds an offset to that table. Compare "
	         "wrapped_alt (variant alternative EnsureTable<i32>: offset to a table holding the i32) with bare_alt "
	         "(variant alternative i32: offset to the raw 4 bytes).",
	         "EnsureTables",
	         EnsureTables::file_identifier,
	         et);
}

void fileIdentifierVectors() {
	constexpr FileIdentifier x = Inner::file_identifier;
	Inner inner{ 1, "x" };
	auto desc = [&](const std::string& what, FileIdentifier f) {
		return what + ": file identifier " + hexstr(f) + " composed from Inner (" + hexstr(x) + ").";
	};

	positive("file_identifier",
	         "fid_plain",
	         "Plain table Inner: file identifier " + hexstr(x) + " (< 2^24).",
	         "Inner",
	         x,
	         inner);

	static_assert(FileIdentifierFor<ReplyPromise<Inner>>::value == ((1u << 24) | x));
	positive("file_identifier",
	         "fid_reply_promise",
	         desc("ReplyPromise<Inner> (wrapper 1), a table holding the reply endpoint UID token",
	              FileIdentifierFor<ReplyPromise<Inner>>::value),
	         "ReplyPromise<Inner>",
	         FileIdentifierFor<ReplyPromise<Inner>>::value,
	         ReplyPromise<Inner>(UID(0x1111111111111111ull, 0x2222222222222222ull)));

	static_assert(FileIdentifierFor<ErrorOr<Inner>>::value == ((2u << 24) | x));
	positive("file_identifier",
	         "fid_error_or",
	         desc("ErrorOr<Inner> (wrapper 2)", FileIdentifierFor<ErrorOr<Inner>>::value),
	         "ErrorOr<Inner>",
	         FileIdentifierFor<ErrorOr<Inner>>::value,
	         ErrorOr<Inner>(inner));

	VectorRef<Inner> vr;
	vr.push_back(valueArena(), inner);
	static_assert(FileIdentifierFor<VectorRef<Inner>>::value == ((3u << 24) | x));
	positive("file_identifier",
	         "fid_vectorref",
	         desc("VectorRef<Inner> (wrapper 3)", FileIdentifierFor<VectorRef<Inner>>::value),
	         "VectorRef<Inner>",
	         FileIdentifierFor<VectorRef<Inner>>::value,
	         vr);

	static_assert(FileIdentifierFor<Optional<Inner>>::value == ((4u << 24) | x));
	positive("file_identifier",
	         "fid_optional",
	         desc("Optional<Inner> (wrapper 4)", FileIdentifierFor<Optional<Inner>>::value),
	         "Optional<Inner>",
	         FileIdentifierFor<Optional<Inner>>::value,
	         Optional<Inner>(inner));

	static_assert(FileIdentifierFor<std::vector<Inner>>::value == ((5u << 24) | x));
	positive("file_identifier",
	         "fid_std_vector",
	         desc("std::vector<Inner> (wrapper 5)", FileIdentifierFor<std::vector<Inner>>::value),
	         "std::vector<Inner>",
	         FileIdentifierFor<std::vector<Inner>>::value,
	         std::vector<Inner>{ inner });

	// ErrorOr<Optional<Inner>> has file identifier (2 << 28) | (4 << 24) | x but
	// cannot be serialized: a union alternative cannot itself be a union. That is
	// what EnsureTable is for.
	using EOO = ErrorOr<Optional<Inner>>;
	static_assert(FileIdentifierFor<EOO>::value == ((2u << 28) | (4u << 24) | x));
	using EOEO = ErrorOr<EnsureTable<Optional<Inner>>>;
	static_assert(FileIdentifierFor<EOEO>::value == FileIdentifierFor<EOO>::value);
	positive("file_identifier",
	         "fid_error_or_ensure_table_optional",
	         desc("ErrorOr<EnsureTable<Optional<Inner>>>, a two-level composition: the inner wrapper (4, Optional) "
	              "takes bits 24..27 and the outer wrapper (2, ErrorOr) bits 28..31. EnsureTable passes T's "
	              "identifier and composition depth through unchanged. Optional is not a table, so EnsureTable wraps "
	              "it in a one-slot table (a union directly inside a union is not serializable)",
	              FileIdentifierFor<EOEO>::value),
	         "ErrorOr<EnsureTable<Optional<Inner>>>",
	         FileIdentifierFor<EOEO>::value,
	         EOEO(EnsureTable<Optional<Inner>>(Optional<Inner>(inner))));

	static_assert(FileIdentifierFor<VectorRef<StringRef>>::value == ((3u << 24) | 13300811));

	RequestLike req;
	req.key = ref("some/key");
	req.version = 0x0000000100000002ll;
	req.debugID = UID(0xabcull, 0xdefull);
	req.reply = ReplyPromise<Inner>(UID(0x5555555555555555ull, 0x6666666666666666ull));
	positive("file_identifier",
	         "request_like",
	         "A request-shaped message: StringRef, i64, Optional<UID>, ReplyPromise<Inner> (a table holding the "
	         "reply token UID), then Arena (no slot). Mirrors e.g. GetValueRequest's shape.",
	         "RequestLike",
	         RequestLike::file_identifier,
	         req);
}

void orderAndArenaVectors() {
	OrderDemo o;
	o.a = 0xaa;
	o.b = 0xbbbbbbbbbbbbbbbbull;
	o.c = "cc";
	o.d = 0xdddd;
	positive("layout",
	         "serializer_order",
	         "Declaration order is a, b, c, d but serialize() calls serializer(ar, d, c, a, b): slots follow the "
	         "serializer argument order (slot 0 = d, 1 = c, 2 = a, 3 = b).",
	         "OrderDemo",
	         OrderDemo::file_identifier,
	         o);

	ArenaHolder h;
	h.keys.push_back(valueArena(), ref("Foo"));
	h.keys.push_back(valueArena(), ref("Bar"));
	h.single = ref("single");
	h.nums.push_back(valueArena(), -1);
	h.nums.push_back(valueArena(), 1ll << 40);
	positive("layout",
	         "arena_refs",
	         "VectorRef<StringRef>, StringRef and VectorRef<i64> followed by an Arena. The Arena is the last "
	         "serializer argument and takes no vtable slot (its slot is null in the dump).",
	         "ArenaHolder",
	         ArenaHolder::file_identifier,
	         h);

	VectorRef<StringRef> vec;
	vec.push_back(valueArena(), ref("Foo"));
	vec.push_back(valueArena(), ref("Bar"));
	positive("layout",
	         "root_vectorref_stringref",
	         "flow/FlatBuffers/VectorRef: a VectorRef<StringRef> as the root, file identifier (3 << 24) | 13300811.",
	         "VectorRef<StringRef>",
	         FileIdentifierFor<VectorRef<StringRef>>::value,
	         vec);

	KVHolder kv;
	kv.data.items = { { "k1", "v1" }, { "key2", "" }, { "", "value3" } };
	kv.more = true;
	positive("layout",
	         "preserialized_kv_vector",
	         "VectorRef<KeyValueRef, VecSerStrategy::String> (as in GetKeyValuesReply::data) is one blob framed "
	         "like a string: u32 blob length, then u32 count and per item u32 key length, key, u32 value length, "
	         "value; zero padding to 4.",
	         "KVHolder",
	         KVHolder::file_identifier,
	         kv);

	DedupHolder d;
	d.a.x = 1;
	d.b.y = 2;
	d.c.z = 3.0f;
	DedupA va;
	va.x = 4;
	d.va.push_back(va);
	positive("layout",
	         "vtable_dedup",
	         "DedupA{i32}, DedupB{u32}, DedupC{f32} and the root wrapper (one offset) all have the vtable "
	         "{6, 8, 4}; it is emitted once and shared. Vtables are deduplicated by (size, alignment) of their "
	         "fields, not by type.",
	         "DedupHolder",
	         DedupHolder::file_identifier,
	         d);
}

void toleranceVectors() {
	SchemaV1 v1;
	v1.a = 7;
	v1.b = "old";
	positive_as<SchemaV1, SchemaV2>("tolerance",
	                                "tolerance_old_writer_new_reader",
	                                "Written as SchemaV1{a, b}, read as SchemaV2{a, b, c, d, e}: slots beyond the "
	                                "writer's vtable length read as defaults (c = 0, d absent, e empty).",
	                                "SchemaV1",
	                                "SchemaV2",
	                                kSchemaFid,
	                                v1);

	SchemaV2 v2;
	v2.a = 7;
	v2.b = "new";
	v2.c = 99;
	v2.d = std::string("extra");
	v2.e = { 1, 2 };
	positive_as<SchemaV2, SchemaV1>("tolerance",
	                                "tolerance_new_writer_old_reader",
	                                "Written as SchemaV2, read as SchemaV1: the extra trailing slots are ignored. "
	                                "Note field a and b sit at different physical offsets than in SchemaV1 because "
	                                "c (8 bytes) sorts first.",
	                                "SchemaV2",
	                                "SchemaV1",
	                                kSchemaFid,
	                                v2);
	positive("tolerance",
	         "schema_v2",
	         "SchemaV2 written and read as itself (reference for the two tolerance vectors).",
	         "SchemaV2",
	         kSchemaFid,
	         v2);

	X<Y1> x1;
	x1.a = 1;
	x1.b.a = 2;
	x1.c = 3;
	positive_as<X<Y1>, X<Y2>>("tolerance",
	                          "nested_compat_y1_as_y2",
	                          "flow/FlatBuffers/nestedCompat: X<Y1> read as X<Y2>; Y2 adds a trailing variant, "
	                          "which reads as its default (a variant missing from the vtable becomes alternative 0 "
	                          "default-constructed; the dump shows tag 1 value 0).",
	                          "X<Y1>",
	                          "X<Y2>",
	                          fid(105),
	                          x1);
	X<Y2> x2;
	x2.a = 1;
	x2.b.a = 2;
	x2.b.b = 4;
	x2.c = 3;
	positive_as<X<Y2>, X<Y1>>("tolerance",
	                          "nested_compat_y2_as_y1",
	                          "flow/FlatBuffers/nestedCompat: X<Y2> (with Y2.b = 4) read as X<Y1>.",
	                          "X<Y2>",
	                          "X<Y1>",
	                          fid(105),
	                          x2);

	// A hand-built SchemaV1 message in a layout the C++ writer never produces:
	// tables before their vtables (negative soffsets), fields in declaration
	// order, unshared vtables.
	std::vector<uint8_t> b(56, 0);
	write32(b, 0, 8); // root uoffset -> wrapper table at 8
	write32(b, 4, kSchemaFid);
	write32(b, 8, (uint32_t)(int32_t)(8 - 40)); // wrapper soffset -> vtable at 40
	write32(b, 12, 4); // wrapper slot 0 -> inner table at 16
	write32(b, 16, (uint32_t)(int32_t)(16 - 46)); // inner soffset -> vtable at 46
	write32(b, 20, 0x01020304); // a
	write32(b, 24, 4); // b -> string at 28
	write32(b, 28, 5);
	memcpy(b.data() + 32, "hello", 5);
	write16(b, 40, 6); // wrapper vtable: size 6, table 8, slot0 at +4
	write16(b, 42, 8);
	write16(b, 44, 4);
	write16(b, 46, 8); // inner vtable: size 8, table 12, a at +4, b at +8
	write16(b, 48, 12);
	write16(b, 50, 4);
	write16(b, 52, 8);
	positive_bytes("tolerance",
	               "handbuilt_alternate_layout",
	               "Hand-built SchemaV1{a = 0x01020304, b = \"hello\"} in a layout the C++ writer never produces: "
	               "vtables placed after their tables (negative soffset), fields in declaration order, vtables not "
	               "shared. The C++ reader accepts it, so readers must not assume C++'s layout.",
	               "SchemaV1",
	               b);
}

Untrusted untrustedSource() {
	Untrusted src;
	src.s = "hello world";
	src.v = { 1, 2, 3, 4, 5 };
	src.o = 7;
	src.vo = { Optional<int>(1), Optional<int>(), Optional<int>(3) };
	src.b = { true, false, true };
	src.strs = { "abc", "", "defgh" };
	return src;
}

void negativeVectors() {
	const std::string T = "Untrusted";
	Untrusted src = untrustedSource();
	positive("malformed",
	         "untrusted_good",
	         "flow/FlatBuffers/RejectsMalformedBuffers: the well-formed message every negative vector in this "
	         "group is derived from.",
	         T,
	         Untrusted::file_identifier,
	         src);
	auto good = encode(Untrusted::file_identifier, src);

	// A root offset that points far outside the buffer.
	for (uint32_t rootOffset : { 0x7ffffff0u, 0xffffffffu, (uint32_t)good.size(), (uint32_t)good.size() - 3 }) {
		auto bad = good;
		write32(bad, 0, rootOffset);
		negative("malformed",
		         "root_offset_" + hexstr(rootOffset),
		         "RejectsMalformedBuffers: root offset " + hexstr(rootOffset) + " points outside the buffer.",
		         T,
		         bad,
		         mutation_u32(0, rootOffset));
	}

	// RejectsHostileStringOffsetAndLength
	{
		Untrusted only;
		only.s = "hello world";
		auto g = encode(Untrusted::file_identifier, only);
		positive("malformed",
		         "untrusted_string_only",
		         "flow/FlatBuffers/RejectsHostileStringOffsetAndLength: the well-formed message the string "
		         "offset/length vectors are derived from.",
		         T,
		         Untrusted::file_identifier,
		         only);
		auto it = std::search(g.begin(), g.end(), only.s.begin(), only.s.end());
		size_t lengthPos = (it - g.begin()) - 4;
		if (read32(g, lengthPos) != only.s.size()) {
			throw std::runtime_error("string length not found");
		}
		int offsetPos = -1;
		for (size_t pos = 0; pos + 4 <= lengthPos; pos += 4) {
			if (pos + read32(g, pos) == lengthPos) {
				offsetPos = pos;
			}
		}
		if (offsetPos < 0) {
			throw std::runtime_error("string offset not found");
		}
		uint32_t sizeToEnd = g.size() - offsetPos;
		for (uint32_t hostile : { 0x7ffffff0u, 0xffffffffu, sizeToEnd, sizeToEnd + 1 }) {
			auto bad = g;
			write32(bad, offsetPos, hostile);
			negative("malformed",
			         "string_offset_" + hexstr(hostile),
			         "RejectsHostileStringOffsetAndLength: the string field's relative offset (at byte " +
			             std::to_string(offsetPos) + ") replaced by " + hexstr(hostile) + ".",
			         T,
			         bad,
			         mutation_u32(offsetPos, hostile));
		}
		uint32_t sizeToEndFromPayload = g.size() - (lengthPos + 4);
		for (uint32_t hostile : { 0x7ffffff0u, 0xffffffffu, sizeToEndFromPayload + 1 }) {
			auto bad = g;
			write32(bad, lengthPos, hostile);
			negative("malformed",
			         "string_length_" + hexstr(hostile),
			         "RejectsHostileStringOffsetAndLength: the string length prefix (at byte " +
			             std::to_string(lengthPos) + ") replaced by " + hexstr(hostile) + ".",
			         T,
			         bad,
			         mutation_u32(lengthPos, hostile));
		}
	}

	// RejectsArrayLengthMismatch
	register_type<FixedBytes>("FixedBytes", FixedBytes::file_identifier);
	for (size_t len : { size_t(15), size_t(17), size_t(4096) }) {
		WireBytes w;
		w.v.assign(len, 0xab);
		register_type<WireBytes>("WireBytes", WireBytes::file_identifier);
		auto bytes = encode(WireBytes::file_identifier, w);
		negative("malformed",
		         "array_length_" + std::to_string(len),
		         "RejectsArrayLengthMismatch: WireBytes{vector<u8> of length " + std::to_string(len) +
		             "} read as FixedBytes{array<u8,16>}.",
		         "FixedBytes",
		         bytes,
		         Json::object()
		             .set("op", Json::string("schema_mismatch"))
		             .set("writer_cpp_type", Json::string("WireBytes")));
	}
	{
		WireBytes ok;
		ok.v.assign(16, 0xcd);
		positive_as<WireBytes, FixedBytes>("malformed",
		                                   "array_length_16",
		                                   "RejectsArrayLengthMismatch: a 16-element vector<u8> reads fine as "
		                                   "array<u8,16>.",
		                                   "WireBytes",
		                                   "FixedBytes",
		                                   WireBytes::file_identifier,
		                                   ok);
	}

	// RejectsWrongFileIdentifier
	{
		auto bytes = encode(Untrusted::file_identifier + 1, Untrusted{});
		negative("malformed",
		         "wrong_file_identifier",
		         "RejectsWrongFileIdentifier: a default Untrusted written with file identifier 4243, read "
		         "expecting 4242.",
		         T,
		         bytes,
		         Json::object().set("op", Json::string("file_identifier")).set("value", Json::uinteger(4243)));
	}

	// BoundsChecksKnob: only the first 8 bytes presented.
	{
		Untrusted s2;
		s2.s = "hello world";
		s2.v = { 1, 2, 3 };
		auto g = encode(Untrusted::file_identifier, s2);
		g.resize(8);
		negative("malformed",
		         "truncated_to_header",
		         "BoundsChecksKnob: a valid message cut to its 8-byte header.",
		         T,
		         g,
		         mutation_truncate(8));
	}
	for (size_t n : { size_t(0), size_t(4), size_t(7) }) {
		std::vector<uint8_t> g(good.begin(), good.begin() + n);
		negative("malformed",
		         "too_short_" + std::to_string(n),
		         "A buffer of " + std::to_string(n) + " bytes, shorter than the root offset + file identifier header.",
		         T,
		         g,
		         mutation_truncate(n));
	}

	// Union vector whose offsets slot is missing from the vtable.
	{
		auto bad = good;
		size_t vt = first_member_vtable(bad);
		// Untrusted slots: s=0, v=1, o=2,3, vo=4,5, b=6, strs=7. Entry for slot k is at vt + 4 + 2k.
		size_t pos = vt + 4 + 2 * 5;
		write16(bad, pos, 0);
		negative("malformed",
		         "union_vector_missing_offsets_slot",
		         "The vtable entry for vo's offsets slot (slot 5) zeroed while its types slot (4) stays present.",
		         T,
		         bad,
		         mutation_u16(pos, 0));
	}

	// A shortened vtable is tolerated: missing slots read as defaults.
	{
		auto bad = good;
		size_t vt = first_member_vtable(bad);
		write16(bad, vt, 2 * 2 + 4);
		positive_bytes("tolerance",
		               "truncated_vtable",
		               "untrusted_good with the vtable length of the Untrusted table (the first u16 of its vtable) "
		               "cut from 20 to 8, so it covers only slots 0 and 1: s and v decode, every later field reads "
		               "as its default. Readers must bounds-check slot indexes against the vtable length.",
		               T,
		               bad);
	}

	// Corpora: every truncation, and hostile words at every aligned position.
	if (g_positiveOnly) {
		return;
	}
	{
		std::vector<Json> lines;
		for (size_t n = 0; n < good.size(); ++n) {
			std::vector<uint8_t> g(good.begin(), good.begin() + n);
			lines.push_back(corpus_entry("truncate_" + std::to_string(n), T, g, mutation_truncate(n)));
		}
		fixtures().corpora.push_back({ "untrusted_truncations", lines });
		fixtures().corpusDescriptions.push_back(
		    { "untrusted_truncations",
		      "RejectsMalformedBuffers: untrusted_good truncated to every length 0..size-1, with the C++ (FDB main) "
		      "reader's outcome." });
	}
	{
		std::vector<Json> lines;
		for (uint32_t hostile : { 0u, 1u, 0xffffffffu, 0x7ffffff0u, 0x80000000u, 0xfffffff0u, 0x10000u }) {
			for (size_t pos = 0; pos + 4 <= good.size(); pos += 4) {
				auto bad = good;
				write32(bad, pos, hostile);
				lines.push_back(corpus_entry(
				    "word_" + std::to_string(pos) + "_" + hexstr(hostile), T, bad, mutation_u32(pos, hostile)));
			}
		}
		fixtures().corpora.push_back({ "untrusted_word_overwrites", lines });
		fixtures().corpusDescriptions.push_back(
		    { "untrusted_word_overwrites",
		      "RejectsMalformedBuffers: each aligned u32 of untrusted_good overwritten with a hostile value (0, 1, "
		      "0xffffffff, 0x7ffffff0, 0x80000000, 0xfffffff0, 0x10000), with the C++ (FDB main) reader's "
		      "outcome." });
	}
}

} // namespace

void build_engine_vectors() {
	scalarVectors();
	stringVectors();
	vectorVectors();
	nestedVectors();
	optionalVectors();
	variantVectors();
	errorOrVectors();
	fileIdentifierVectors();
	orderAndArenaVectors();
	toleranceVectors();
	negativeVectors();
}
