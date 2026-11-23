// The real FDB types behind the names the dumper, schema lister and strict
// checker use (src/stubs.h has stand-ins for the engine build). Compiled only
// inside an FDB build tree (GV_REAL_FDB, see targets.cmake).
#pragma once

#include <stdexcept>
#include <string>
#include <vector>

#include "flow/flow.h"
#include "flow/ObjectSerializer.h"
#include "flow/ProtocolVersion.h"
#include "flow/WipedString.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/GrvProxyInterface.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/VersionVector.h"
#if __has_include("fdbclient/CDCProxyInterface.h")
#include "fdbclient/CDCProxyInterface.h"
#endif
#if __has_include("fdbclient/TagThrottle.h")
#include "fdbclient/TagThrottle.h"
#else
#include "fdbclient/TagThrottle.actor.h"
#endif
#if __has_include("fdbclient/IdempotencyId.h")
#include "fdbclient/IdempotencyId.h"
#else
#include "fdbclient/IdempotencyId.actor.h"
#endif

#include "hooks.h"
#include "json.h"

// Thrown for any buffer the tool rejects before handing it to FDB's reader.
struct DecodeError : std::runtime_error {
	using std::runtime_error::runtime_error;
};

// Real serialize() functions may call ar.protocolVersion() (e.g. StorageServerInterface's
// ASSERT_WE_THINK). FlowTransport serializes with the current protocol version.
#define GV_VISITOR_EXTRAS                                                                                              \
	ProtocolVersion protocolVersion() const {                                                                          \
		return currentProtocolVersion();                                                                               \
	}

// Standalone<T> is serialized as T (flow/ObjectSerializer.h: LoadSaveHelper<Standalone<T>>).
template <class T>
struct gv_unwrap<Standalone<T>> {
	using type = T;
};

inline Json gv_bytes(StringRef s) {
	static const char* digits = "0123456789abcdef";
	std::string h;
	for (uint8_t c : s) {
		h += digits[c >> 4];
		h += digits[c & 15];
	}
	return Json::string(h);
}

// VectorRef<KeyValueRef, VecSerStrategy::String> (GetKeyValuesReply::data): one blob,
// u32 count, then per item u32 key length, key, u32 value length, value.
template <>
struct gv_leaf<VectorRef<KeyValueRef, VecSerStrategy::String>> : std::true_type {
	static std::string name() { return "preserialized_kv_vector"; }
	static Json dump(const VectorRef<KeyValueRef, VecSerStrategy::String>& v) {
		Json a = Json::array();
		for (const auto& kv : v) {
			a.push(Json::object().set("key", gv_bytes(kv.key)).set("value", gv_bytes(kv.value)));
		}
		return a;
	}
};

// TagSet (fdbclient/TagThrottle.h): one blob; per tag a u8 length then the tag bytes.
template <>
struct gv_leaf<TagSet> : std::true_type {
	static std::string name() { return "tag_set"; }
	static Json dump(const TagSet& v) {
		Json a = Json::array();
		for (const auto& tag : v) {
			a.push(gv_bytes(tag));
		}
		return a;
	}
};

// VersionVector (fdbclient/VersionVector.h, dynamic_size_traits<VersionVector>): one
// blob with a compacted encoding of the (tag -> version) map and maxVersion.
template <>
struct gv_leaf<VersionVector> : std::true_type {
	static std::string name() { return "version_vector"; }
	static Json dump(const VersionVector& v) {
		Json a = Json::array();
		for (const auto& [tag, version] : v.versions) { // private; compiled with -fno-access-control
			a.push(Json::object()
			           .set("locality", Json::integer(tag.locality))
			           .set("id", Json::uinteger(tag.id))
			           .set("version", Json::integer(version)));
		}
		return Json::object().set("versions", a).set("maxVersion", Json::integer(v.getMaxVersion()));
	}
};

// IdempotencyIdRef: a blob with string framing (empty for an absent id).
template <>
struct gv_leaf<IdempotencyIdRef> : std::true_type {
	static std::string name() { return "bytes"; }
	static Json dump(const IdempotencyIdRef& v) { return gv_bytes(v.valid() ? v.asStringRefUnsafe() : StringRef()); }
};

// The string inside WipedString (TenantInfo::token): string framing.
template <>
struct gv_leaf<detail::WipedStringSerdesWrapper> : std::true_type {
	static std::string name() { return "bytes"; }
	static Json dump(const detail::WipedStringSerdesWrapper& v) { return gv_bytes(v.get()); }
};

// Template tables, named like the engine stubs.
template <class T>
struct gv_table_info<ReplyPromise<T>> : std::true_type {
	static constexpr const char* name = "reply_promise";
	static constexpr const char* field_names = "token";
	static std::vector<const void*> addresses(const ReplyPromise<T>&) { return { nullptr }; }
	static std::vector<const void*> alt_addresses(const ReplyPromise<T>&) { return {}; }
};

template <class T, bool P>
struct gv_table_info<RequestStream<T, P>> : std::true_type {
	static constexpr const char* name = "request_stream";
	static constexpr const char* field_names = "endpoint";
	static std::vector<const void*> addresses(const RequestStream<T, P>&) { return { nullptr }; }
	static std::vector<const void*> alt_addresses(const RequestStream<T, P>&) { return {}; }
};

// FlowTransport's PingRequest is defined in fdbrpc/FlowTransport(.actor).cpp, not in a header,
// so it is mirrored here (the reply's PeerCompatibilityPolicy is not serialized).
// gv_fields.g.h, generated from that source file, static_asserts its file identifier, and
// the dumper checks its serializer arguments against it.
struct PingRequest {
	constexpr static FileIdentifier file_identifier = 4707015;
	ReplyPromise<Void> reply;
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

// gv_table_info for every concrete table type, generated from this branch's headers
// by extract_fields.py (see targets.cmake).
#include "gv_fields.g.h"
