// Message-level golden vectors: real FDB request and reply types of one release
// branch (release-7.4 or release-8.0), serialized by that branch's own code.
// Built inside the branch's CMake build (targets.cmake) and run by
// ../regen_messages.sh. See ../README.md ("Message-level vectors").
#include "fixtures.h"
#if __has_include("fdbclient/NativeAPI.h")
#include "fdbclient/NativeAPI.h"
#else
#include "fdbclient/NativeAPI.actor.h"
#endif
#if __has_include("fdbrpc/TokenCache.h")
#include "fdbrpc/TokenCache.h"
#define GV_HAS_TOKEN_CACHE 1
#endif

#if GV_FDB_MAJOR < 8
// Fields removed in 8.0: TenantInfo in every read/commit request, GetReadVersionReply's
// proxyTagThrottledDuration, ClientDBInfo's tenantMode/encryptKeyProxy/metaclusterName.
#define GV_V74 1
#endif

using namespace std::string_literals;

namespace {

// Memory for the StringRef/VectorRef parts of the values below; never freed.
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
template <class T>
VectorRef<T> vec(std::initializer_list<T> items) {
	VectorRef<T> v;
	for (const auto& t : items) {
		v.push_back(valueArena(), t);
	}
	return v;
}

// --- addresses, endpoints, interfaces -------------------------------------
NetworkAddress addr4(uint32_t ip, uint16_t port, bool tls = false) {
	return NetworkAddress(IPAddress(ip), port, /*isPublic*/ true, tls);
}
NetworkAddress addr6(uint8_t last, uint16_t port, bool tls) {
	IPAddress::IPAddressStore a{};
	a[0] = 0xfd;
	a[1] = 0xdb;
	a[15] = last;
	return NetworkAddress(IPAddress(a), port, /*isPublic*/ true, tls);
}
// The primary address is not TLS and the secondary is, so Endpoint::choosePrimaryAddress()
// (run by the reader, and by this process, whose local address is not TLS) keeps the order.
Endpoint endpoint(UID token, bool withSecondary = false) {
	NetworkAddressList l;
	l.address = addr4(0x0a000001, 4500);
	if (withSecondary) {
		l.secondaryAddress = addr6(1, 4501, /*tls*/ true);
	}
	return Endpoint(l, token);
}
// A reply promise as a client sends it: only the token travels.
template <class T>
ReplyPromise<T> replyTo(uint64_t n) {
	return ReplyPromise<T>(Endpoint(NetworkAddressList(), UID(0x5e9d000000000000ULL + n, 0x7e9d000000000000ULL + n)));
}

SpanContext span(uint64_t n) {
	return SpanContext(
	    UID(0x7ace000000000000ULL + n, 0x7ace100000000000ULL + n), 0x5ba0000000000000ULL + n, TraceFlags::sampled);
}

#ifdef GV_V74
TenantInfo tenant(int64_t id) {
	return TenantInfo(id, WipedString("authz-token"_sr));
}
#endif

TagSet tags(std::initializer_list<const char*> names) {
	TagSet t;
	for (const char* n : names) {
		t.addTag(TransactionTagRef(sref(n)));
	}
	return t;
}

VersionVector versionVector(Version maxVersion, std::initializer_list<std::tuple<int8_t, uint16_t, Version>> entries) {
	VersionVector v; // setVersion() requires increasing versions; maxVersion follows the last one
	for (const auto& [locality, id, version] : entries) {
		v.setVersion(Tag(locality, id), version);
	}
	v.setMaxVersion(maxVersion);
	return v;
}

ReadOptions readOptions() {
	ReadOptions o(UID(0xdeb0000000000001ULL, 0xdeb0000000000002ULL), ReadType::FETCH, CacheResult::False, Version(777));
	o.lockAware = true;
	return o;
}

// ReadOptions as fdb-native-client builds them from the transaction options: NativeAPI's
// setOption starts from ReadOptions() and READ_PRIORITY_* sets type, READ_SERVER_SIDE_CACHE_*
// cacheResult, LOCK_AWARE / READ_LOCK_AWARE lockAware. debugID and consistencyCheckStartVersion
// stay absent (the client sends no debug ids and runs no consistency checks).
ReadOptions clientReadOptions(ReadType type, CacheResult cache, bool lockAware) {
	ReadOptions o(type, cache);
	o.lockAware = lockAware;
	return o;
}

LocalityData locality(const std::string& process) {
	LocalityData l;
	l.set("processid"_sr, key(process));
	l.set("zoneid"_sr, key("zone-a"));
	l.set("machineid"_sr, key("machine-1"));
	l.set("dcid"_sr, Optional<Standalone<StringRef>>()); // present key, absent value
	return l;
}

StorageServerInterface storageServer(uint64_t n, bool tss) {
	StorageServerInterface s(UID(0x55000000000000ULL + n, 0x55100000000000ULL + n));
	s.locality = locality("ss-" + std::to_string(n));
	s.getValue =
	    PublicRequestStream<GetValueRequest>(endpoint(UID(0x6e70000000000000ULL + n, 0x6e70000000000000ULL), n == 1));
	if (tss) {
		s.tssPairID = UID(0x7550000000000000ULL + n, 1);
	}
	s.acceptingRequests = n % 2 == 1; // private; compiled with -fno-access-control
	return s;
}

GrvProxyInterface grvProxy(uint64_t n) {
	GrvProxyInterface g;
	g.processId = key("grv-process-" + std::to_string(n));
	g.provisional = n == 2;
	g.getConsistentReadVersion = PublicRequestStream<GetReadVersionRequest>(
	    endpoint(UID(0x67e0000000000000ULL + n, 0x67e0000000000000ULL), n == 1));
	return g;
}

CommitProxyInterface commitProxy(uint64_t n) {
	CommitProxyInterface c;
	c.processId = key("commit-process-" + std::to_string(n));
	c.provisional = n == 2;
	c.commit = PublicRequestStream<CommitTransactionRequest>(
	    endpoint(UID(0xc0a0000000000000ULL + n, 0xc0a0000000000000ULL), n == 1));
	return c;
}

MutationRef mutation(MutationRef::Type type, const std::string& p1, const std::string& p2) {
	return MutationRef(type, sref(p1), sref(p2));
}

// --- registration and checks ----------------------------------------------

template <class T>
void messageChecked(const std::string& group,
                    const std::string& name,
                    const std::string& description,
                    const std::string& cppType,
                    const T& value);

// A positive message vector. Besides what positive() checks (the branch's reader
// decodes the bytes back to the same field dump, and the bytes pass the strict
// checks), the bytes must equal what FDB's own ObjectWriter produces, which is
// what FlowTransport puts in a packet (with AssumeVersion: no version prefix).
template <class T>
void message(const std::string& group,
             const std::string& name,
             const std::string& description,
             const std::string& cppType,
             const T& value) {
	try {
		messageChecked(group, name, description, cppType, value);
	} catch (Error& e) {
		throw std::runtime_error(name + ": FDB error " + std::to_string(e.code()) + " " + e.name());
	}
}

template <class T>
void messageChecked(const std::string& group,
                    const std::string& name,
                    const std::string& description,
                    const std::string& cppType,
                    const T& value) {
	FileIdentifier fid = FileIdentifierFor<T>::value;
	positive(group, name, description, cppType, fid, value);
	ObjectWriter writer(AssumeVersion(currentProtocolVersion()));
	writer.serialize(value);
	std::vector<uint8_t> ours = encode(fid, value);
	StringRef out = writer.toStringRef();
	if (std::vector<uint8_t>(out.begin(), out.end()) != ours) {
		throw std::runtime_error(name + ": ObjectWriter output differs from the tool's encoder");
	}
}

// The reply envelope FlowTransport sends for a reply of type T.
template <class T>
using Reply = ErrorOr<EnsureTable<T>>;

std::string replyName(const std::string& t) {
	return "ErrorOr<EnsureTable<" + t + ">>";
}

// --- the catalog ----------------------------------------------------------
void storageVectors() {
	const std::string g = "storage";
#ifdef GV_V74
	const std::string tenantNote = " 7.4 only: tenantInfo (TenantInfo: tenantId, token, arena), the slot after "
	                               "spanContext; 8.0 removed it mid-table, so every later slot moved down by one.";
#else
	const std::string tenantNote = " 7.4 had tenantInfo (TenantInfo) after spanContext; 8.0 removed it mid-table, so "
	                               "every later slot is one lower than on 7.4.";
#endif
	{
		GetValueRequest r;
		r.spanContext = span(1);
#ifdef GV_V74
		r.tenantInfo = tenant(42);
#endif
		r.key = key("user/key\x00\xff"s);
		r.version = 1234567890123LL;
		r.tags = tags({ "tagA", "tag-b" });
		r.options = readOptions();
		r.ssLatestCommitVersions =
		    versionVector(1234567890000LL, { { 0, 1, 1234567889000LL }, { 0, 7, 1234567889100LL } });
		r.reply = replyTo<GetValueReply>(1);
		message(g,
		        "get_value_request",
		        "StorageServerInterface::getValue request (the storage server's base endpoint). Every field set: "
		        "tags (Optional<TagSet>), options (Optional<ReadOptions>), ssLatestCommitVersions (VersionVector "
		        "with two tags of one locality and 1-byte version deltas)." +
		            tenantNote,
		        "GetValueRequest",
		        r);
	}
	{
		GetValueRequest r;
		r.key = key("k");
		r.version = 1;
		r.ssLatestCommitVersions = versionVector(2000, { { 0, 2, 1000 }, { 0, 3, 2000 } });
		r.reply = replyTo<GetValueReply>(11);
		message(g,
		        "get_value_request_minimal",
		        "GetValueRequest with tags and options absent and a default SpanContext" +
#ifdef GV_V74
		            std::string(" and TenantInfo (tenantId -1, token absent)") +
#endif
		            std::string(". ssLatestCommitVersions needs 2-byte version deltas."),
		        "GetValueRequest",
		        r);
	}
	{
		// What fdb-native-client sends for a transaction with READ_PRIORITY_NORMAL set: present
		// options with ReadOptions()'s values.
		GetValueRequest r;
		r.key = key("prio/normal");
		r.version = 4000000000000LL;
		r.options = clientReadOptions(ReadType::NORMAL, CacheResult::True, false);
		r.reply = replyTo<GetValueReply>(12);
		message(g,
		        "get_value_request_read_options",
		        "GetValueRequest as fdb-native-client sends it with a transaction read option set: options "
		        "present with ReadOptions()'s values (type NORMAL, cacheResult true, lockAware false; debugID and "
		        "consistencyCheckStartVersion absent), tags absent, a default SpanContext" +
#ifdef GV_V74
		            std::string(" and TenantInfo") +
#endif
		            std::string(", an empty VersionVector."),
		        "GetValueRequest",
		        r);
	}
	{
		GetValueReply r(Optional<Value>(key("the value")), true);
		r.penalty = 1.5;
		r.error = Optional<Error>();
		message(g,
		        "get_value_reply",
		        "GetValueReply as a bare table (penalty and error come from LoadBalancedReply).",
		        "GetValueReply",
		        r);
		message(g,
		        "get_value_reply_envelope",
		        "GetValueReply as sent: ErrorOr<EnsureTable<GetValueReply>>, tag 2 (success).",
		        replyName("GetValueReply"),
		        Reply<GetValueReply>(r));
		GetValueReply absent;
		absent.penalty = 0.25;
		absent.error = Optional<Error>(Error(1037)); // process_behind, a load-balancer error
		message(g,
		        "get_value_reply_absent_value",
		        "GetValueReply with value absent (key not found) and LoadBalancedReply::error present.",
		        replyName("GetValueReply"),
		        Reply<GetValueReply>(absent));
		message(g,
		        "get_value_reply_error",
		        "Error reply envelope: ErrorOr<EnsureTable<GetValueReply>> tag 1, Error 1009 (future_version).",
		        replyName("GetValueReply"),
		        Reply<GetValueReply>(Error(1009)));
	}
	{
		GetKeyValuesRequest r;
		r.spanContext = span(2);
#ifdef GV_V74
		r.tenantInfo = tenant(43);
#endif
		r.begin = KeySelectorRef(sref("range/a"), false, 1);
		r.end = KeySelectorRef(sref("range/z"), true, -3);
		r.version = 2000000000001LL;
		r.limit = 1000;
		r.limitBytes = 80000;
		r.tags = tags({ "scan" });
		r.options = readOptions();
		r.ssLatestCommitVersions =
		    versionVector(2000000000000LL, { { -2, 300, 1000000000000LL }, { 1, 2, 1999999999999LL } });
#if GV_FDB_MAJOR >= 8
		r.taskID = TaskPriority::FetchKeys;
#endif
		r.reply = replyTo<GetKeyValuesReply>(2);
		message(g,
		        "get_key_values_request",
		        "StorageServerInterface::getKeyValues request (adjusted endpoint 2). begin/end are KeySelectorRef "
		        "tables (key, orEqual, offset). ssLatestCommitVersions has a tag id > 255 and a negative locality "
		        "and version deltas that need 8 bytes. 8.0 added taskID (Optional<TaskPriority>, an i32 enum) after "
		        "ssLatestCommitVersions." +
		            tenantNote,
		        "GetKeyValuesRequest",
		        r);
	}
	{
		// READ_PRIORITY_LOW and READ_SERVER_SIDE_CACHE_DISABLE: a background scan.
		GetKeyValuesRequest r;
		r.begin = KeySelectorRef(sref("scan/a"), false, 1);
		r.end = KeySelectorRef(sref("scan/z"), false, 1);
		r.version = 4000000000001LL;
		r.limit = 80000;
		r.limitBytes = 80000;
		r.options = clientReadOptions(ReadType::LOW, CacheResult::False, false);
#if GV_FDB_MAJOR >= 8
		r.taskID = TaskPriority::DefaultEndpoint;
#endif
		r.reply = replyTo<GetKeyValuesReply>(12);
		message(g,
		        "get_key_values_request_read_options",
		        "GetKeyValuesRequest as fdb-native-client sends it with READ_PRIORITY_LOW and "
		        "READ_SERVER_SIDE_CACHE_DISABLE: options type LOW, cacheResult false, lockAware false (debugID and "
		        "consistencyCheckStartVersion absent), tags absent, an empty VersionVector"
#if GV_FDB_MAJOR >= 8
		        ", taskID DefaultEndpoint"
#endif
		        ".",
		        "GetKeyValuesRequest",
		        r);
	}
	{
		GetKeyValuesReply r;
		r.penalty = 2.0;
		r.data.push_back_deep(r.arena, KeyValueRef("range/b"_sr, "value-b"_sr));
		r.data.push_back_deep(r.arena, KeyValueRef("range/c"_sr, ""_sr));
		r.data.push_back_deep(r.arena, KeyValueRef("range/d\xff"_sr, "\x00\x01"_sr));
		// push_back on a VectorRef<_, String> leaves its cached serialized size at 0 (Arena.h:
		// VectorRefPreserializer::add only updates a positive size); the storage server builds
		// it from a FlatBuffers VectorRef, which marks the size unknown. Do the same.
		r.data.invalidate();
		r.version = 2000000000001LL;
		r.more = true;
		r.cached = false;
		message(g,
		        "get_key_values_reply",
		        "GetKeyValuesReply as a bare table: data is the pre-serialized KV vector (one blob).",
		        "GetKeyValuesReply",
		        r);
		message(g,
		        "get_key_values_reply_envelope",
		        "GetKeyValuesReply as sent (ErrorOr<EnsureTable<>> tag 2).",
		        replyName("GetKeyValuesReply"),
		        Reply<GetKeyValuesReply>(r));
		GetKeyValuesReply empty;
		empty.version = 5;
		message(g,
		        "get_key_values_reply_empty",
		        "GetKeyValuesReply with no rows: data is a 4-byte blob (count 0), not the shared empty blob.",
		        replyName("GetKeyValuesReply"),
		        Reply<GetKeyValuesReply>(empty));
	}
	{
		GetKeyRequest r;
		r.spanContext = span(3);
#ifdef GV_V74
		r.tenantInfo = tenant(44);
#endif
		r.sel = KeySelectorRef(sref("sel/key"), true, 5);
		r.version = 3000000000000LL;
		r.tags = tags({ "k" });
		r.options = readOptions();
		r.ssLatestCommitVersions = versionVector(3000000000000LL, {});
		r.reply = replyTo<GetKeyReply>(3);
		message(g,
		        "get_key_request",
		        "StorageServerInterface::getKey request (adjusted endpoint 1). ssLatestCommitVersions is an empty "
		        "VersionVector (only maxVersion)." +
		            tenantNote,
		        "GetKeyRequest",
		        r);
	}
	{
		// READ_PRIORITY_HIGH in a LOCK_AWARE transaction.
		GetKeyRequest r;
		r.sel = KeySelectorRef(sref("prio/high"), false, 1);
		r.version = 4000000000002LL;
		r.options = clientReadOptions(ReadType::HIGH, CacheResult::True, true);
		r.reply = replyTo<GetKeyReply>(12);
		message(g,
		        "get_key_request_read_options",
		        "GetKeyRequest as fdb-native-client sends it with READ_PRIORITY_HIGH and LOCK_AWARE: options type "
		        "HIGH, cacheResult true, lockAware true (debugID and consistencyCheckStartVersion absent), tags "
		        "absent, an empty VersionVector.",
		        "GetKeyRequest",
		        r);
	}
	{
		GetKeyReply r(KeySelector(KeySelectorRef("sel/result"_sr, false, 0)), true);
		r.penalty = 0.5;
		message(
		    g, "get_key_reply", "GetKeyReply as a bare table (sel is a Standalone<KeySelectorRef>).", "GetKeyReply", r);
		message(g, "get_key_reply_envelope", "GetKeyReply as sent.", replyName("GetKeyReply"), Reply<GetKeyReply>(r));
	}
	{
		WatchValueRequest r;
		r.spanContext = span(4);
#ifdef GV_V74
		r.tenantInfo = tenant(45);
#endif
		r.key = key("watched");
		r.value = Optional<Value>(key("old-value"));
		r.version = 4000000000000LL;
		r.tags = tags({ "w" });
		r.debugID = UID(0xdeb1, 0xdeb2);
		r.reply = replyTo<WatchValueReply>(4);
		message(g,
		        "watch_value_request",
		        "StorageServerInterface::watchValue request (adjusted endpoint 10)." + tenantNote,
		        "WatchValueRequest",
		        r);
	}
	{
		WatchValueReply r(4000000000777LL);
		r.cached = true;
		message(g, "watch_value_reply", "WatchValueReply as a bare table.", "WatchValueReply", r);
		message(g,
		        "watch_value_reply_envelope",
		        "WatchValueReply as sent.",
		        replyName("WatchValueReply"),
		        Reply<WatchValueReply>(r));
	}
	{
		StorageMetrics min;
		min.bytes = 1;
		min.bytesWrittenPerKSecond = 2;
		min.iosPerKSecond = 3;
		min.bytesReadPerKSecond = 4;
		min.opsReadPerKSecond = 5;
		StorageMetrics max;
		max.bytes = -1;
		max.bytesWrittenPerKSecond = 6000000000LL;
		max.iosPerKSecond = 7;
		max.bytesReadPerKSecond = 8;
		max.opsReadPerKSecond = StorageMetrics::infinity;
#ifdef GV_V74
		WaitMetricsRequest r(tenant(48), 9000000000000LL, KeyRangeRef("metrics/a"_sr, "metrics/z"_sr), min, max);
#else
		WaitMetricsRequest r(9000000000000LL, KeyRangeRef("metrics/a"_sr, "metrics/z"_sr), min, max);
#endif
		r.reply = replyTo<StorageMetrics>(12);
		message(g,
		        "wait_metrics_request",
		        "StorageServerInterface::waitMetrics request (adjusted endpoint 4). min and max are StorageMetrics "
		        "tables (bytes, bytesWrittenPerKSecond, iosPerKSecond, bytesReadPerKSecond, opsReadPerKSecond: all "
		        "i64), every field distinct. 7.4: (keys, min, max, reply, tenantInfo, minVersion, arena); 8.0: "
		        "(keys, min, max, reply, legacyVersion, arena): tenantInfo removed and minVersion renamed, so "
		        "legacyVersion (i64) is slot 4 where 7.4 has tenantInfo.",
		        "WaitMetricsRequest",
		        r);
	}
	{
		// What getStorageMetrics (doGetStorageMetrics) sends: min all zero, max.bytes -1 (a
		// reversed bound on bytes, so the server replies at once), latestVersion, no tenant.
		StorageMetrics min, max;
		min.bytes = 0;
		max.bytes = -1;
#ifdef GV_V74
		WaitMetricsRequest r(TenantInfo(), latestVersion, KeyRangeRef("metrics/a"_sr, "metrics/z"_sr), min, max);
#else
		WaitMetricsRequest r(latestVersion, KeyRangeRef("metrics/a"_sr, "metrics/z"_sr), min, max);
#endif
		r.reply = replyTo<StorageMetrics>(13);
		message(g,
		        "wait_metrics_request_estimate",
		        "WaitMetricsRequest as Transaction::getEstimatedRangeSizeBytes sends it (doGetStorageMetrics): min "
		        "zero, max.bytes = -1 and the rest zero, version latestVersion (-2)" +
#ifdef GV_V74
		            std::string(", TenantInfo() (tenantId -1, token absent)") +
#endif
		            std::string("."),
		        "WaitMetricsRequest",
		        r);
	}
	{
		StorageMetrics m;
		m.bytes = 123456789012LL;
		m.bytesWrittenPerKSecond = 1000;
		m.iosPerKSecond = 20;
		m.bytesReadPerKSecond = 300000;
		m.opsReadPerKSecond = 4;
		message(g, "storage_metrics", "StorageMetrics (the waitMetrics reply) as a bare table.", "StorageMetrics", m);
		message(g,
		        "storage_metrics_envelope",
		        "StorageMetrics as sent (ErrorOr<EnsureTable<StorageMetrics>> tag 2). Not a LoadBalancedReply: no "
		        "penalty or error fields; a server's error comes as the envelope's error.",
		        replyName("StorageMetrics"),
		        Reply<StorageMetrics>(m));
		message(g,
		        "storage_metrics_error",
		        "Error reply envelope: ErrorOr<EnsureTable<StorageMetrics>> tag 1, Error 1001 (wrong_shard_server), "
		        "as sendErrorWithPenalty sends it for a reply type that is not a LoadBalancedReply.",
		        replyName("StorageMetrics"),
		        Reply<StorageMetrics>(Error(1001)));
	}
	{
#ifdef GV_V74
		SplitRangeRequest r(tenant(49), KeyRangeRef("split/a"_sr, "split/z"_sr), 10000000);
#else
		SplitRangeRequest r(KeyRangeRef("split/a"_sr, "split/z"_sr), 10000000, 17);
#endif
		r.reply = replyTo<SplitRangeReply>(14);
		message(g,
		        "split_range_request",
		        "StorageServerInterface::getRangeSplitPoints request (adjusted endpoint 12). 7.4: (keys, chunkSize, "
		        "reply, tenantInfo, arena); 8.0: (keys, chunkSize, reply, limit, arena): tenantInfo removed and limit "
		        "(i32, the most split points to return, -1 for no limit) added in its slot.",
		        "SplitRangeRequest",
		        r);
	}
	{
		SplitRangeReply r;
		r.splitPoints.push_back_deep(r.splitPoints.arena(), "split/f"_sr);
		r.splitPoints.push_back_deep(r.splitPoints.arena(), "split/m\x00\xff"_sr);
		message(g,
		        "split_range_reply",
		        "SplitRangeReply as a bare table: splitPoints, a vector of strings.",
		        "SplitRangeReply",
		        r);
		message(g,
		        "split_range_reply_envelope",
		        "SplitRangeReply as sent (ErrorOr<EnsureTable<>> tag 2). Not a LoadBalancedReply.",
		        replyName("SplitRangeReply"),
		        Reply<SplitRangeReply>(r));
		message(g,
		        "split_range_reply_empty",
		        "SplitRangeReply with no split points (a range smaller than the chunk size).",
		        replyName("SplitRangeReply"),
		        Reply<SplitRangeReply>(SplitRangeReply()));
	}
}

void proxyVectors() {
	const std::string g = "proxy";
	{
		TransactionTagMap<uint32_t> tagCounts;
		tagCounts[key("grv-tag")] = 3;
		GetReadVersionRequest r(span(5),
		                        3,
		                        TransactionPriority::IMMEDIATE,
		                        5000000000000LL,
		                        GetReadVersionRequest::FLAG_USE_MIN_KNOWN_COMMITTED_VERSION |
		                            GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY,
		                        tagCounts,
		                        Optional<UID>(UID(0xdeb3, 0xdeb4)));
#if GV_FDB_MAJOR >= 8
		r.maxGrvQueueDelayMS = 250;
#endif
		r.reply = replyTo<GetReadVersionReply>(5);
		message(g,
		        "get_read_version_request",
		        "GrvProxyInterface::getConsistentReadVersion request. flags carries the priority in its high byte "
		        "(PRIORITY_SYSTEM_IMMEDIATE = 15 << 24) plus FLAG_USE_MIN_KNOWN_COMMITTED_VERSION | "
		        "FLAG_CAUSAL_READ_RISKY; tags is a TransactionTagMap<uint32_t> (std::unordered_map, one entry so the "
		        "order is canonical). 8.0 added maxGrvQueueDelayMS (Optional<int64_t>) as the last slot.",
		        "GetReadVersionRequest",
		        r);
	}
	{
		GetReadVersionReply r;
		r.processBusyTime = 125000; // int (BasicLoadBalancedReply)
		r.version = 5000000000123LL;
		r.locked = true;
		r.metadataVersion = Optional<Value>(key("\x00\x00\x00\x01\x00\x00\x00\x02\x00\x03"s));
		r.midShardSize = 123456789;
		r.rkDefaultThrottled = true;
		r.rkBatchThrottled = true;
		// ClientTagThrottleLimits serializes `expiration - now()` as `duration`; the network
		// clock does not advance in this process, so 30.0 round-trips exactly.
		r.tagThrottleInfo[key("throttled")] = ClientTagThrottleLimits(100.5, now() + 30.0);
		r.ssVersionVectorDelta =
		    versionVector(5000000070000LL, { { 0, 3, 5000000000000LL }, { 1, 4, 5000000070000LL } }); // 4-byte deltas
		r.proxyId = UID(0x9e0, 0x9e1);
#ifdef GV_V74
		r.proxyTagThrottledDuration = 0.75;
#endif
		message(g,
		        "get_read_version_reply",
		        "GetReadVersionReply as a bare table: processBusyTime (BasicLoadBalancedReply), tagThrottleInfo "
		        "(TransactionTagMap<ClientTagThrottleLimits>: the throttle table holds tpsRate and duration = "
		        "expiration - now()), ssVersionVectorDelta (two localities, 4-byte version deltas). processBusyTime "
		        "is an int. 7.4 only: proxyTagThrottledDuration, the last slot (removed in 8.0).",
		        "GetReadVersionReply",
		        r);
		message(g,
		        "get_read_version_reply_envelope",
		        "GetReadVersionReply as sent.",
		        replyName("GetReadVersionReply"),
		        Reply<GetReadVersionReply>(r));
	}
	{
		CommitTransactionRequest r(span(6));
		CommitTransactionRef& t = r.transaction;
		t.read_conflict_ranges =
		    vec<KeyRangeRef>({ KeyRangeRef(sref("read/a"), sref("read/m")),
		                       KeyRangeRef(sref("read/single"), sref(std::string("read/single\x00", 12))) });
		t.write_conflict_ranges = vec<KeyRangeRef>(
		    { KeyRangeRef(sref("k1"), sref(std::string("k1\x00", 3))), KeyRangeRef(sref("clear/a"), sref("clear/z")) });
		t.mutations =
		    vec<MutationRef>({ mutation(MutationRef::SetValue, "k1", "v1"),
		                       mutation(MutationRef::ClearRange, "clear/a", "clear/z"),
		                       mutation(MutationRef::AddValue, "counter", std::string("\x01\x00\x00\x00", 4)),
		                       mutation(MutationRef::SetVersionstampedKey,
		                                std::string("vs/\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00", 17),
		                                "vsv") });
		t.read_snapshot = 6000000000000LL;
		t.report_conflicting_keys = true;
		t.lock_aware = true;
		t.spanContext = span(7);
		t.tenantIds = vec<int64_t>({ 7, 8 });
		r.reply = replyTo<CommitID>(6);
		r.flags = CommitTransactionRequest::FLAG_IS_LOCK_AWARE | CommitTransactionRequest::FLAG_FIRST_IN_BATCH;
		r.debugID = UID(0xdeb5, 0xdeb6);
		ClientTrCommitCostEstimation cost;
		cost.opsCount = 4;
		cost.writeCosts = 4096;
		cost.clearIdxCosts.emplace_back(1, 8192);
		cost.expensiveCostEstCount = 1;
		r.commitCostEstimation = cost;
		r.tagSet = tags({ "commit-tag" });
#ifdef GV_V74
		r.tenantInfo = tenant(46);
#endif
		r.idempotencyId = IdempotencyIdRef(sref("idempotency-id-0123456789"));
		message(g,
		        "commit_transaction_request",
		        "CommitProxyInterface::commit request (the commit proxy's base endpoint). transaction is a "
		        "CommitTransactionRef table: two read and two write conflict ranges (one each a single-key range, "
		        "which the writer sends as (begin = end, end = \"\"): KeyRangeRef's compressed form, decoded back to "
		        "[k, k\\x00)), four mutations (SetValue, ClearRange, AddValue, SetVersionstampedKey; MutationRef "
		        "type is a u8), report_conflicting_keys, lock_aware, spanContext (Optional<SpanContext>), tenantIds. "
		        "idempotencyId is a string-framed blob. 7.4 only: tenantInfo, between spanContext and idempotencyId.",
		        "CommitTransactionRequest",
		        r);
	}
	{
		CommitTransactionRequest r;
		CommitTransactionRef& t = r.transaction;
		t.mutations = vec<MutationRef>({ mutation(MutationRef::ClearRange, "single", std::string("single\x00", 7)) });
		t.write_conflict_ranges = vec<KeyRangeRef>({ KeyRangeRef(sref("single"), sref(std::string("single\x00", 7))) });
		r.reply = replyTo<CommitID>(7);
		message(g,
		        "commit_transaction_request_single_key_clear",
		        "A clear of one key: the ClearRange mutation [k, k\\x00) is written as (type, param1 = k\\x00, "
		        "param2 = \"\") and the reader restores it (MutationRef::serialize); the single-key write conflict "
		        "range is compressed the same way. Everything else default.",
		        "CommitTransactionRequest",
		        r);
	}
	{
		CommitID r(6000000000500LL,
		           17,
		           Optional<Value>(key("metadata-version")),
		           Optional<Standalone<VectorRef<int>>>(Standalone<VectorRef<int>>(vec<int>({ 0, 2 }), valueArena())));
		message(g, "commit_id", "CommitID (the commit reply) as a bare table.", "CommitID", r);
		message(g,
		        "commit_id_envelope",
		        "CommitID as sent (ErrorOr<EnsureTable<CommitID>> tag 2).",
		        replyName("CommitID"),
		        Reply<CommitID>(r));
		message(g,
		        "commit_id_error",
		        "Commit failed: ErrorOr<EnsureTable<CommitID>> tag 1, Error 1020 (not_committed).",
		        replyName("CommitID"),
		        Reply<CommitID>(Error(1020)));
	}
	{
		GetKeyServerLocationsRequest r;
		r.spanContext = span(8);
		r.begin = sref("loc/a");
		r.end = Optional<KeyRef>(sref("loc/z"));
		r.limit = 100;
		r.reverse = true;
#ifdef GV_V74
		r.tenant = tenant(47);
		r.minTenantVersion = 7000000000000LL;
#else
		r.legacyVersion = 7000000000000LL;
#endif
		r.reply = replyTo<GetKeyServerLocationsReply>(8);
		message(g,
		        "get_key_server_locations_request",
		        "CommitProxyInterface::getKeyServersLocations request (adjusted endpoint 2 on both branches). 7.4: "
		        "(begin, end, limit, reverse, reply, spanContext, tenant: TenantInfo, minTenantVersion, arena); 8.0: "
		        "tenant removed and minTenantVersion renamed legacyVersion, so legacyVersion is slot 6 where 7.4 has "
		        "tenant.",
		        "GetKeyServerLocationsRequest",
		        r);
	}
	{
		GetKeyServerLocationsReply r;
		r.results.emplace_back(KeyRangeRef(r.arena, KeyRangeRef("loc/a"_sr, "loc/m"_sr)),
		                       std::vector<StorageServerInterface>{ storageServer(1, false), storageServer(2, true) });
		r.results.emplace_back(KeyRangeRef(r.arena, KeyRangeRef("loc/m"_sr, "loc/z"_sr)),
		                       std::vector<StorageServerInterface>{ storageServer(3, false) });
		r.resultsTssMapping.emplace_back(UID(0x55000000000000ULL + 2, 0x55100000000000ULL + 2),
		                                 storageServer(4, false));
		r.resultsTagMapping.emplace_back(UID(0x55000000000000ULL + 1, 0x55100000000000ULL + 1), Tag(0, 1));
		r.resultsTagMapping.emplace_back(UID(0x55000000000000ULL + 3, 0x55100000000000ULL + 3), Tag(-2, 300));
		message(g,
		        "get_key_server_locations_reply",
		        "GetKeyServerLocationsReply as a bare table: results is a vector of pair<KeyRangeRef, "
		        "vector<StorageServerInterface>> tables; StorageServerInterface is (uniqueID, locality: LocalityData "
		        "holding a std::map<key, Optional<value>>, getValue: request_stream whose endpoint is the storage "
		        "server's base endpoint, tssPairID, acceptingRequests); resultsTagMapping pairs hold Tag, a struct "
		        "(id u16, locality i8).",
		        "GetKeyServerLocationsReply",
		        r);
		message(g,
		        "get_key_server_locations_reply_envelope",
		        "GetKeyServerLocationsReply as sent.",
		        replyName("GetKeyServerLocationsReply"),
		        Reply<GetKeyServerLocationsReply>(r));
	}
}

ClientDBInfo clientDBInfo() {
	ClientDBInfo info;
	info.id = UID(0xdb1f0, 0xdb1f1);
	info.grvProxies = { grvProxy(1), grvProxy(2) };
	info.commitProxies = { commitProxy(1), commitProxy(2) };
	info.forward = Optional<Value>(key("forward-connection-string@10.0.0.9:4500"));
	VersionHistory h(8000000000000LL);
	h.mutations.push_back_deep(h.mutations.arena(), MutationRef(MutationRef::SetValue, "\xff/conf/x"_sr, "1"_sr));
	info.history = { h };
	info.clusterId = UID(0xc105, 0xc106);
	info.clusterType = ClusterType::STANDALONE;
#ifdef GV_V74
	info.tenantMode = TenantMode::OPTIONAL_TENANT;
	EncryptKeyProxyInterface ekp(locality("ekp"), UID(0xe4b, 0xe4c));
	ekp.waitFailure = RequestStream<ReplyPromise<Void>>(endpoint(UID(0xe4b0000000000000ULL, 0xe4b0000000000000ULL)));
	info.encryptKeyProxy = ekp;
	info.metaclusterName = Optional<ClusterName>(key("meta"));
#else
	info.nativeCdcEnabled = true;
	info.nativeCdcTagCount = 3;
	CDCProxyInterface cdc;
	cdc.processId = key("cdc-process");
	cdc.consume = PublicRequestStream<CDCConsumeRequest>(endpoint(UID(0xcdc0000000000000ULL, 0xcdc0000000000000ULL)));
	cdc.supportsOrderedStreams = true;
	info.cdcProxies = { cdc };
	info.streamToCDCProxyId[11] = UID(0xcdc1, 0xcdc2);
#endif
	return info;
}

void coordinatorVectors() {
	const std::string g = "coordinator";
	{
		OpenDatabaseCoordRequest r;
		r.traceLogGroup = key("log-group");
		r.issues = Standalone<VectorRef<StringRef>>(
		    vec<StringRef>({ sref("incompatible_protocol"), sref("tls_error") }), valueArena());
		r.supportedVersions = Standalone<VectorRef<ClientVersionRef>>(
		    vec<ClientVersionRef>(
		        { ClientVersionRef(sref(GV_FDB_VERSION), sref("0123456789abcdef"), sref("fdb00b074000000")) }),
		    valueArena());
		r.knownClientInfoID = UID(0xdb1f0, 0xdb1f1);
		r.clusterKey = key("desc:id");
		r.hostnames = { Hostname("coord.example", "4500", false), Hostname("coord-tls.example", "4501", true) };
		r.coordinators = { addr4(0x0a000002, 4500), addr6(2, 4500, false) };
		r.reply = replyTo<CachedSerialization<ClientDBInfo>>(9);
		r.internal = false;
		message(g,
		        "open_database_coord_request",
		        "ClientLeaderRegInterface::openDatabase request (sent to a coordinator). coordinators holds an IPv4 "
		        "and an IPv6 NetworkAddress: ip is IPAddress, a table holding std::variant<uint32_t, "
		        "std::array<uint8_t, 16>>. The reply is a CachedSerialization<ClientDBInfo>, whose wire form is "
		        "exactly ErrorOr<EnsureTable<ClientDBInfo>> (see client_db_info_envelope).",
		        "OpenDatabaseCoordRequest",
		        r);
	}
	{
		ClientDBInfo info = clientDBInfo();
#ifdef GV_V74
		const std::string diff =
		    "7.4 slots: (grvProxies, commitProxies, id, forward, history, tenantMode (TenantMode, a table "
		    "holding mode: u32), "
		    "encryptKeyProxy (Optional<EncryptKeyProxyInterface>), clusterId, clusterType, "
		    "metaclusterName). 8.0 drops tenantMode, encryptKeyProxy and metaclusterName and appends "
		    "nativeCdcEnabled, nativeCdcTagCount, cdcProxies, streamToCDCProxyId, so clusterId is "
		    "slot 7 on 7.4 (union slots count twice) and slot 6 on 8.0.";
#else
		const std::string diff = "8.0 slots: (grvProxies, commitProxies, id, forward, history, clusterId, clusterType, "
		                         "nativeCdcEnabled, nativeCdcTagCount, cdcProxies (CDCProxyInterface), "
		                         "streamToCDCProxyId (std::map<u64, UID>)). 7.4 had tenantMode, encryptKeyProxy and "
		                         "metaclusterName instead of the four CDC fields.";
#endif
		message(g,
		        "client_db_info",
		        "ClientDBInfo as a bare table: two GRV proxies and two commit proxies (each a request_stream "
		        "endpoint: GrvProxyInterface::getConsistentReadVersion and CommitProxyInterface::commit; the other "
		        "endpoints are derived with Endpoint::getAdjustedEndpoint, commit index 1 = GRV (unused), 2 = "
		        "getKeyServersLocations on both branches). The first proxy of each kind has a secondary (TLS) "
		        "address. " +
		            diff,
		        "ClientDBInfo",
		        info);
		message(g,
		        "client_db_info_envelope",
		        "ClientDBInfo as the coordinator sends it in reply to OpenDatabaseCoordRequest "
		        "(ErrorOr<EnsureTable<ClientDBInfo>> tag 2, written from CachedSerialization).",
		        replyName("ClientDBInfo"),
		        Reply<ClientDBInfo>(info));
	}
}

void transportVectors() {
	const std::string g = "transport";
	{
		PingRequest r;
		r.reply = replyTo<Void>(10);
		message(
		    g,
		    "ping_request",
		    "FlowTransport's connection-keeper ping, sent to the well-known token WLTOKEN_PING_PACKET (UID(-1, 1)). "
		    "PingRequest lives in fdbrpc/FlowTransport.cpp; this tool mirrors it (file identifier and fields "
		    "checked against that file).",
		    "PingRequest",
		    r);
		message(g,
		        "ping_reply",
		        "The ping reply: ErrorOr<EnsureTable<Void>> tag 2.",
		        replyName("Void"),
		        Reply<Void>(Void()));
	}
	{
		UID token(0x5e9d000000000011ULL, 0x7e9d000000000011ULL);
		message(
		    g,
		    "endpoint_not_found",
		    "Payload FlowTransport sends to the well-known token WLTOKEN_ENDPOINT_NOT_FOUND (UID(-1, 0)) when a "
		    "packet arrives for an unknown endpoint: the unknown token, a UID as the root member (a 16-byte "
		    "scalar inline in the root table). WLTOKEN_UNAUTHORIZED_ENDPOINT (UID(-1, 2)) carries the same payload.",
		    "UID",
		    token);
	}
}

} // namespace

void gv_init_fdb() {
	setupNetwork(0, UseMetrics::False);
#ifdef GV_HAS_TOKEN_CACHE
	// TenantInfo's reader validates authorization tokens through the TokenCache.
	TokenCache::createInstance();
#endif
}

void build_message_vectors() {
	try {
		storageVectors();
		proxyVectors();
		coordinatorVectors();
		transportVectors();
	} catch (Error& e) {
		throw std::runtime_error(std::string("FDB error ") + std::to_string(e.code()) + " " + e.name());
	}
}
