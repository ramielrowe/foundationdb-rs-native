// Fixture records and the helpers that build them. See ../README.md for the format.
#pragma once

#include <cstdio>
#include <string>
#include <vector>

#include "codec.h"

constexpr int kFormatVersion = 1;

struct Fixture {
	std::string group;
	std::string name;
	std::string kind; // "positive" | "negative" | "corpus"
	Json record;
	std::vector<uint8_t> bytes; // positives: the message
};

struct FixtureSet {
	std::vector<Fixture> positives;
	std::vector<Fixture> negatives;
	// corpus name -> JSON-lines records
	std::vector<std::pair<std::string, std::vector<Json>>> corpora;
	std::vector<std::pair<std::string, std::string>> corpusDescriptions;
};

FixtureSet& fixtures();

// Skip negative vectors and corpora. Needed when building against a release
// branch, whose reader has no bounds checks: feeding it malformed buffers is
// undefined behaviour.
inline bool g_positiveOnly = false;

inline std::string fid_hex(FileIdentifier fid) {
	char buf[16];
	snprintf(buf, sizeof(buf), "0x%08x", fid);
	return buf;
}

inline std::vector<uint8_t> bytes_from(const uint8_t* p, size_t n) {
	return std::vector<uint8_t>(p, p + n);
}

inline Json base_record(const std::string& group,
                        const std::string& name,
                        const std::string& kind,
                        const std::string& description,
                        const std::string& cppType,
                        FileIdentifier fid,
                        const std::vector<uint8_t>& bytes) {
	Json r = Json::object();
	r.set("format_version", Json::integer(kFormatVersion));
	r.set("name", Json::string(name));
	r.set("group", Json::string(group));
	r.set("kind", Json::string(kind));
	r.set("description", Json::string(description));
	r.set("cpp_type", Json::string(cppType));
	r.set("file_identifier", Json::uinteger(fid));
	r.set("file_identifier_hex", Json::string(fid_hex(fid)));
	r.set("size", Json::uinteger(bytes.size()));
	r.set("bytes", Json::string(hex(bytes.data(), bytes.size())));
	return r;
}

inline Json decode_registered(const std::string& type, const std::vector<uint8_t>& bytes) {
	return registry().at(type).decode(bytes.data(), bytes.size(), DecodeOptions{});
}

inline std::set<std::string> strict_violations(const std::string& type, const std::vector<uint8_t>& bytes) {
	return registry().at(type).strict(bytes.data(), bytes.size());
}

inline Json string_array(const std::set<std::string>& s) {
	Json a = Json::array();
	for (const auto& x : s) {
		a.push(Json::string(x));
	}
	return a;
}

// Every positive vector must also pass the permitted strict checks: they are
// meant to accept everything a correct writer produces.
inline void require_strictly_valid(const std::string& name, const std::string& type, const std::vector<uint8_t>& bytes) {
	auto v = strict_violations(type, bytes);
	if (!v.empty()) {
		throw std::runtime_error("positive vector " + name + " fails strict check " + *v.begin());
	}
}

// A positive vector whose bytes are given (hand-built or mutated) and must decode.
inline void positive_bytes(const std::string& group,
                           const std::string& name,
                           const std::string& description,
                           const std::string& readerType,
                           const std::vector<uint8_t>& bytes,
                           const std::string& writerType = "") {
	const auto& t = registry().at(readerType);
	Json root = decode_registered(readerType, bytes);
	require_strictly_valid(name, readerType, bytes);
	Json r = base_record(group, name, "positive", description, readerType, t.fid, bytes);
	if (!writerType.empty()) {
		r.set("writer_cpp_type", Json::string(writerType));
	}
	r.set("root", root);
	fixtures().positives.push_back({ group, name, "positive", r, bytes });
}

// A positive vector: encode |items| as the root of a message, decode the bytes
// again, and check that the decoded dump equals the dump of the input.
template <class... Items>
void positive(const std::string& group,
              const std::string& name,
              const std::string& description,
              const std::string& cppType,
              FileIdentifier fid,
              const Items&... items) {
	register_type<Items...>(cppType, fid);
	auto bytes = encode(fid, items...);
	Json root = decode_registered(cppType, bytes);
	Json expected = dump_root(items...);
	if (!(root == expected)) {
		fprintf(stderr,
		        "round trip mismatch for %s\nexpected: %s\ndecoded: %s\n",
		        name.c_str(),
		        expected.dump().c_str(),
		        root.dump().c_str());
		throw std::runtime_error("round trip mismatch");
	}
	require_strictly_valid(name, cppType, bytes);
	Json r = base_record(group, name, "positive", description, cppType, fid, bytes);
	r.set("root", root);
	fixtures().positives.push_back({ group, name, "positive", r, bytes });
}

// Encode as Writer, decode as Reader (a different schema with the same file identifier).
template <class Writer, class Reader>
void positive_as(const std::string& group,
                 const std::string& name,
                 const std::string& description,
                 const std::string& writerType,
                 const std::string& readerType,
                 FileIdentifier fid,
                 const Writer& value) {
	register_type<Writer>(writerType, fid);
	register_type<Reader>(readerType, fid);
	auto bytes = encode(fid, value);
	positive_bytes(group, name, description, readerType, bytes, writerType);
}

// Runs the registered decoder; returns the rejection reason, or "" if accepted.
inline std::string try_decode(const std::string& type, const std::vector<uint8_t>& bytes, Json* root) {
	try {
		Json j = decode_registered(type, bytes);
		if (root) {
			*root = j;
		}
		return "";
	} catch (const DecodeError& e) {
		return e.what();
	}
}

// A negative vector: the reader must reject |bytes|.
inline void negative(const std::string& group,
                     const std::string& name,
                     const std::string& description,
                     const std::string& readerType,
                     const std::vector<uint8_t>& bytes,
                     Json mutation) {
	if (g_positiveOnly) {
		return;
	}
	std::string reason = try_decode(readerType, bytes, nullptr);
	if (reason.empty()) {
		throw std::runtime_error("negative vector " + name + " was accepted");
	}
	const auto& t = registry().at(readerType);
	Json r = base_record(group, name, "negative", description, readerType, t.fid, bytes);
	r.set("mutation", mutation);
	r.set("expect", Json::string("reject"));
	r.set("reason", Json::string(reason));
	fixtures().negatives.push_back({ group, name, "negative", r });
}

// One line of a corpus: whatever the reader does with |bytes|, recorded.
// Accepted entries are classified against |sourceRoot|, the decoded message
// the mutation started from (see README.md, "Corpus accept classes").
inline Json corpus_entry(const std::string& name,
                         const std::string& readerType,
                         const std::vector<uint8_t>& bytes,
                         Json mutation,
                         const Json& sourceRoot) {
	Json root;
	std::string reason = try_decode(readerType, bytes, &root);
	const auto& t = registry().at(readerType);
	Json r = Json::object();
	r.set("name", Json::string(name));
	r.set("cpp_type", Json::string(readerType));
	r.set("file_identifier", Json::uinteger(t.fid));
	r.set("bytes", Json::string(hex(bytes.data(), bytes.size())));
	r.set("mutation", mutation);
	if (reason.empty()) {
		r.set("expect", Json::string("accept"));
		auto violations = strict_violations(readerType, bytes);
		if (!violations.empty()) {
			r.set("accept_class", Json::string("structural"));
			r.set("violations", string_array(violations));
		} else if (root == sourceRoot) {
			r.set("accept_class", Json::string("padding"));
		} else {
			r.set("accept_class", Json::string("data"));
		}
		r.set("root", root);
	} else {
		r.set("expect", Json::string("reject"));
		r.set("reason", Json::string(reason));
	}
	return r;
}
