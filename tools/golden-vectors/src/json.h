// A tiny ordered JSON value with a deterministic printer. Objects keep
// insertion order so fixture files diff cleanly.
#pragma once

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

class Json {
public:
	enum class Kind { Null, Bool, Int, UInt, Double, String, Array, Object };

	Json() = default;
	static Json null() { return Json(); }
	static Json boolean(bool b) {
		Json j;
		j.kind = Kind::Bool;
		j.b = b;
		return j;
	}
	static Json integer(int64_t v) {
		Json j;
		j.kind = Kind::Int;
		j.i = v;
		return j;
	}
	static Json uinteger(uint64_t v) {
		Json j;
		j.kind = Kind::UInt;
		j.u = v;
		return j;
	}
	static Json number(double v) {
		if (!std::isfinite(v)) {
			throw std::runtime_error("non-finite double cannot be represented in a fixture");
		}
		Json j;
		j.kind = Kind::Double;
		j.d = v;
		return j;
	}
	static Json string(std::string s) {
		Json j;
		j.kind = Kind::String;
		j.s = std::move(s);
		return j;
	}
	static Json array() {
		Json j;
		j.kind = Kind::Array;
		return j;
	}
	static Json object() {
		Json j;
		j.kind = Kind::Object;
		return j;
	}

	Json& push(Json v) {
		arr.push_back(std::move(v));
		return *this;
	}
	Json& set(const std::string& key, Json v) {
		for (auto& kv : obj) {
			if (kv.first == key) {
				kv.second = std::move(v);
				return *this;
			}
		}
		obj.emplace_back(key, std::move(v));
		return *this;
	}

	// Appends every key of the object |o| to this object.
	Json& mergeFrom(const Json& o) {
		for (const auto& kv : o.obj) {
			set(kv.first, kv.second);
		}
		return *this;
	}

	bool operator==(const Json& o) const {
		if (kind != o.kind)
			return false;
		switch (kind) {
		case Kind::Null:
			return true;
		case Kind::Bool:
			return b == o.b;
		case Kind::Int:
			return i == o.i;
		case Kind::UInt:
			return u == o.u;
		case Kind::Double:
			return d == o.d;
		case Kind::String:
			return s == o.s;
		case Kind::Array:
			return arr == o.arr;
		case Kind::Object:
			return obj == o.obj;
		}
		return false;
	}

	// indent < 0 prints on one line.
	std::string dump(int indent = 2) const {
		std::string out;
		write(out, indent, 0);
		return out;
	}

private:
	static void escape(std::string& out, const std::string& s) {
		out += '"';
		for (unsigned char c : s) {
			switch (c) {
			case '"':
				out += "\\\"";
				break;
			case '\\':
				out += "\\\\";
				break;
			case '\n':
				out += "\\n";
				break;
			case '\t':
				out += "\\t";
				break;
			case '\r':
				out += "\\r";
				break;
			default:
				if (c < 0x20 || c >= 0x7f) {
					char buf[8];
					snprintf(buf, sizeof(buf), "\\u%04x", c);
					out += buf;
				} else {
					out += static_cast<char>(c);
				}
			}
		}
		out += '"';
	}

	static void newline(std::string& out, int indent, int depth) {
		if (indent >= 0) {
			out += '\n';
			out.append(static_cast<size_t>(indent * depth), ' ');
		}
	}

	// Arrays whose elements are all scalars print on one line to keep fixtures compact.
	bool flat() const {
		for (const auto& e : arr) {
			if (e.kind == Kind::Array || e.kind == Kind::Object)
				return false;
		}
		return true;
	}

	void write(std::string& out, int indent, int depth) const {
		char buf[64];
		switch (kind) {
		case Kind::Null:
			out += "null";
			break;
		case Kind::Bool:
			out += b ? "true" : "false";
			break;
		case Kind::Int:
			snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(i));
			out += buf;
			break;
		case Kind::UInt:
			snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(u));
			out += buf;
			break;
		case Kind::Double:
			snprintf(buf, sizeof(buf), "%.17g", d);
			out += buf;
			if (std::string(buf).find_first_of(".eE") == std::string::npos) {
				out += ".0";
			}
			break;
		case Kind::String:
			escape(out, s);
			break;
		case Kind::Array:
			if (arr.empty()) {
				out += "[]";
			} else if (flat()) {
				out += '[';
				for (size_t k = 0; k < arr.size(); ++k) {
					if (k)
						out += indent >= 0 ? ", " : ",";
					arr[k].write(out, -1, 0);
				}
				out += ']';
			} else {
				out += '[';
				for (size_t k = 0; k < arr.size(); ++k) {
					if (k)
						out += ',';
					newline(out, indent, depth + 1);
					arr[k].write(out, indent, depth + 1);
				}
				newline(out, indent, depth);
				out += ']';
			}
			break;
		case Kind::Object:
			if (obj.empty()) {
				out += "{}";
				break;
			}
			out += '{';
			for (size_t k = 0; k < obj.size(); ++k) {
				if (k)
					out += ',';
				newline(out, indent, depth + 1);
				escape(out, obj[k].first);
				out += indent >= 0 ? ": " : ":";
				obj[k].second.write(out, indent, depth + 1);
			}
			newline(out, indent, depth);
			out += '}';
			break;
		}
	}

	Kind kind = Kind::Null;
	bool b = false;
	int64_t i = 0;
	uint64_t u = 0;
	double d = 0;
	std::string s;
	std::vector<Json> arr;
	std::vector<std::pair<std::string, Json>> obj;
};
