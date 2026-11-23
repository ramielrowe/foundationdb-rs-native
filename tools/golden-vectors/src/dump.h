// Typed field dumps. Every serializable value becomes a node
//   {"field"?, "slot"?, "offset"?, "type", "tag"?, "value"}
// See ../README.md ("Field dump") for the exact format.
#pragma once

#include <array>
#include <deque>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <variant>
#include <vector>

#include "gv_types.h"
#include "json.h"

// ---------------------------------------------------------------------------
// Type classification helpers.
template <class T, template <class...> class Tmpl>
struct is_spec : std::false_type {};
template <template <class...> class Tmpl, class... Args>
struct is_spec<Tmpl<Args...>, Tmpl> : std::true_type {};
template <class T, template <class...> class Tmpl>
constexpr bool is_spec_v = is_spec<T, Tmpl>::value;

template <class T>
struct is_std_array : std::false_type {};
template <class T, size_t N>
struct is_std_array<std::array<T, N>> : std::true_type {};

template <class T>
concept HasFieldNames = requires { T::field_names; };
template <class T>
concept HasTraitFieldNames = requires { serializable_traits<T>::field_names; };
template <class T>
concept HasTypeName = requires { T::type_name; };

inline std::string hex(const uint8_t* p, size_t n) {
	static const char* digits = "0123456789abcdef";
	std::string s;
	s.reserve(2 * n);
	for (size_t i = 0; i < n; ++i) {
		s += digits[p[i] >> 4];
		s += digits[p[i] & 15];
	}
	return s;
}

inline std::vector<std::string> split_names(const char* names) {
	std::vector<std::string> out;
	std::string cur;
	for (const char* c = names; *c; ++c) {
		if (*c == ',') {
			out.push_back(cur);
			cur.clear();
		} else if (*c != ' ' && *c != '\n' && *c != '\t') {
			cur += *c;
		}
	}
	if (!cur.empty()) {
		out.push_back(cur);
	}
	return out;
}

template <class T>
std::string type_name();

template <class... Ts>
std::string type_list() {
	std::string s;
	((s += (s.empty() ? "" : ",") + type_name<Ts>()), ...);
	return s;
}

template <class T>
struct type_list_of;
template <template <class...> class Tmpl, class... Ts>
struct type_list_of<Tmpl<Ts...>> {
	static std::string get() { return type_list<Ts...>(); }
};

template <class T>
std::string type_name() {
	if constexpr (!std::is_same_v<T, gv_unwrap_t<T>>)
		return type_name<gv_unwrap_t<T>>();
	else if constexpr (gv_leaf<T>::value)
		return gv_leaf<T>::name();
	else if constexpr (std::is_enum_v<T>)
		return type_name<std::underlying_type_t<T>>(); // enums are scalars of their size
	else if constexpr (std::is_same_v<T, bool>)
		return "bool";
	else if constexpr (std::is_same_v<T, uint8_t>)
		return "u8";
	else if constexpr (std::is_same_v<T, int8_t>)
		return "i8";
	else if constexpr (std::is_same_v<T, uint16_t>)
		return "u16";
	else if constexpr (std::is_same_v<T, int16_t>)
		return "i16";
	else if constexpr (std::is_same_v<T, uint32_t>)
		return "u32";
	else if constexpr (std::is_same_v<T, int32_t>)
		return "i32";
	else if constexpr (std::is_same_v<T, uint64_t>)
		return "u64";
	else if constexpr (std::is_same_v<T, int64_t>)
		return "i64";
	else if constexpr (std::is_same_v<T, float>)
		return "f32";
	else if constexpr (std::is_same_v<T, double>)
		return "f64";
	else if constexpr (std::is_same_v<T, UID>)
		return "uid";
	else if constexpr (std::is_same_v<T, Arena>)
		return "arena";
	else if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, StringRef>)
		return "bytes";
#ifndef GV_REAL_FDB
	else if constexpr (std::is_same_v<T, PreserializedKVVector>)
		return "preserialized_kv_vector";
#endif
	else if constexpr (std::is_same_v<T, std::vector<bool>>)
		return "vector<bool>";
	else if constexpr (is_std_array<T>::value)
		return "array<" + type_name<typename T::value_type>() + "," + std::to_string(std::tuple_size_v<T>) + ">";
	else if constexpr (is_spec_v<T, Optional>)
		return "optional<" + type_name<typename T::ValueType>() + ">";
	else if constexpr (is_spec_v<T, ErrorOr>)
		return "error_or<" + type_name<typename T::ValueType>() + ">";
	else if constexpr (is_spec_v<T, std::variant>)
		return "variant<" + type_list_of<T>::get() + ">";
	else if constexpr (is_spec_v<T, EnsureTable>)
		return "ensure_table<" + type_name<std::decay_t<decltype(std::declval<T&>().asUnderlyingType())>>() + ">";
	else if constexpr (struct_like_traits<T>::value)
		return "struct<" + type_list_of<typename struct_like_traits<T>::types>::get() + ">";
	else if constexpr (is_spec_v<T, std::pair>)
		return "table<pair<" + type_list_of<T>::get() + ">>";
	else if constexpr (is_spec_v<T, ReplyPromise>)
		return "table<reply_promise>";
	else if constexpr (vector_like_traits<T>::value)
		return "vector<" + type_name<typename vector_like_traits<T>::value_type>() + ">";
	else if constexpr (HasTableInfo<T>)
		return std::string("table<") + gv_table_info<T>::name + ">";
	else if constexpr (HasTypeName<T>)
		return std::string("table<") + T::type_name + ">";
	else
		static_assert(sizeof(T) == 0, "no type_name for this type");
}

template <class T>
Json dump_node(const T& v);

// Builds the "value" array for a table by running its serializer against a visitor.
struct TableDumper {
	static constexpr bool is_fb_visitor = true;
	static constexpr bool isDeserializing = false;
	static constexpr bool isSerializing = true;
	GV_VISITOR_EXTRAS

	std::vector<std::string> names;
	// Per argument: the address of the data member the name refers to, or nullptr
	// (gv_table_info::addresses). Used to check names against the actual arguments.
	std::vector<const void*> addresses;
	// The same for an alternative serializer() call with the same slots (gv_table_info::alt_addresses).
	std::vector<const void*> altAddresses;
	Json fields = Json::array();
	int index = 0;
	int slot = 0;

	template <class... Members>
	void operator()(const Members&... members) {
		(add(members), ...);
	}

private:
	template <class M>
	void add(const M& m) {
		const void* at = &m;
		bool mainOk = index >= (int)addresses.size() || !addresses[index] || addresses[index] == at;
		bool altOk = index < (int)altAddresses.size() && (!altAddresses[index] || altAddresses[index] == at);
		if (!mainOk && !altOk) {
			throw std::runtime_error("serializer argument " + std::to_string(index) + " is not the member named '" +
			                         names.at(index) + "'");
		}
		Json node = Json::object();
		node.set("field", Json::string(index < (int)names.size() ? names[index] : "f" + std::to_string(index)));
		// Same rule as detail::fields_helper: zero-size members (Arena) take no slot, unions take two.
		constexpr int slots = pack_size(detail::Fields<M>{});
		node.set("slot", slots == 0 ? Json::null() : Json::integer(slot));
		node.mergeFrom(dump_node(m));
		fields.push(std::move(node));
		++index;
		slot += slots;
	}
};

template <class T>
Json dump_table(const T& v) {
	TableDumper d;
	bool named = true;
	if constexpr (is_spec_v<T, std::pair>) {
		d.names = { "first", "second" };
	} else if constexpr (HasTableInfo<T>) {
		d.names = split_names(gv_table_info<T>::field_names);
		d.addresses = gv_table_info<T>::addresses(v);
		d.altAddresses = gv_table_info<T>::alt_addresses(v);
	} else if constexpr (HasFieldNames<T>) {
		d.names = split_names(T::field_names);
	} else if constexpr (HasTraitFieldNames<T>) {
		d.names = split_names(serializable_traits<T>::field_names);
	} else {
		named = false; // dumped as f0, f1, ...
	}
	if constexpr (serializable_traits<T>::value) {
		serializable_traits<T>::serialize(d, const_cast<T&>(v));
	} else {
		const_cast<T&>(v).serialize(d);
	}
	if (named && d.index != (int)d.names.size()) {
		throw std::runtime_error(type_name<T>() + ": " + std::to_string(d.index) + " serializer arguments but " +
		                         std::to_string(d.names.size()) + " field names");
	}
	return d.fields;
}

template <class Union, size_t I = 0>
Json dump_union_alt(const Union& u, int idx) {
	using Traits = union_like_traits<Union>;
	using Alts = typename Traits::alternatives;
	if constexpr (I < pack_size(Alts{})) {
		if ((int)I == idx) {
			int dummy = 0;
			return dump_node(Traits::template get<I>(u, dummy));
		}
		return dump_union_alt<Union, I + 1>(u, idx);
	} else {
		throw std::runtime_error("bad union index");
	}
}

template <class Tuple, size_t... I>
Json dump_struct(const Tuple& t, std::index_sequence<I...>) {
	using Traits = struct_like_traits<Tuple>;
	using types = typename Traits::types;
	Json arr = Json::array();
	int ctx = 0;
	(
	    [&] {
		    Json n = Json::object();
		    n.set("field", Json::string(std::to_string(I)));
		    n.set("offset", Json::integer(detail::struct_offset<I>(types{})));
		    n.mergeFrom(dump_node(Traits::template get<I>(t, ctx)));
		    arr.push(std::move(n));
	    }(),
	    ...);
	return arr;
}

template <class T>
Json dump_node_impl(const T& v) {
	Json value;
	Json tag;
	bool hasTag = false;
	if constexpr (gv_leaf<T>::value) {
		value = gv_leaf<T>::dump(v);
	} else if constexpr (std::is_enum_v<T>) {
		return dump_node(static_cast<std::underlying_type_t<T>>(v));
	} else if constexpr (std::is_same_v<T, bool>) {
		value = Json::boolean(v);
	} else if constexpr (std::is_floating_point_v<T>) {
		value = Json::number(v);
	} else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
		value = Json::integer(v);
	} else if constexpr (std::is_integral_v<T>) {
		value = Json::uinteger(v);
	} else if constexpr (std::is_same_v<T, UID>) {
		value = Json::object().set("first", Json::uinteger(v.first())).set("second", Json::uinteger(v.second()));
	} else if constexpr (std::is_same_v<T, Arena>) {
		value = Json::null();
	} else if constexpr (std::is_same_v<T, std::string>) {
		value = Json::string(hex(reinterpret_cast<const uint8_t*>(v.data()), v.size()));
	} else if constexpr (std::is_same_v<T, StringRef>) {
		value = Json::string(hex(v.begin(), v.size()));
#ifndef GV_REAL_FDB
	} else if constexpr (std::is_same_v<T, PreserializedKVVector>) {
		value = Json::array();
		for (const auto& kv : v.items) {
			value.push(Json::object()
			               .set("key", Json::string(hex((const uint8_t*)kv.key.data(), kv.key.size())))
			               .set("value", Json::string(hex((const uint8_t*)kv.value.data(), kv.value.size()))));
		}
#endif
	} else if constexpr (std::is_same_v<T, std::vector<bool>>) {
		value = Json::array();
		for (bool b : v) {
			value.push(dump_node(b));
		}
	} else if constexpr (is_spec_v<T, EnsureTable>) {
		value = dump_node(v.asUnderlyingType());
	} else if constexpr (union_like_traits<T>::value) {
		using Traits = union_like_traits<T>;
		int dummy = 0;
		hasTag = true;
		if (Traits::empty(v, dummy)) {
			tag = Json::integer(0);
			value = Json::null();
		} else {
			int idx = Traits::index(v, dummy);
			tag = Json::integer(idx + 1);
			value = dump_union_alt<T>(v, idx);
		}
	} else if constexpr (struct_like_traits<T>::value) {
		value = dump_struct(v, std::make_index_sequence<pack_size(typename struct_like_traits<T>::types{})>{});
	} else if constexpr (vector_like_traits<T>::value) {
		value = Json::array();
		int dummy = 0;
		using VT = vector_like_traits<T>;
		auto it = VT::begin(v, dummy);
		size_t n = VT::num_entries(v, dummy);
		for (size_t i = 0; i < n; ++i, ++it) {
			// Convert to the serialized element type (e.g. std::map yields pair<const K, V>).
			typename VT::value_type element = *it;
			value.push(dump_node(element));
		}
	} else {
		value = dump_table(v);
	}
	Json n = Json::object();
	n.set("type", Json::string(type_name<T>()));
	if (hasTag) {
		n.set("tag", tag);
	}
	n.set("value", std::move(value));
	return n;
}

template <class T>
Json dump_node(const T& v) {
	if constexpr (!std::is_same_v<T, gv_unwrap_t<T>>) {
		return dump_node(static_cast<const gv_unwrap_t<T>&>(v)); // e.g. Standalone<T> is serialized as T
	} else {
		return dump_node_impl(v);
	}
}

// The synthetic root wrapper (detail::FakeRoot) around the serialized members.
template <class... Ts>
Json dump_root(const Ts&... items) {
	TableDumper d;
	for (size_t i = 0; i < sizeof...(Ts); ++i) {
		d.names.push_back(std::to_string(i));
	}
	d(items...);
	Json n = Json::object();
	n.set("type", Json::string("table<root>"));
	n.set("value", d.fields);
	return n;
}
