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

#include "json.h"
#include "stubs.h"

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
	if constexpr (std::is_same_v<T, bool>)
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
	else if constexpr (std::is_same_v<T, PreserializedKVVector>)
		return "preserialized_kv_vector";
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
	else if constexpr (is_spec_v<T, std::tuple>)
		return "struct<" + type_list_of<T>::get() + ">";
	else if constexpr (is_spec_v<T, std::pair>)
		return "table<pair<" + type_list_of<T>::get() + ">>";
	else if constexpr (is_spec_v<T, ReplyPromise>)
		return "table<reply_promise>";
	else if constexpr (vector_like_traits<T>::value)
		return "vector<" + type_name<typename vector_like_traits<T>::value_type>() + ">";
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

	std::vector<std::string> names;
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
	if constexpr (is_spec_v<T, std::pair>) {
		d.names = { "first", "second" };
	} else if constexpr (HasFieldNames<T>) {
		d.names = split_names(T::field_names);
	} else if constexpr (HasTraitFieldNames<T>) {
		d.names = split_names(serializable_traits<T>::field_names);
	}
	if constexpr (serializable_traits<T>::value) {
		serializable_traits<T>::serialize(d, const_cast<T&>(v));
	} else {
		const_cast<T&>(v).serialize(d);
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
	using types = typename struct_like_traits<Tuple>::types;
	Json arr = Json::array();
	(
	    [&] {
		    Json n = Json::object();
		    n.set("field", Json::string(std::to_string(I)));
		    n.set("offset", Json::integer(detail::struct_offset<I>(types{})));
		    n.mergeFrom(dump_node(std::get<I>(t)));
		    arr.push(std::move(n));
	    }(),
	    ...);
	return arr;
}

template <class T>
Json dump_node(const T& v) {
	Json value;
	Json tag;
	bool hasTag = false;
	if constexpr (std::is_same_v<T, bool>) {
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
	} else if constexpr (std::is_same_v<T, PreserializedKVVector>) {
		value = Json::array();
		for (const auto& kv : v.items) {
			value.push(Json::object()
			               .set("key", Json::string(hex((const uint8_t*)kv.key.data(), kv.key.size())))
			               .set("value", Json::string(hex((const uint8_t*)kv.value.data(), kv.value.size()))));
		}
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
		value = dump_struct(v, std::make_index_sequence<std::tuple_size_v<T>>{});
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
