// Schema listing: the field list of every table type reachable from the
// decodable types, in the same type vocabulary as the field dumps.
#pragma once

#include <map>
#include <stdexcept>
#include <string>
#include <tuple>
#include <typeinfo>

#include "dump.h"

struct SchemaCollector {
	std::map<std::string, Json> tables;
	std::map<std::string, std::string> cppTypes; // dump name -> typeid name, to catch name clashes
};

template <class T>
void collect_schema(SchemaCollector& c);

// Builds [{"field", "slot", "type"}] for a table by running its serializer.
struct SchemaVisitor {
	static constexpr bool is_fb_visitor = true;
	static constexpr bool isDeserializing = false;
	// Like FDB's own vtable collection: real serialize() functions skip value
	// checks (e.g. RequestStream's valid-address assert) when this is false.
	static constexpr bool isSerializing = false;
	GV_VISITOR_EXTRAS

	SchemaCollector& c;
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
	void add(const M&) {
		constexpr int slots = pack_size(detail::Fields<M>{});
		fields.push(
		    Json::object()
		        .set("field", Json::string(index < (int)names.size() ? names[index] : "f" + std::to_string(index)))
		        .set("slot", slots == 0 ? Json::null() : Json::integer(slot))
		        .set("type", Json::string(type_name<M>())));
		collect_schema<M>(c);
		++index;
		slot += slots;
	}
};

template <class Alts, size_t... I>
void collect_alternatives(SchemaCollector& c, std::index_sequence<I...>) {
	(collect_schema<index_t<I, Alts>>(c), ...);
}

template <class T>
Json table_fields(SchemaCollector& c) {
	SchemaVisitor v{ c };
	if constexpr (is_spec_v<T, std::pair>) {
		v.names = { "first", "second" };
	} else if constexpr (HasTableInfo<T>) {
		v.names = split_names(gv_table_info<T>::field_names);
	} else if constexpr (HasFieldNames<T>) {
		v.names = split_names(T::field_names);
	} else if constexpr (HasTraitFieldNames<T>) {
		v.names = split_names(serializable_traits<T>::field_names);
	}
	T obj{};
	if constexpr (serializable_traits<T>::value) {
		serializable_traits<T>::serialize(v, obj);
	} else {
		obj.serialize(v);
	}
	return v.fields;
}

template <class T>
void collect_schema(SchemaCollector& c) {
	if constexpr (!std::is_same_v<T, gv_unwrap_t<T>>) {
		collect_schema<gv_unwrap_t<T>>(c);
	} else if constexpr (gv_leaf<T>::value) {
		// Leaf type with its own dump vocabulary entry.
	} else if constexpr (is_spec_v<T, EnsureTable>) {
		collect_schema<std::decay_t<decltype(std::declval<T&>().asUnderlyingType())>>(c);
	} else if constexpr (union_like_traits<T>::value) {
		using Alts = typename union_like_traits<T>::alternatives;
		collect_alternatives<Alts>(c, std::make_index_sequence<pack_size(Alts{})>{});
	} else if constexpr (detail::is_scalar<T> || detail::is_struct_like<T> || detail::is_dynamic_size<T> ||
	                     std::is_same_v<T, std::vector<bool>>) {
		// Leaf types: struct members are scalars.
	} else if constexpr (vector_like_traits<T>::value) {
		collect_schema<typename vector_like_traits<T>::value_type>(c);
	} else {
		std::string name = type_name<T>();
		auto clash = c.cppTypes.find(name);
		if (clash != c.cppTypes.end()) {
			if (clash->second != typeid(T).name()) {
				// Several C++ types may share a dump name only if they have the same
				// fields (e.g. ReplyPromise<T> for every T: "table<reply_promise>").
				std::string key = name + "|" + typeid(T).name();
				if (c.cppTypes.count(key)) {
					return;
				}
				c.cppTypes[key] = typeid(T).name();
				if (!(table_fields<T>(c) == c.tables[name])) {
					throw std::runtime_error("two C++ types with different fields dump as " + name);
				}
			}
			return; // also stops recursion
		}
		c.cppTypes[name] = typeid(T).name();
		c.tables[name] = Json::array(); // placeholder while recursing
		c.tables[name] = table_fields<T>(c);
	}
}

// The members of the synthetic root table for a message of Ts...
template <class... Ts>
Json root_schema(SchemaCollector& c) {
	std::tuple<Ts...> items{};
	SchemaVisitor v{ c };
	for (size_t i = 0; i < sizeof...(Ts); ++i) {
		v.names.push_back(std::to_string(i));
	}
	std::apply([&](Ts&... it) { v(it...); }, items);
	return v.fields;
}
