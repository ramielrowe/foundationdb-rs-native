// Customization points shared by the dumper, the schema lister and the strict
// checker. The engine build (stubs.h) uses none of them; the message build
// (../messages/real_types.h, compiled against a real FDB tree) specializes them
// for the real flow/fdbrpc/fdbclient types.
#pragma once

#include <string>
#include <type_traits>
#include <vector>

// gv_unwrap<T>::type is the type T is serialized as. Standalone<T> is
// serialized exactly as T (flow/ObjectSerializer.h, LoadSaveHelper<Standalone<T>>).
template <class T>
struct gv_unwrap {
	using type = T;
};
template <class T>
using gv_unwrap_t = typename gv_unwrap<T>::type;

// A leaf type with its own dump vocabulary entry (a dynamic-size blob with an
// inner structure, e.g. TagSet). Specializations provide
//   static std::string name();
//   static Json dump(const T&);   (Json from json.h)
template <class T>
struct gv_leaf : std::false_type {};

// The dump name and serializer argument names of a table type that does not
// carry them itself (the engine test types have type_name/field_names members).
// Specializations provide
//   static constexpr const char* name;          e.g. "GetValueRequest"
//   static constexpr const char* field_names;   comma-separated
//   static std::vector<const void*> addresses(const T&);
//   static std::vector<const void*> alt_addresses(const T&);
// addresses() returns, per serializer argument, the address of the data member
// the argument names (nullptr where the argument is not a plain data member),
// so the dumper can check that every name is attached to the right member.
// alt_addresses() is the same for an alternative serializer() call with the same
// slots (e.g. a single-key KeyRangeRef is written as (end, "")), or empty.
template <class T>
struct gv_table_info : std::false_type {};

template <class T>
concept HasTableInfo = gv_table_info<T>::value;
