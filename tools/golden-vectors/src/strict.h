// A schema-driven structural checker that applies the checks a stricter
// decoder is *permitted* (not required) to make beyond what FDB main's reader
// checks. It is used only to classify corpus entries that the C++ reader
// accepts; see README.md ("Corpus accept classes") for the list of checks.
//
// It assumes nothing about the bytes: every read is bounds-checked, and
// anything out of bounds is reported as "out_of_bounds" (which only happens on
// paths the C++ reader does not follow, since it accepted the buffer).
//
// It also counts the "work" of one decode from the root, as README.md ("Corpus
// accept classes", `work_limit`) and fdb-native-wire's Reader define it, and
// reports "work_limit" when the count exceeds 4 * buffer length + 1024. The walk
// ends at that point (an aliasing bomb would otherwise take millions of steps),
// so violations that lie after it are not listed; a decoder that enforces the
// limit stops there as well.
#pragma once

#include <climits>
#include <cstring>
#include <set>
#include <string>
#include <tuple>

#include "dump.h"
#include "gv_types.h"

class StrictWalker {
public:
	StrictWalker(const uint8_t* b, size_t n) : b(b), n(n), budget(4 * n + 1024) {}

	std::set<std::string> violations;

	// Units of work charged so far, and the limit they are checked against.
	size_t work() const { return spent; }
	size_t workLimit() const { return budget; }

	template <class... Ts>
	void root() {
		if (n < 8) {
			return;
		}
		size_t t = follow(0);
		std::tuple<Ts...> items{};
		std::apply(
		    [&](Ts&... it) {
			    detail::FakeRoot<Ts...> r(it...);
			    walkTable(r, t);
		    },
		    items);
	}

private:
	static constexpr size_t npos = SIZE_MAX;
	const uint8_t* b;
	size_t n;
	int depth = 0;
	long steps = 0;
	size_t budget; // the work limit: 4 * n + 1024 units
	size_t spent = 0;
	bool over = false; // the work limit was exceeded: the walk is over

	void flag(const char* v) { violations.insert(v); }
	// Charges |units| of work. Past the budget it flags work_limit and ends the walk.
	void charge(size_t units) {
		if (over) {
			return;
		}
		spent += units;
		if (spent > budget) {
			over = true;
			flag("work_limit");
		}
	}
	bool inb(size_t p, size_t len) const { return p != npos && p <= n && len <= n - p; }
	uint32_t r32(size_t p) const {
		uint32_t x;
		memcpy(&x, b + p, 4);
		return x;
	}
	uint16_t r16(size_t p) const {
		uint16_t x;
		memcpy(&x, b + p, 2);
		return x;
	}

	// Target of the uoffset stored at p, or npos.
	size_t follow(size_t p) {
		if (!inb(p, 4)) {
			flag("out_of_bounds");
			return npos;
		}
		uint32_t off = r32(p);
		if (off == 0 && p >= 8) {
			flag("zero_uoffset"); // the target is the offset field itself
		}
		size_t t = p + off;
		if (t > n) {
			flag("out_of_bounds");
			return npos;
		}
		if (t < 8) {
			flag("offset_into_header");
		}
		return t;
	}

	struct Ctx {
		size_t t; // table start
		size_t vt; // vtable start
		size_t vlen; // vtable length in u16 entries (as the C++ reader computes it)
		size_t tsize; // table size field
		size_t i; // next vtable entry index
	};

	struct TableVisitor {
		static constexpr bool is_fb_visitor = true;
		static constexpr bool isDeserializing = false;
		static constexpr bool isSerializing = false; // a type walk, like FDB's vtable collection
		GV_VISITOR_EXTRAS
		StrictWalker& w;
		size_t t;
		template <class... Ms>
		void operator()(Ms&... ms) {
			w.members(t, ms...);
		}
	};

	template <class T>
	void walkTable(T& obj, size_t t) {
		if (t == npos || over) {
			return;
		}
		if (++steps > 1000000 || depth > 64) {
			flag("too_deep");
			return;
		}
		charge(4); // a table, or a zero-field table
		if (over) {
			return;
		}
		++depth;
		TableVisitor v{ *this, t };
		if constexpr (serializable_traits<T>::value) {
			serializable_traits<T>::serialize(v, obj);
		} else {
			obj.serialize(v);
		}
		--depth;
	}

	template <class... Ms>
	void members(size_t t, Ms&... ms) {
		if constexpr (sizeof...(Ms) == 0) {
			// The C++ reader never follows an offset to a zero-field table.
			if (!inb(t, 4)) {
				flag("empty_table_offset");
			}
		} else {
			if (!inb(t, 4)) {
				flag("out_of_bounds");
				return;
			}
			int64_t so = static_cast<int32_t>(r32(t));
			int64_t vt = static_cast<int64_t>(t) - so;
			if (vt < 0 || !inb(static_cast<size_t>(vt), 4)) {
				flag("out_of_bounds");
				return;
			}
			if (vt < 8) {
				flag("offset_into_header");
			}
			if (vt % 2) {
				flag("vtable_alignment");
			}
			uint16_t vsize = r16(vt), tsize = r16(vt + 2);
			if (vsize < 4 || vsize % 2) {
				flag("vtable_size");
			}
			if (!inb(vt, vsize)) {
				flag("out_of_bounds");
				return;
			}
			if (tsize < 4 || !inb(t, tsize)) {
				flag("table_size");
			}
			Ctx c{ t, static_cast<size_t>(vt), static_cast<size_t>(vsize / 2), tsize, 2 };
			(member(c, ms), ...);
		}
	}

	// Offset of slot i's field in the table, or 0 if absent.
	size_t entry(Ctx& c, size_t i, size_t size) {
		if (i >= c.vlen) {
			return 0;
		}
		uint16_t e = r16(c.vt + 2 * i);
		if (e == 0) {
			return 0;
		}
		if (e < 4) {
			flag("vtable_entry"); // the C++ reader treats 1..3 as absent
			return 0;
		}
		if (e + size > c.tsize) {
			flag("table_size");
		}
		if (!inb(c.t + e, size)) {
			flag("out_of_bounds");
			return 0;
		}
		return e;
	}

	template <class M>
	void member(Ctx& c, M&) {
		memberOfType<gv_unwrap_t<std::remove_cv_t<M>>>(c);
	}

	template <class M>
	void memberOfType(Ctx& c) {
		if (over) {
			return;
		}
		if constexpr (detail::is_vector_of_union_like<M>) {
			size_t e1 = entry(c, c.i, 4), e2 = entry(c, c.i + 1, 4);
			c.i += 2;
			if (!e1 || !e2) {
				return; // absent, or rejected by the C++ reader
			}
			size_t tv = follow(c.t + e1), ov = follow(c.t + e2);
			if (!inb(tv, 4) || !inb(ov, 4)) {
				flag("out_of_bounds");
				return;
			}
			uint32_t tc = r32(tv), oc = r32(ov);
			if (tc != oc) {
				flag("union_vector_count_mismatch");
			}
			using U = typename vector_like_traits<M>::value_type;
			charge(5 * size_t(oc)); // a vector of unions
			for (uint32_t k = 0; k < oc && !over; ++k) {
				if (!inb(tv + 4 + k, 1) || !inb(ov + 4 + 4 * size_t(k), 4)) {
					flag("out_of_bounds");
					return;
				}
				unionPayload<U>(b[tv + 4 + k], ov + 4 + 4 * size_t(k));
			}
		} else if constexpr (detail::is_union_like<M>) {
			size_t e1 = entry(c, c.i, 1), e2 = entry(c, c.i + 1, 4);
			c.i += 2;
			if (!e1) {
				return;
			}
			uint8_t tag = b[c.t + e1];
			if (tag != 0 && !e2) {
				flag("union_slots");
				return;
			}
			unionPayload<M>(tag, e2 ? c.t + e2 : npos);
		} else if constexpr (detail::_SizeOf<M>::size == 0) {
			// Arena: no slot.
		} else {
			size_t e = entry(c, c.i, detail::fb_size<M>);
			c.i += 1;
			if (e) {
				value<M>(c.t + e);
			}
		}
	}

	template <class U>
	void unionPayload(uint8_t tag, size_t offPos) {
		using Alts = typename union_like_traits<U>::alternatives;
		constexpr size_t na = pack_size(Alts{});
		if (tag == 0) {
			if (!is_spec_v<U, Optional>) {
				flag("union_tag_range"); // only Optional can be empty
			}
			return;
		}
		if (tag > na || offPos == npos) {
			if (tag > na) {
				flag("union_tag_range");
			}
			return;
		}
		alternative<Alts>(tag - 1, offPos);
	}

	template <class Alts, size_t I = 0>
	void alternative(size_t idx, size_t offPos) {
		if constexpr (I < pack_size(Alts{})) {
			if (idx == I) {
				using A = index_t<I, Alts>;
				if constexpr (detail::use_indirection<A>) {
					value<A>(offPos);
				} else {
					size_t p = follow(offPos);
					if (!inb(p, detail::fb_size<A>)) {
						flag("out_of_bounds");
					} else {
						charge(detail::fb_size<A>); // an out-of-line scalar or struct
						value<A>(p);
					}
				}
				return;
			}
			alternative<Alts, I + 1>(idx, offPos);
		}
	}

	template <class Tuple, size_t... I>
	void structMembers(size_t pos, std::index_sequence<I...>) {
		using types = typename struct_like_traits<Tuple>::types;
		(value<index_t<I, types>>(pos + detail::struct_offset<I>(types{})), ...);
	}

	// pos is where a field of type T is stored: inline bytes for scalars and
	// structs, a uoffset for everything else.
	template <class T>
	void value(size_t pos) {
		if (over) {
			return;
		}
		if constexpr (!std::is_same_v<T, gv_unwrap_t<T>>) {
			value<gv_unwrap_t<T>>(pos); // e.g. Standalone<T> is serialized as T
		} else if constexpr (detail::is_scalar<T>) {
			if constexpr (std::is_same_v<T, bool>) {
				if (b[pos] > 1) {
					flag("bool_value"); // undefined behaviour in the C++ reader
				}
			}
		} else if constexpr (detail::is_struct_like<T>) {
			structMembers<T>(pos, std::make_index_sequence<pack_size(typename struct_like_traits<T>::types{})>{});
		} else if constexpr (is_spec_v<T, EnsureTable>) {
			using I = std::decay_t<decltype(std::declval<T&>().asUnderlyingType())>;
			if constexpr (detail::expect_serialize_member<I>) {
				value<I>(pos);
			} else {
				I inner{};
				detail::FakeRoot<I> r(inner);
				walkTable(r, follow(pos));
			}
		} else if constexpr (detail::is_dynamic_size<T>) {
			size_t p = follow(pos);
			if (!inb(p, 4) || !inb(p + 4, r32(p))) {
				flag("out_of_bounds");
			} else {
				charge(4 + size_t(r32(p))); // a string or blob
			}
		} else if constexpr (detail::is_vector_like<T>) {
			using E = typename vector_like_traits<T>::value_type;
			size_t p = follow(pos);
			if (!inb(p, 4)) {
				flag("out_of_bounds");
				return;
			}
			size_t count = r32(p);
			if (!inb(p + 4, count * size_t(detail::fb_size<E>))) {
				flag("out_of_bounds");
				return;
			}
			charge(4 + count * size_t(detail::fb_size<E>)); // a vector
			if constexpr (std::is_same_v<T, std::vector<bool>>) {
				return; // vector<bool> treats any nonzero byte as true
			} else if constexpr (detail::is_scalar<E> && !std::is_same_v<E, bool>) {
				return;
			} else {
				for (size_t k = 0; k < count && !over; ++k) {
					value<E>(p + 4 + k * detail::fb_size<E>);
				}
			}
		} else {
			T obj{};
			walkTable(obj, follow(pos));
		}
	}
};
