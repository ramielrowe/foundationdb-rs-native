// Out-of-line pieces of the flatbuffers engine that live in flow/flat_buffers.cpp.
// generate_vtable is copied verbatim from that file (identical on release-7.4,
// release-8.0 and main); the load-failure hooks are replaced by ones that throw
// DecodeError instead of tracing and throwing serialization_failed / aborting.
#include "stubs.h"

#include <algorithm>

namespace detail {

namespace {
thread_local std::vector<int> gWriteToOffsetsMemory;
}

void swapWithThreadLocalGlobal(std::vector<int>& writeToOffsets) {
	gWriteToOffsetsMemory.swap(writeToOffsets);
}

// Declared only by main's flat_buffers.h; harmless extra definitions on release branches.
[[noreturn]] void flat_buffer_load_failed(const char* reason) {
	throw DecodeError(reason);
}

[[noreturn]] void flat_buffer_load_fatal(const char* reason) {
	throw DecodeError(reason);
}

// --- copied from flow/flat_buffers.cpp ---
VTable generate_vtable(size_t numMembers, const std::vector<unsigned>& sizesAlignments) {
	if (numMembers == 0) {
		return VTable{ 4, 4 };
	}
	// first is index, second is size
	std::vector<std::pair<unsigned, unsigned>> indexed;
	indexed.reserve(numMembers);
	for (unsigned i = 0; i < numMembers; ++i) {
		if (sizesAlignments[i] > 0) {
			indexed.emplace_back(i, sizesAlignments[i]);
		}
	}
	std::stable_sort(indexed.begin(),
	                 indexed.end(),
	                 [](const std::pair<unsigned, unsigned>& lhs, const std::pair<unsigned, unsigned>& rhs) {
		                 return lhs.second > rhs.second;
	                 });
	VTable result;
	result.resize(numMembers + 2);
	// size of the vtable is
	// - 2 bytes per member +
	// - 2 bytes for the size entry +
	// - 2 bytes for the size of the object
	result[0] = 2 * numMembers + 4;
	int offset = 0;
	for (auto p : indexed) {
		auto align = sizesAlignments[numMembers + p.first];
		auto& res = result[p.first + 2];
		res = offset % align == 0 ? offset : ((offset / align) + 1) * align;
		offset = res + p.second;
		res += 4;
	}
	result[1] = offset + 4;
	return result;
}
// --- end copy ---

} // namespace detail
