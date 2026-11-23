// FlowTransport's ConnectPacket, mirrored from fdbrpc/FlowTransport.actor.cpp (release-7.4) and
// fdbrpc/FlowTransport.cpp (release-8.0), where it is a file-local type and so cannot be
// included. The struct is the same on both branches except for the TraceEvent line of
// serialize(), which is omitted here; messages/check_mirrors.py (run by regen_messages.sh
// before the build) compares this struct, line by line and without comments, with the branch's
// source and fails the build if they differ. It is in an anonymous namespace so that it is a
// different type from the one in fdbrpc's object file.
#pragma once

#include "flow/flow.h"
#include "flow/network.h"
#include "flow/serialize.h"

namespace {

// BEGIN MIRROR
#pragma pack(push, 1)
struct ConnectPacket {
	// The value does not include the size of `connectPacketLength` itself,
	// but only the other fields of this structure.
	uint32_t connectPacketLength = 0;
	ProtocolVersion protocolVersion; // Expect currentProtocolVersion

	uint16_t canonicalRemotePort = 0; // Port number to reconnect to the originating process
	uint64_t connectionId = 0; // Multi-version clients will use the same Id for both connections, other connections
	                           // will set this to zero. Added at protocol Version 0x0FDB00A444020001LL.

	// IP Address to reconnect to the originating process. Only one of these must be populated.
	uint32_t canonicalRemoteIp4 = 0;

	enum ConnectPacketFlags { FLAG_IPV6 = 1 };
	uint16_t flags = 0;
	uint8_t canonicalRemoteIp6[16] = { 0 };

	ConnectPacket() = default;

	IPAddress canonicalRemoteIp() const {
		if (isIPv6()) {
			IPAddress::IPAddressStore store;
			memcpy(store.data(), canonicalRemoteIp6, sizeof(canonicalRemoteIp6));
			return IPAddress(store);
		} else {
			return IPAddress(canonicalRemoteIp4);
		}
	}

	void setCanonicalRemoteIp(const IPAddress& ip) {
		if (ip.isV6()) {
			flags = flags | FLAG_IPV6;
			memcpy(&canonicalRemoteIp6, ip.toV6().data(), 16);
		} else {
			flags = flags & ~FLAG_IPV6;
			canonicalRemoteIp4 = ip.toV4();
		}
	}

	bool isIPv6() const { return flags & FLAG_IPV6; }

	uint32_t totalPacketSize() const { return connectPacketLength + sizeof(connectPacketLength); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, connectPacketLength);
		if (connectPacketLength > sizeof(ConnectPacket) - sizeof(connectPacketLength)) {
			ASSERT(!g_network->isSimulated());
			throw serialization_failed();
		}

		serializer(ar, protocolVersion, canonicalRemotePort, connectionId, canonicalRemoteIp4);
		// We can send everything in serialized packet, since the current version of ConnectPacket
		// is backward compatible with CONNECT_PACKET_V0.
		serializer(ar, flags);
		ar.serializeBytes(&canonicalRemoteIp6, sizeof(canonicalRemoteIp6));
	}
};
#pragma pack(pop)
// END MIRROR

} // namespace
