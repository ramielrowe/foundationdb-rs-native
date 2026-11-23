// Selects the serializable types the tool is built against: the stand-ins in
// stubs.h (engine vectors, compiled against flow headers only), or the real
// FDB types (message vectors, compiled inside an FDB build; GV_REAL_FDB is set
// by ../messages/targets.cmake).
#pragma once

#include "hooks.h"

#ifdef GV_REAL_FDB
#include "real_types.h" // ../messages/real_types.h
#else
#include "stubs.h"
// Extra members every flatbuffers visitor and context in this tool declares.
// Real FDB serialize() functions may call ar.protocolVersion(); the stubs never do.
#define GV_VISITOR_EXTRAS
#endif
