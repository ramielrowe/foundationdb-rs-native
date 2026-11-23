# Builds the golden-vector generator against FoundationDB's flow headers.
# Dev-only: nothing in the Rust workspace depends on this.
#
#   make                       # build build/golden-vectors against $(FDB_SRC)
#   make FDB_SRC=/path/to/fdb  # use another FoundationDB checkout or extracted tree
#   make SANITIZE=1            # build with ASan + UBSan (used by regen.sh --check); run it with
#                              # UBSAN_OPTIONS=suppressions=ubsan.supp:halt_on_error=1
#   make CXX=clang++ CXXFLAGS=-O2
#   make clean
#
# CXXFLAGS, CPPFLAGS and LDFLAGS given on the command line are kept; the flags
# the tool needs (-std=c++20, include paths, sanitizers) are always appended.

TOOL_DIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
FDB_SRC ?= $(abspath $(TOOL_DIR)/../../../foundationdb)
BUILD_DIR ?= $(TOOL_DIR)/build

# Make's default CXX is g++; any C++20 compiler works (tested with g++ 15 and clang).
CXXFLAGS ?= -O1 -g
override CXXFLAGS += -std=c++20 -Wall -Wextra -Wno-unused-parameter -Wno-missing-field-initializers \
                     -Wno-sign-compare -Wno-unused-const-variable
override CPPFLAGS += -I$(FDB_SRC)/flow/include -I$(TOOL_DIR)/src

ifeq ($(SANITIZE),1)
  # Every UBSan check aborts, except that alignment is made recoverable so that
  # ubsan.supp can suppress it inside flat_buffers.h only (FDB reader UB on
  # hostile input, see that file). Run with halt_on_error=1 so any other
  # alignment report still aborts.
  override CXXFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=all \
                       -fsanitize-recover=alignment
  override LDFLAGS += -fsanitize=address,undefined
  BIN := $(BUILD_DIR)/golden-vectors-san
  OBJ_DIR := $(BUILD_DIR)/obj-san
else
  BIN := $(BUILD_DIR)/golden-vectors
  OBJ_DIR := $(BUILD_DIR)/obj
endif

SRCS := $(wildcard $(TOOL_DIR)/src/*.cpp)
OBJS := $(patsubst $(TOOL_DIR)/src/%.cpp,$(OBJ_DIR)/%.o,$(SRCS))
DEPS := $(OBJS:.o=.d)

HEADERS_CHECK := $(FDB_SRC)/flow/include/flow/flat_buffers.h

.PHONY: all clean check-fdb

all: check-fdb $(BIN)

check-fdb:
	@test -f $(HEADERS_CHECK) || { echo "FDB_SRC=$(FDB_SRC) has no flow/include/flow/flat_buffers.h; set FDB_SRC" >&2; exit 1; }

$(BIN): $(OBJS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

$(OBJ_DIR)/%.o: $(TOOL_DIR)/src/%.cpp $(TOOL_DIR)/Makefile
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -c -o $@ $<

clean:
	rm -rf $(BUILD_DIR)

-include $(DEPS)
