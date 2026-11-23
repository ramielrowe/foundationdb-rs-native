# The message-vector generator, built inside FoundationDB's own CMake build (see
# project_include.cmake and ../regen_messages.sh). It links fdbclient, so it
# serializes the real message types of the branch being built.
#
#   GV_RELEASE_NAME   fixture directory name, e.g. release-7.4 (required)
set(GV_MESSAGES_DIR "${CMAKE_CURRENT_LIST_DIR}")
set(GV_GEN_DIR "${CMAKE_BINARY_DIR}/golden_vectors_messages")
if(NOT GV_RELEASE_NAME)
  message(FATAL_ERROR "golden-vectors: set -DGV_RELEASE_NAME=<fixture directory name>")
endif()

# Field names of every table type, taken from this branch's serializer() calls.
find_package(Python3 REQUIRED COMPONENTS Interpreter)
add_custom_command(
  OUTPUT "${GV_GEN_DIR}/gv_fields.g.h"
  COMMAND ${CMAKE_COMMAND} -E make_directory "${GV_GEN_DIR}"
  COMMAND Python3::Interpreter "${GV_MESSAGES_DIR}/extract_fields.py" "${CMAKE_SOURCE_DIR}"
          "${GV_GEN_DIR}/gv_fields.g.h"
  DEPENDS "${GV_MESSAGES_DIR}/extract_fields.py"
  COMMENT "golden-vectors: extracting serializer field names")

add_executable(golden_vectors_messages "${GV_MESSAGES_DIR}/message_vectors.cpp" "${GV_MESSAGES_DIR}/../src/main.cpp"
                                       "${GV_GEN_DIR}/gv_fields.g.h")
target_include_directories(golden_vectors_messages PRIVATE "${GV_MESSAGES_DIR}" "${GV_MESSAGES_DIR}/../src"
                                                           "${GV_GEN_DIR}")
target_compile_definitions(
  golden_vectors_messages PRIVATE GV_REAL_FDB=1 GV_RELEASE_NAME="${GV_RELEASE_NAME}" GV_FDB_MAJOR=${FDB_MAJOR}
                                  GV_FDB_MINOR=${FDB_MINOR} GV_FDB_VERSION="${FDB_VERSION}")
# The generated gv_table_info specializations name private data members.
target_compile_options(golden_vectors_messages PRIVATE -fno-access-control -Wno-unused-parameter)
target_link_libraries(golden_vectors_messages PRIVATE fdbclient)

# The binary-format (BinaryWriter/BinaryReader) vectors: real flow and fdbclient types,
# written and read back by this release's own code. See binary_vectors.cpp.
add_executable(golden_vectors_binary "${GV_MESSAGES_DIR}/binary_vectors.cpp")
target_include_directories(golden_vectors_binary PRIVATE "${GV_MESSAGES_DIR}" "${GV_MESSAGES_DIR}/../src")
target_compile_definitions(golden_vectors_binary PRIVATE GV_RELEASE_NAME="${GV_RELEASE_NAME}"
                                                         GV_FDB_VERSION="${FDB_VERSION}")
target_compile_options(golden_vectors_binary PRIVATE -Wno-unused-parameter)
target_link_libraries(golden_vectors_binary PRIVATE fdbclient)
