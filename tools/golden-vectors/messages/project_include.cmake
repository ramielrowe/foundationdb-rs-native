# Injected into FoundationDB's top-level project() call with
#   -DCMAKE_PROJECT_foundationdb_INCLUDE=<this file>
# so the message-vector generator is built as a target of FDB's own build
# without modifying the exported FDB source tree. targets.cmake is included at
# the end of FDB's top-level CMakeLists.txt (a deferred call), when the
# fdbclient target exists. (A deferred add_subdirectory is not allowed, and the
# arguments of a deferred call are evaluated late, hence EVAL.)
cmake_language(EVAL CODE
               "cmake_language(DEFER DIRECTORY [[${CMAKE_SOURCE_DIR}]] CALL include [[${CMAKE_CURRENT_LIST_DIR}/targets.cmake]])")
