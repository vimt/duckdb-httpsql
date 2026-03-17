cmake_minimum_required(VERSION 3.5.0)
set(TARGET_NAME httpsql)
project(${TARGET_NAME})

# ── nanoarrow (IPC) ──────────────────────────────────────────────────────────
include(FetchContent)
set(NANOARROW_IPC ON)
set(NANOARROW_NAMESPACE "DuckDBExtHttpsql")
FetchContent_Declare(
  nanoarrow
  URL "https://github.com/apache/arrow-nanoarrow/archive/4bf5a9322626e95e3717e43de7616c0a256179eb.zip"
  URL_HASH SHA256=49d588ee758a2a1d099ed4525c583a04adf71ce40405011e0190aa1e75e61b59
)
FetchContent_MakeAvailable(nanoarrow)

set(EXTENSION_NAME ${TARGET_NAME}_extension)

add_subdirectory(src)

build_static_extension(${TARGET_NAME} ${ALL_OBJECT_FILES})
target_include_directories(${TARGET_NAME}_extension PRIVATE src/include)

install(
  TARGETS ${TARGET_NAME}_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})
target_include_directories(${TARGET_NAME}_loadable_extension PRIVATE src/include)

# Link nanoarrow (core + IPC) to both targets (plain signature to match upstream)
target_link_libraries(${TARGET_NAME}_extension nanoarrow nanoarrow_ipc)
target_link_libraries(${TARGET_NAME}_loadable_extension nanoarrow nanoarrow_ipc)
