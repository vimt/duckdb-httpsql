cmake_minimum_required(VERSION 3.5.0)
set(TARGET_NAME httpsql)
project(${TARGET_NAME})

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
