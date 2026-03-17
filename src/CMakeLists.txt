include_directories(include)
# yyjson from DuckDB third_party
include_directories(${CMAKE_SOURCE_DIR}/third_party/yyjson/include)

add_library(
  httpsql_library OBJECT
  httpsql_extension.cpp
  httpsql_storage.cpp
  httpsql_catalog.cpp
  httpsql_schema_entry.cpp
  httpsql_table_entry.cpp
  httpsql_scanner.cpp
  httpsql_arrow_reader.cpp
  httpsql_http_client.cpp
  httpsql_optimizer.cpp
  httpsql_transaction.cpp
  httpsql_transaction_manager.cpp
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:httpsql_library>
    PARENT_SCOPE)
