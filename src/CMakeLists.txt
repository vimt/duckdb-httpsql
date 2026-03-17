include_directories(include)

add_library(
  httpsql_library OBJECT
  httpsql_extension.cpp
  httpsql_storage.cpp
  httpsql_catalog.cpp
  httpsql_schema_entry.cpp
  httpsql_table_entry.cpp
  httpsql_scanner.cpp
  httpsql_ipc_stream.cpp
  httpsql_http_client.cpp
  httpsql_optimizer.cpp
  httpsql_transaction.cpp
  httpsql_transaction_manager.cpp
)

# Propagate nanoarrow include dirs to this OBJECT library.
# FetchContent_MakeAvailable(nanoarrow) is called in the parent CMakeLists.txt.
target_include_directories(httpsql_library PRIVATE
    $<TARGET_PROPERTY:nanoarrow_ipc,INTERFACE_INCLUDE_DIRECTORIES>)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:httpsql_library>
    PARENT_SCOPE)
