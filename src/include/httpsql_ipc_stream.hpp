#pragma once
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include <string>

namespace duckdb {

// Creates an ArrowArrayStreamWrapper that streams Arrow IPC record batches from
// the given in-memory body (complete Arrow IPC stream: Schema + RecordBatches).
unique_ptr<ArrowArrayStreamWrapper> HttpSQLMakeIPCStream(std::string body);

} // namespace duckdb
