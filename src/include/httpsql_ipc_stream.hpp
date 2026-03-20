#pragma once
#include "httpsql_http_client.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include <memory>

namespace duckdb {

// Creates an ArrowArrayStreamWrapper that streams Arrow IPC record batches
// incrementally from a streaming HTTP response body.
// Takes ownership of the body stream; reads one batch at a time from the socket
// so the full response never needs to be buffered in memory.
unique_ptr<ArrowArrayStreamWrapper> HttpSQLMakeIPCStream(std::unique_ptr<HttpSQLBodyStream> stream);

} // namespace duckdb
