#pragma once
#include "duckdb.hpp"
#include <string>
#include <vector>

namespace duckdb {

// Per-column metadata parsed from an Arrow IPC Schema message.
struct ArrowColInfo {
	LogicalType type;
	bool nullable = false;
	bool is_var_len = false; // Utf8 / Binary (variable-length buffers)
	bool is_bool = false;    // Bool (bit-packed value buffer)
};

// Reads an Arrow IPC streaming format response body.
// Call ReadSchema() once, then ReadRecordBatch() in a loop until it returns false.
class ArrowIPCReader {
public:
	explicit ArrowIPCReader(std::string body);

	// Parse the leading Schema message.  Returns false on parse failure.
	bool ReadSchema(std::vector<ArrowColInfo> &cols);

	// Parse the next RecordBatch message and fill output.
	// Returns false when the stream is exhausted or on error.
	bool ReadRecordBatch(const std::vector<ArrowColInfo> &cols, DataChunk &output);

private:
	std::string body_;
	size_t pos_ = 0;

	struct RawMsg {
		uint8_t header_type; // 1=Schema, 3=RecordBatch
		const uint8_t *meta;
		size_t meta_len;
		int64_t body_len;
		size_t body_start; // absolute offset in body_ where body data begins
	};

	bool nextMsg(RawMsg &out);
};

} // namespace duckdb
