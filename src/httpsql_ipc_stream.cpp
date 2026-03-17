#include "httpsql_ipc_stream.hpp"

// nanoarrow IPC C API (symbols renamed to DuckDBExtHttpsql_* via NANOARROW_NAMESPACE)
#include "nanoarrow/nanoarrow_ipc.h"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow.hpp"

#include <cerrno>
#include <cstring>

namespace duckdb {

// ── IPC state ──────────────────────────────────────────────────────────────

struct HttpSQLIPCState {
	std::string body;         // owns the raw Arrow IPC bytes
	const uint8_t *data;      // pointer into body.data()
	size_t body_len;
	size_t pos = 0;

	ArrowIpcDecoder decoder;
	ArrowSchema schema;       // cached Schema message result
	bool schema_read = false;
	bool finished = false;
	std::string last_error;
	size_t last_body_start = 0; // start of the body region of the last message

	explicit HttpSQLIPCState(std::string body_p)
	    : body(std::move(body_p)), data(reinterpret_cast<const uint8_t *>(this->body.data())),
	      body_len(this->body.size()) {
		ArrowIpcDecoderInit(&decoder);
		schema.release = nullptr;
	}

	~HttpSQLIPCState() {
		ArrowIpcDecoderReset(&decoder);
		if (schema.release) {
			schema.release(&schema);
		}
	}

	// Read the next IPC message envelope (prefix + metadata), advance pos.
	// After a successful call, decoder.message_type and decoder.body_size_bytes are set.
	// The body region is [last_body_start, last_body_start + decoder.body_size_bytes).
	// Returns false on EOS or error (sets last_error).
	bool NextMessage() {
		if (finished) {
			return false;
		}
		// Need at least 8 bytes for [continuation(4)][metadata_size(4)]
		if (pos + 8 > body_len) {
			finished = true;
			return false;
		}

		// Read the 4-byte metadata length (little-endian int32 at offset +4).
		int32_t meta_size;
		memcpy(&meta_size, data + pos + 4, 4);

		// meta_size == -1 means EOS marker.
		if (meta_size < 0) {
			finished = true;
			return false;
		}

		// Header view for nanoarrow: [4 continuation][4 meta_size][meta_size bytes metadata]
		size_t header_size = 8 + (size_t)meta_size;
		if (pos + header_size > body_len) {
			last_error = "Truncated Arrow IPC metadata";
			finished = true;
			return false;
		}

		ArrowBufferView header_view;
		header_view.data.data = data + pos;
		header_view.size_bytes = (int64_t)header_size;

		pos += header_size;

		// Align to 8-byte boundary (metadata may have padding before body).
		if (pos % 8 != 0) {
			pos += 8 - (pos % 8);
		}

		ArrowError nanoarrow_err;
		int rc = ArrowIpcDecoderDecodeHeader(&decoder, header_view, &nanoarrow_err);
		if (rc == ENODATA) {
			finished = true;
			return false;
		}
		if (rc != NANOARROW_OK) {
			last_error = nanoarrow_err.message;
			finished = true;
			return false;
		}

		// Record body boundaries.
		last_body_start = pos;
		size_t body_bytes = (size_t)decoder.body_size_bytes;
		if (pos + body_bytes > body_len) {
			last_error = "Truncated Arrow IPC body";
			finished = true;
			return false;
		}
		pos += body_bytes;
		return true;
	}

	ArrowBufferView BodyView() const {
		ArrowBufferView v;
		v.data.data = data + last_body_start;
		v.size_bytes = decoder.body_size_bytes;
		return v;
	}

	// ── ArrowArrayStream callbacks ──────────────────────────────────────────

	int GetSchema(ArrowSchema *out) {
		if (schema_read) {
			ArrowError err;
			int rc = ArrowSchemaDeepCopy(&schema, out);
			if (rc != NANOARROW_OK) {
				last_error = "ArrowSchemaDeepCopy failed";
			}
			return rc;
		}

		if (!NextMessage()) {
			last_error = last_error.empty() ? "Empty Arrow IPC stream" : last_error;
			return EIO;
		}
		if (decoder.message_type != NANOARROW_IPC_MESSAGE_TYPE_SCHEMA) {
			last_error = "Expected Arrow IPC Schema message";
			return EIO;
		}

		ArrowError err;
		int rc = ArrowIpcDecoderDecodeSchema(&decoder, &schema, &err);
		if (rc != NANOARROW_OK) {
			last_error = err.message;
			return rc;
		}
		rc = ArrowIpcDecoderSetSchema(&decoder, &schema, &err);
		if (rc != NANOARROW_OK) {
			last_error = err.message;
			return rc;
		}
		schema_read = true;
		return GetSchema(out); // recurse once to deep-copy
	}

	int GetNext(ArrowArray *out) {
		if (finished) {
			out->release = nullptr;
			return NANOARROW_OK;
		}

		// Ensure schema is loaded (needed by the decoder for array decoding).
		if (!schema_read) {
			ArrowSchema tmp;
			tmp.release = nullptr;
			int rc = GetSchema(&tmp);
			if (tmp.release) {
				tmp.release(&tmp);
			}
			if (rc != NANOARROW_OK) {
				return rc;
			}
		}

		// Skip non-RecordBatch messages (e.g. DictionaryBatch).
		while (true) {
			if (!NextMessage()) {
				out->release = nullptr;
				return NANOARROW_OK; // EOS
			}
			if (decoder.message_type == NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH) {
				break;
			}
		}

		ArrowError err;
		int rc = ArrowIpcDecoderDecodeArray(&decoder, BodyView(), -1, out,
		                                   NANOARROW_VALIDATION_LEVEL_FULL, &err);
		if (rc != NANOARROW_OK) {
			last_error = err.message;
		}
		return rc;
	}
};

// ── C callbacks ────────────────────────────────────────────────────────────

static int IPCGetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
	return static_cast<HttpSQLIPCState *>(stream->private_data)->GetSchema(out);
}

static int IPCGetNext(ArrowArrayStream *stream, ArrowArray *out) {
	return static_cast<HttpSQLIPCState *>(stream->private_data)->GetNext(out);
}

static const char *IPCGetLastError(ArrowArrayStream *stream) {
	return static_cast<HttpSQLIPCState *>(stream->private_data)->last_error.c_str();
}

static void IPCRelease(ArrowArrayStream *stream) {
	delete static_cast<HttpSQLIPCState *>(stream->private_data);
	stream->release = nullptr;
}

// ── Public API ─────────────────────────────────────────────────────────────

unique_ptr<ArrowArrayStreamWrapper> HttpSQLMakeIPCStream(std::string body) {
	auto result = make_uniq<ArrowArrayStreamWrapper>();
	auto &stream = result->arrow_array_stream;
	stream.get_schema = IPCGetSchema;
	stream.get_next = IPCGetNext;
	stream.get_last_error = IPCGetLastError;
	stream.release = IPCRelease;
	stream.private_data = new HttpSQLIPCState(std::move(body));
	result->number_of_rows = -1; // unknown row count
	return result;
}

} // namespace duckdb
