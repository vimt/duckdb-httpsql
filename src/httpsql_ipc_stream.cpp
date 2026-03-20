#include "httpsql_ipc_stream.hpp"

// nanoarrow IPC C API (symbols renamed to DuckDBExtHttpsql_* via NANOARROW_NAMESPACE)
#include "nanoarrow/nanoarrow_ipc.h"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow.hpp"

#include <cerrno>
#include <cstring>
#include <vector>

namespace duckdb {

// ── IPC state ──────────────────────────────────────────────────────────────
//
// Reads Arrow IPC messages one at a time from an HttpSQLBodyStream.
// Memory usage is bounded by a single RecordBatch at a time rather than the
// entire response body.

struct HttpSQLIPCState {
	std::unique_ptr<HttpSQLBodyStream> stream;
	// Logical byte offset within the Arrow IPC stream (used for 8-byte alignment).
	int64_t bytes_consumed = 0;

	ArrowIpcDecoder decoder;
	ArrowSchema schema; // cached Schema message result
	bool schema_read = false;
	bool finished = false;
	std::string last_error;

	// Reusable per-message buffers — avoid repeated allocation.
	std::vector<uint8_t> header_buf;
	std::vector<uint8_t> body_buf;
	size_t body_size = 0; // valid bytes in body_buf after the last NextMessage()

	explicit HttpSQLIPCState(std::unique_ptr<HttpSQLBodyStream> s) : stream(std::move(s)) {
		ArrowIpcDecoderInit(&decoder);
		schema.release = nullptr;
	}

	~HttpSQLIPCState() {
		ArrowIpcDecoderReset(&decoder);
		if (schema.release) {
			schema.release(&schema);
		}
	}

	// ── Low-level read helpers ────────────────────────────────────────────

	bool ReadExact(void *dst, size_t n) {
		if (!stream->ReadExact(dst, n)) {
			if (!stream->LastError().empty()) {
				last_error = stream->LastError();
			}
			return false;
		}
		bytes_consumed += (int64_t)n;
		return true;
	}

	// Skip padding bytes so that bytes_consumed is a multiple of 8.
	// Arrow IPC requires 8-byte alignment before each message body.
	void SkipAlignPadding() {
		int rem = (int)(bytes_consumed % 8);
		if (rem == 0) {
			return;
		}
		uint8_t pad[7];
		size_t skip = (size_t)(8 - rem);
		// Best-effort: if this fails the stream is already broken.
		stream->ReadExact(pad, skip);
		bytes_consumed += (int64_t)skip;
	}

	// ── Arrow IPC message reader ──────────────────────────────────────────
	//
	// Arrow IPC stream layout per message:
	//   [continuation(4)] [metadata_size(4)] [metadata(metadata_size)]
	//   <padding to 8-byte boundary>
	//   [body(body_size_bytes)]   ← body_size_bytes is already 8-byte padded
	//
	// After NextMessage() returns true, body_buf holds the body bytes and
	// decoder.message_type identifies the message kind.

	bool NextMessage() {
		if (finished) {
			return false;
		}

		// Read 8-byte prefix: [continuation token (4)] [metadata_size (4)]
		uint8_t prefix[8];
		if (!ReadExact(prefix, 8)) {
			finished = true;
			return false;
		}
		// bytes_consumed already incremented inside ReadExact.

		int32_t meta_size;
		memcpy(&meta_size, prefix + 4, 4);

		// meta_size == -1 is the Arrow IPC EOS marker.
		if (meta_size < 0) {
			finished = true;
			return false;
		}

		// Read header: prefix bytes we already have + metadata bytes.
		size_t header_total = 8 + (size_t)meta_size;
		header_buf.resize(header_total);
		memcpy(header_buf.data(), prefix, 8);
		if (!ReadExact(header_buf.data() + 8, (size_t)meta_size)) {
			last_error = "truncated Arrow IPC header";
			finished = true;
			return false;
		}

		// Align to 8-byte boundary before the body.
		SkipAlignPadding();

		// Decode the header to learn the body size and message type.
		ArrowBufferView hv;
		hv.data.data = header_buf.data();
		hv.size_bytes = (int64_t)header_total;

		ArrowError err;
		int rc = ArrowIpcDecoderDecodeHeader(&decoder, hv, &err);
		if (rc == ENODATA) {
			finished = true;
			return false;
		}
		if (rc != NANOARROW_OK) {
			last_error = err.message;
			finished = true;
			return false;
		}

		// Read the message body (already 8-byte padded per Arrow spec).
		body_size = (size_t)decoder.body_size_bytes;
		if (body_size > 0) {
			body_buf.resize(body_size);
			if (!ReadExact(body_buf.data(), body_size)) {
				last_error = "truncated Arrow IPC body";
				finished = true;
				return false;
			}
		}

		return true;
	}

	ArrowBufferView BodyView() const {
		ArrowBufferView v;
		v.data.data = body_buf.empty() ? nullptr : body_buf.data();
		v.size_bytes = (int64_t)body_size;
		return v;
	}

	// ── ArrowArrayStream callbacks ────────────────────────────────────────

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
		return GetSchema(out); // recurse once to deep-copy into caller
	}

	int GetNext(ArrowArray *out) {
		if (finished) {
			out->release = nullptr;
			return NANOARROW_OK;
		}

		// Ensure schema is loaded (decoder needs it for array decoding).
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
				// If last_error is set, the stream was truncated unexpectedly;
				// propagate as an error so DuckDB surfaces it to the user.
				// If last_error is empty, this is a normal Arrow IPC EOS marker.
				return last_error.empty() ? NANOARROW_OK : EIO;
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

unique_ptr<ArrowArrayStreamWrapper> HttpSQLMakeIPCStream(std::unique_ptr<HttpSQLBodyStream> stream) {
	auto result = make_uniq<ArrowArrayStreamWrapper>();
	auto &s = result->arrow_array_stream;
	s.get_schema = IPCGetSchema;
	s.get_next = IPCGetNext;
	s.get_last_error = IPCGetLastError;
	s.release = IPCRelease;
	s.private_data = new HttpSQLIPCState(std::move(stream));
	result->number_of_rows = -1; // unknown row count
	return result;
}

} // namespace duckdb
