#include "httpsql_arrow_reader.hpp"

#include <cstring>

namespace duckdb {

// ─── Minimal FlatBuffer table accessor ───────────────────────────────────────
namespace {

class FBT {
	const uint8_t *buf_; // base of the flatbuffer
	size_t pos_;         // absolute position of this table in buf_

	template <typename T>
	T rd(size_t p) const {
		T v{};
		memcpy(&v, buf_ + p, sizeof(T));
		return v;
	}

	// Absolute position of field fi's content, or 0 if not present.
	size_t fpos(int fi) const {
		if (!buf_ || !pos_) {
			return 0;
		}
		// vtable is at:  pos_ - *(int32_t*)pos_
		int32_t vt_soff = rd<int32_t>(pos_);
		size_t vt = (size_t)((int64_t)pos_ - (int64_t)vt_soff);
		uint16_t vt_sz = rd<uint16_t>(vt);
		int slot = 4 + fi * 2;
		if (slot >= (int)vt_sz) {
			return 0;
		}
		uint16_t foff = rd<uint16_t>(vt + slot);
		return foff ? pos_ + foff : 0;
	}

public:
	FBT() : buf_(nullptr), pos_(0) {
	}
	FBT(const uint8_t *buf, size_t pos) : buf_(buf), pos_(pos) {
	}

	bool valid() const {
		return buf_ != nullptr && pos_ != 0;
	}

	// Read a scalar field (integer types stored directly in the table).
	template <typename T>
	T sc(int fi, T def = T {}) const {
		size_t p = fpos(fi);
		return p ? rd<T>(p) : def;
	}

	// Read a string field (forward-offset → uint32 len + bytes).
	std::string str(int fi) const {
		size_t ref = fpos(fi);
		if (!ref) {
			return {};
		}
		int32_t off = rd<int32_t>(ref);
		size_t sp = ref + (size_t)off;
		uint32_t len = rd<uint32_t>(sp);
		return {(const char *)(buf_ + sp + 4), len};
	}

	// Navigate to a nested table field.
	FBT table(int fi) const {
		size_t ref = fpos(fi);
		if (!ref) {
			return {};
		}
		int32_t off = rd<int32_t>(ref);
		return {buf_, ref + (size_t)off};
	}

	// Number of elements in a vector field.
	size_t vlen(int fi) const {
		size_t ref = fpos(fi);
		if (!ref) {
			return 0;
		}
		int32_t off = rd<int32_t>(ref);
		return rd<uint32_t>(ref + (size_t)off);
	}

	// i-th element of a vector of tables.
	FBT vtbl(int fi, size_t i) const {
		size_t ref = fpos(fi);
		if (!ref) {
			return {};
		}
		int32_t off = rd<int32_t>(ref);
		size_t vp = ref + (size_t)off;
		size_t ep = vp + 4 + i * 4;
		int32_t eoff = rd<int32_t>(ep);
		return {buf_, ep + (size_t)eoff};
	}

	// Absolute position of i-th element in a vector of fixed-size structs.
	size_t vstruct(int fi, size_t i, size_t esz) const {
		size_t ref = fpos(fi);
		if (!ref) {
			return 0;
		}
		int32_t off = rd<int32_t>(ref);
		size_t vp = ref + (size_t)off;
		return vp + 4 + i * esz;
	}

	// Read any type at an absolute position in buf_.
	template <typename T>
	T at(size_t abs_pos) const {
		T v{};
		memcpy(&v, buf_ + abs_pos, sizeof(T));
		return v;
	}
};

// Return the root table of a FlatBuffer (root offset at byte 0).
FBT fb_root(const uint8_t *meta) {
	uint32_t root_off;
	memcpy(&root_off, meta, 4);
	return FBT(meta, (size_t)root_off);
}

// Arrow type_type union tags (from format/Schema.fbs).
enum ArrowTypeTag : int8_t {
	AT_NULL = 1,
	AT_INT = 2,
	AT_FLOAT = 3,
	AT_BINARY = 4,
	AT_UTF8 = 5,
	AT_BOOL = 6,
	AT_DECIMAL = 7,
	AT_DATE = 8,
	AT_TIME = 9,
	AT_TIMESTAMP = 10,
	AT_LARGE_BINARY = 19,
	AT_LARGE_UTF8 = 20,
};

// Arrow MessageHeader enum.
enum ArrowMsgHeader : int8_t { MH_SCHEMA = 1, MH_DICT = 2, MH_RECORD_BATCH = 3 };

// Parse one Field table → ArrowColInfo.
ArrowColInfo ParseField(FBT field) {
	ArrowColInfo ci;
	ci.nullable = field.sc<int8_t>(1, 1) != 0;
	auto type_tag = (ArrowTypeTag)field.sc<int8_t>(2, 0);
	FBT tt = field.table(3); // type union

	switch (type_tag) {
	case AT_INT: {
		int32_t bits = tt.sc<int32_t>(0, 32);
		bool is_signed = tt.sc<int8_t>(1, 1) != 0;
		if (is_signed) {
			ci.type = bits == 8    ? LogicalType::TINYINT
			          : bits == 16 ? LogicalType::SMALLINT
			          : bits == 32 ? LogicalType::INTEGER
			                       : LogicalType::BIGINT;
		} else {
			ci.type = bits == 8    ? LogicalType::UTINYINT
			          : bits == 16 ? LogicalType::USMALLINT
			          : bits == 32 ? LogicalType::UINTEGER
			                       : LogicalType::UBIGINT;
		}
		break;
	}
	case AT_FLOAT: {
		int16_t prec = tt.sc<int16_t>(0, 2);
		ci.type = (prec == 1) ? LogicalType::FLOAT : LogicalType::DOUBLE;
		break;
	}
	case AT_BINARY:
	case AT_LARGE_BINARY:
		ci.type = LogicalType::BLOB;
		ci.is_var_len = true;
		break;
	case AT_UTF8:
	case AT_LARGE_UTF8:
		ci.type = LogicalType::VARCHAR;
		ci.is_var_len = true;
		break;
	case AT_BOOL:
		ci.type = LogicalType::BOOLEAN;
		ci.is_bool = true;
		break;
	case AT_DATE:
		ci.type = LogicalType::DATE;
		break;
	case AT_TIME:
		ci.type = LogicalType::TIME;
		break;
	case AT_TIMESTAMP:
		ci.type = LogicalType::TIMESTAMP;
		break;
	default:
		ci.type = LogicalType::VARCHAR;
		ci.is_var_len = true;
		break;
	}
	return ci;
}

} // anonymous namespace

// ─── ArrowIPCReader implementation ───────────────────────────────────────────

ArrowIPCReader::ArrowIPCReader(std::string body) : body_(std::move(body)) {
}

bool ArrowIPCReader::nextMsg(RawMsg &out) {
	const auto *data = (const uint8_t *)body_.data();
	size_t sz = body_.size();

	if (pos_ + 8 > sz) {
		return false;
	}

	// Continuation marker (must be 0xFFFFFFFF).
	pos_ += 4;

	// Metadata size; 0 signals EOS.
	int32_t meta_size;
	memcpy(&meta_size, data + pos_, 4);
	pos_ += 4;

	if (meta_size <= 0) {
		return false;
	}
	if (pos_ + (size_t)meta_size > sz) {
		return false;
	}

	out.meta = data + pos_;
	out.meta_len = (size_t)meta_size;
	pos_ += (size_t)meta_size;

	// Align to 8-byte boundary.
	pos_ += (8 - (pos_ % 8)) % 8;

	// Parse Message FlatBuffer for header_type and bodyLength.
	FBT msg = fb_root(out.meta);
	out.header_type = (uint8_t)msg.sc<int8_t>(1, 0);
	out.body_len = msg.sc<int64_t>(3, 0);
	out.body_start = pos_;

	pos_ += (size_t)out.body_len;
	return true;
}

bool ArrowIPCReader::ReadSchema(std::vector<ArrowColInfo> &cols) {
	RawMsg msg;
	if (!nextMsg(msg) || msg.header_type != (uint8_t)MH_SCHEMA) {
		return false;
	}

	FBT m = fb_root(msg.meta);
	FBT schema = m.table(2); // Message.header union → Schema table

	size_t nfields = schema.vlen(1); // Schema.fields vector
	cols.reserve(nfields);
	for (size_t i = 0; i < nfields; i++) {
		cols.push_back(ParseField(schema.vtbl(1, i)));
	}
	return true;
}

bool ArrowIPCReader::ReadRecordBatch(const std::vector<ArrowColInfo> &cols, DataChunk &output) {
	RawMsg msg;
	// Skip any non-RecordBatch messages (e.g. DictionaryBatch).
	do {
		if (!nextMsg(msg)) {
			return false;
		}
	} while (msg.header_type != (uint8_t)MH_RECORD_BATCH);

	FBT m = fb_root(msg.meta);
	FBT rb = m.table(2); // Message.header union → RecordBatch table

	int64_t nrows = rb.sc<int64_t>(0, 0); // RecordBatch.length
	if (nrows == 0) {
		output.SetCardinality(0);
		return true;
	}

	const auto *body = (const uint8_t *)body_.data() + msg.body_start;
	const idx_t ncols = (idx_t)cols.size();
	idx_t buf_idx = 0;

	for (idx_t col = 0; col < ncols; col++) {
		const auto &ci = cols[col];

		// FieldNode struct (16 bytes): int64 length + int64 null_count.
		size_t node_pos = rb.vstruct(1, (size_t)col, 16);
		int64_t null_count = rb.at<int64_t>(node_pos + 8);

		// Validity bitmap buffer.
		size_t vbuf_pos = rb.vstruct(2, buf_idx++, 16);
		int64_t vbuf_off = rb.at<int64_t>(vbuf_pos);
		int64_t vbuf_len = rb.at<int64_t>(vbuf_pos + 8);
		const uint8_t *validity_bits =
		    (vbuf_len > 0 && null_count > 0) ? body + vbuf_off : nullptr;

		// Data buffer (values for fixed-width, or offsets for var-len).
		size_t dbuf_pos = rb.vstruct(2, buf_idx++, 16);
		int64_t dbuf_off = rb.at<int64_t>(dbuf_pos);
		const uint8_t *val_buf = body + dbuf_off;

		// Third buffer for var-len data section.
		int64_t data_off = 0;
		if (ci.is_var_len) {
			size_t d2_pos = rb.vstruct(2, buf_idx++, 16);
			data_off = rb.at<int64_t>(d2_pos);
		}

		// Apply null mask.
		auto &validity = FlatVector::Validity(output.data[col]);
		if (validity_bits) {
			validity.Initialize((idx_t)nrows);
			for (idx_t i = 0; i < (idx_t)nrows; i++) {
				if (!((validity_bits[i / 8] >> (i % 8)) & 1)) {
					validity.SetInvalid(i);
				}
			}
		}

		// Copy values.
		if (ci.is_var_len) {
			const auto *offsets = (const int32_t *)val_buf;
			const char *data = (const char *)(body + data_off);
			auto *sv = FlatVector::GetData<string_t>(output.data[col]);
			for (idx_t i = 0; i < (idx_t)nrows; i++) {
				if (validity_bits && !validity.RowIsValid(i)) {
					sv[i] = string_t {};
				} else {
					int32_t s = offsets[i], e = offsets[i + 1];
					sv[i] = StringVector::AddStringOrBlob(output.data[col], data + s, (uint32_t)(e - s));
				}
			}
		} else if (ci.is_bool) {
			auto *bv = FlatVector::GetData<uint8_t>(output.data[col]);
			for (idx_t i = 0; i < (idx_t)nrows; i++) {
				bv[i] = (val_buf[i / 8] >> (i % 8)) & 1;
			}
		} else {
			switch (ci.type.id()) {
			case LogicalTypeId::TINYINT:
			case LogicalTypeId::UTINYINT:
				memcpy(FlatVector::GetData<int8_t>(output.data[col]), val_buf, (size_t)nrows);
				break;
			case LogicalTypeId::SMALLINT:
			case LogicalTypeId::USMALLINT:
				memcpy(FlatVector::GetData<int16_t>(output.data[col]), val_buf, (size_t)nrows * 2);
				break;
			case LogicalTypeId::INTEGER:
			case LogicalTypeId::UINTEGER:
				memcpy(FlatVector::GetData<int32_t>(output.data[col]), val_buf, (size_t)nrows * 4);
				break;
			case LogicalTypeId::BIGINT:
			case LogicalTypeId::UBIGINT:
				memcpy(FlatVector::GetData<int64_t>(output.data[col]), val_buf, (size_t)nrows * 8);
				break;
			case LogicalTypeId::FLOAT:
				memcpy(FlatVector::GetData<float>(output.data[col]), val_buf, (size_t)nrows * 4);
				break;
			case LogicalTypeId::DOUBLE:
				memcpy(FlatVector::GetData<double>(output.data[col]), val_buf, (size_t)nrows * 8);
				break;
			case LogicalTypeId::DATE: {
				// Arrow Date32 (days since 1970-01-01) → DuckDB date_t (days since 2000-01-01).
				const auto *src = (const int32_t *)val_buf;
				auto *dst = FlatVector::GetData<date_t>(output.data[col]);
				for (idx_t i = 0; i < (idx_t)nrows; i++) {
					dst[i].days = src[i] - 10957;
				}
				break;
			}
			case LogicalTypeId::TIMESTAMP: {
				// Arrow Timestamp(us, UTC) since 1970 → DuckDB timestamp_t (us) since 2000.
				const auto *src = (const int64_t *)val_buf;
				auto *dst = FlatVector::GetData<timestamp_t>(output.data[col]);
				static constexpr int64_t kEpochOffset = 10957LL * 86400LL * 1000000LL;
				for (idx_t i = 0; i < (idx_t)nrows; i++) {
					dst[i].value = src[i] - kEpochOffset;
				}
				break;
			}
			case LogicalTypeId::TIME:
				// Arrow Time64(us) since midnight == DuckDB dtime_t — direct copy.
				memcpy(FlatVector::GetData<dtime_t>(output.data[col]), val_buf, (size_t)nrows * 8);
				break;
			default:
				break;
			}
		}
	}

	output.SetCardinality((idx_t)nrows);
	return true;
}

} // namespace duckdb
