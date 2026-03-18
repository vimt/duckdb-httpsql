#pragma once
#include "duckdb.hpp"

namespace duckdb {

struct ColumnInfo {
	string name;
	string type_name;
	string column_type;
	bool is_nullable = false;
	int64_t precision = -1;
	int64_t scale = -1;
	// duckdb_type is the authoritative DuckDB type string supplied by the server
	// (e.g. "DECIMAL(10,4)", "JSON", "TIMESTAMP"). When non-empty it is used
	// directly for catalog construction; type_name/column_type are kept as
	// fallback for servers that predate this field.
	string duckdb_type;
};

} // namespace duckdb
