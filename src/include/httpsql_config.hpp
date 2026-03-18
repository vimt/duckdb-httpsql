#pragma once
#include "duckdb.hpp"

namespace duckdb {

struct ColumnInfo {
	string name;
	bool is_nullable = false;
	string duckdb_type; // DuckDB type string supplied by the server (e.g. "DECIMAL(10,4)", "JSON")
};

} // namespace duckdb
