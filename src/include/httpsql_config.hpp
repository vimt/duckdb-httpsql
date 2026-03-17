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
};

} // namespace duckdb
