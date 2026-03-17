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

struct PhysicalTableInfo {
	string host;
	string db_name;
	string table_name;
};

struct LogicTableInfo {
	string logic_db;
	string logic_table;
	vector<PhysicalTableInfo> physical_tables;
	vector<ColumnInfo> columns;
};

} // namespace duckdb
