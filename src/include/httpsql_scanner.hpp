#pragma once
#include "duckdb.hpp"
#include "httpsql_config.hpp"

namespace duckdb {
class HttpSQLTableEntry;

struct AggOutputCol {
	string mysql_expr;
};

struct AggPushdown {
	vector<AggOutputCol> output_cols;
	vector<string> group_col_names;
};

struct HttpSQLBindData : public FunctionData {
	explicit HttpSQLBindData(HttpSQLTableEntry &table);

	HttpSQLTableEntry &table;
	vector<string> names;
	vector<LogicalType> types;

	string limit_clause;
	string order_clause;
	unique_ptr<AggPushdown> agg_pushdown;

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("HttpSQLBindData copy not supported");
	}
	bool Equals(const FunctionData &) const override { return false; }
};

TableFunction CreateHttpSQLScanFunction();

} // namespace duckdb
