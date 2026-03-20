#pragma once
#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "httpsql_config.hpp"

namespace duckdb {

class HttpSQLTableEntry;

struct AggOutputCol {
	string sql_expr;
};

struct AggPushdown {
	vector<AggOutputCol> output_cols;
	vector<string> group_col_names;
};

// stream_factory_produce_t callback – defined in httpsql_scanner.cpp
unique_ptr<ArrowArrayStreamWrapper> HttpSQLProduce(uintptr_t factory_ptr, ArrowStreamParameters &parameters);

// Build a SQL WHERE clause from a LogicalGet's table_filters before they are
// invalidated by the agg-pushdown column rewrite.
// The keys in table_filters.filters are physical column indices, so names must
// be the original full column list (bind_data.all_names).
string HttpSQLBuildWhereFromTableFilters(const TableFilterSet &filters, const vector<string> &names);

struct HttpSQLBindData : public ArrowScanFunctionData {
	explicit HttpSQLBindData(HttpSQLTableEntry &table);

	// Keep a reference to the table entry (lifetime managed by catalog).
	HttpSQLTableEntry &table;

	// All column names in physical schema order; used to map filter col IDs → names.
	vector<string> all_names;

	// ── Pushdown state (written by optimizer, read by HttpSQLProduce) ──────
	string limit_clause;
	string order_clause;
	unique_ptr<AggPushdown> agg_pushdown;
	// WHERE clause pre-computed by the optimizer before agg column rewrite.
	// Used in agg mode so HttpSQLProduce doesn't try to build it from the
	// (now-invalid) virtual column ArrowStreamParameters.
	string agg_where_clause;

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("HttpSQLBindData copy not supported");
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};

TableFunction CreateHttpSQLScanFunction();

} // namespace duckdb
