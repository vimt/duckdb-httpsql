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

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("HttpSQLBindData copy not supported");
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};

TableFunction CreateHttpSQLScanFunction();

} // namespace duckdb
