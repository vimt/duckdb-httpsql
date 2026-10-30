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

struct HttpSQLBindData;

// ArrowScanFactory implementation: bridges DuckDB v2.0's Arrow scan framework
// (which calls factory->ProduceStream during execution) to httpsql's HTTP-based
// stream producer.  Holds a raw back-pointer to the bind_data that owns the
// factory (via ArrowScanFunctionData::factory); the bind_data outlives the factory.
struct HttpSQLArrowScanFactory : public ArrowScanFactory {
	HttpSQLBindData *bind_data = nullptr;

	void GetSchema(ArrowSchema &schema) override;
	unique_ptr<ArrowArrayStreamWrapper> ProduceStream(ArrowStreamParameters &parameters) override;
};

struct HttpSQLBindData : public ArrowScanFunctionData {
	explicit HttpSQLBindData(HttpSQLTableEntry &table_p);

	// Keep a reference to the table entry (lifetime managed by catalog).
	HttpSQLTableEntry &table;

	// All column names in physical schema order; used to map filter col IDs → names.
	vector<string> all_names;

	// ── Pushdown state (written by optimizer, read by ProduceStream) ──────
	string limit_clause;
	string order_clause;
	unique_ptr<AggPushdown> agg_pushdown;
	// WHERE clause pre-computed by the optimizer before agg column rewrite.
	string agg_where_clause;
	// WHERE captured from parent LogicalFilter
	string pre_filter_where;
	// HAVING clause captured from parent LogicalFilter after agg rewrite.
	string having_clause;

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("HttpSQLBindData copy not supported");
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};

// Build a SQL WHERE clause from a LogicalGet's table_filters before they are
// invalidated by the agg-pushdown column rewrite.
string HttpSQLBuildWhereFromTableFilters(const TableFilterSet &filters, const vector<ColumnIndex> &column_ids,
                                          const vector<string> &names);

TableFunction CreateHttpSQLScanFunction();

} // namespace duckdb
