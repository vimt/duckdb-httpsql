#include "httpsql_scanner.hpp"
#include "httpsql_catalog.hpp"
#include "httpsql_table_entry.hpp"
#include "httpsql_http_client.hpp"
#include "httpsql_arrow_reader.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"

namespace duckdb {

HttpSQLBindData::HttpSQLBindData(HttpSQLTableEntry &table) : table(table) {}

// ─── Filter → SQL WHERE clause ─────────────────────────────────────────────

static string TransformConstant(const Value &val) {
	if (val.IsNull()) return "NULL";
	switch (val.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return val.GetValue<bool>() ? "1" : "0";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return val.ToString();
	default:
		return "'" + StringUtil::Replace(val.ToString(), "'", "\\'") + "'";
	}
}

static string TransformComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL: return "=";
	case ExpressionType::COMPARE_NOTEQUAL: return "!=";
	case ExpressionType::COMPARE_LESSTHAN: return "<";
	case ExpressionType::COMPARE_GREATERTHAN: return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: return ">=";
	default: throw NotImplementedException("Unsupported comparison type");
	}
}

static string TransformFilter(const string &col, const TableFilter &filter);

static string CreateExpression(const string &col, const vector<unique_ptr<TableFilter>> &filters,
                               const string &op) {
	vector<string> parts;
	for (auto &f : filters) {
		auto s = TransformFilter(col, *f);
		if (!s.empty()) parts.push_back(s);
	}
	if (parts.empty()) return "";
	return "(" + StringUtil::Join(parts, " " + op + " ") + ")";
}

static string TransformFilter(const string &col, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::IS_NULL:
		return col + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return col + " IS NOT NULL";
	case TableFilterType::CONJUNCTION_AND: {
		auto &cf = filter.Cast<ConjunctionAndFilter>();
		return CreateExpression(col, cf.child_filters, "AND");
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &cf = filter.Cast<ConjunctionOrFilter>();
		return CreateExpression(col, cf.child_filters, "OR");
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &cf = filter.Cast<ConstantFilter>();
		return StringUtil::Format("%s %s %s", col, TransformComparison(cf.comparison_type),
		                          TransformConstant(cf.constant));
	}
	case TableFilterType::IN_FILTER: {
		auto &inf = filter.Cast<InFilter>();
		string in_list;
		for (auto &v : inf.values) {
			if (!in_list.empty()) in_list += ",";
			in_list += TransformConstant(v);
		}
		return col + " IN (" + in_list + ")";
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &of = filter.Cast<OptionalFilter>();
		if (of.child_filter) return TransformFilter(col, *of.child_filter);
		return "";
	}
	default:
		return "";
	}
}

static string BuildWhereClause(const vector<column_t> &column_ids,
                                optional_ptr<TableFilterSet> filters,
                                const vector<string> &names) {
	if (!filters || filters->filters.empty()) return "";
	vector<string> parts;
	for (auto &[col_idx, filter] : filters->filters) {
		if (col_idx >= column_ids.size()) continue;
		auto phys_col = column_ids[col_idx];
		if (phys_col == COLUMN_IDENTIFIER_ROW_ID || phys_col >= names.size()) continue;
		string col_name = "`" + names[phys_col] + "`";
		auto s = TransformFilter(col_name, *filter);
		if (!s.empty()) parts.push_back(s);
	}
	return parts.empty() ? "" : StringUtil::Join(parts, " AND ");
}

// ─── Global state: holds the Arrow IPC response body and parse position ───────

struct HttpSQLGlobalState : public GlobalTableFunctionState {
	string body;                      // complete Arrow IPC response body
	vector<ArrowColInfo> col_info;    // parsed from Schema message
	bool schema_parsed = false;
	bool done = false;

	// Reader is constructed lazily on first scan call (needs schema parsed first).
	unique_ptr<ArrowIPCReader> reader;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct HttpSQLLocalState : public LocalTableFunctionState {};

// ─── Build and send the HTTP query, populate global state ──────────────────

static unique_ptr<FunctionData> HttpSQLBind(ClientContext &, TableFunctionBindInput &,
                                               vector<LogicalType> &, vector<string> &) {
	throw InternalException("HttpSQLBind should not be called directly");
}

static unique_ptr<GlobalTableFunctionState> HttpSQLInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<HttpSQLBindData>();
	auto &table = bind_data.table;
	auto &cat = table.catalog.Cast<HttpSQLCatalog>();

	auto gstate = make_uniq<HttpSQLGlobalState>();

	bool agg_mode = bind_data.agg_pushdown != nullptr;

	// Build column list (projection)
	vector<string> col_names;
	if (!agg_mode) {
		for (auto col_id : input.column_ids) {
			if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
				// skip, we'll use NULL below
			} else {
				col_names.push_back(table.GetColumn(LogicalIndex(col_id)).GetName());
			}
		}
	}

	// Build WHERE clause from filters
	string where_clause = BuildWhereClause(input.column_ids, input.filters, bind_data.names);

	// Build JSON request
	string body = "{";
	body += "\"schema\":\"" + table.schema.name + "\",";
	body += "\"table\":\"" + table.name + "\",";

	// columns array
	body += "\"columns\":[";
	for (idx_t i = 0; i < col_names.size(); i++) {
		if (i > 0) body += ",";
		body += "\"" + col_names[i] + "\"";
	}
	body += "]";

	if (!where_clause.empty()) {
		body += ",\"where\":\"" + StringUtil::Replace(where_clause, "\"", "\\\"") + "\"";
	}
	if (!bind_data.limit_clause.empty()) {
		body += ",\"limit\":\"" + bind_data.limit_clause + "\"";
	}
	if (!bind_data.order_clause.empty()) {
		body += ",\"order\":\"" + bind_data.order_clause + "\"";
	}
	if (agg_mode) {
		auto &pd = *bind_data.agg_pushdown;
		string agg_select;
		for (idx_t i = 0; i < pd.output_cols.size(); i++) {
			if (i > 0) agg_select += ", ";
			agg_select += pd.output_cols[i].sql_expr;
		}
		body += ",\"agg_select\":\"" + StringUtil::Replace(agg_select, "\"", "\\\"") + "\"";
		if (!pd.group_col_names.empty()) {
			string group_by;
			for (idx_t i = 0; i < pd.group_col_names.size(); i++) {
				if (i > 0) group_by += ", ";
				group_by += pd.group_col_names[i];
			}
			body += ",\"agg_group_by\":\"" + group_by + "\"";
		}
	}
	body += "}";

	Printer::Print(StringUtil::Format("[httpsql] POST /api/query: %s",
	                                  body.substr(0, 200)));

	auto resp = cat.http.Post("/api/query", body);
	if (!resp.ok()) {
		throw IOException("httpsql: POST /api/query failed (status %d): %s",
		                  resp.status_code, resp.body.empty() ? resp.error : resp.body);
	}

	// Store the Arrow IPC body and parse the Schema message immediately.
	gstate->body = std::move(resp.body);
	gstate->reader = make_uniq<ArrowIPCReader>(gstate->body);
	if (!gstate->reader->ReadSchema(gstate->col_info)) {
		throw IOException("httpsql: failed to parse Arrow IPC Schema message");
	}
	gstate->schema_parsed = true;
	return std::move(gstate);
}

static unique_ptr<LocalTableFunctionState> HttpSQLInitLocalState(ExecutionContext &, TableFunctionInitInput &,
                                                                    GlobalTableFunctionState *) {
	return make_uniq<HttpSQLLocalState>();
}

// ─── Scan: parse next batch of NDJSON rows into DataChunk ─────────────────

// ─── Scan: read one Arrow IPC RecordBatch per call ────────────────────────────

static void HttpSQLScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<HttpSQLGlobalState>();

	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}

	if (!gstate.reader->ReadRecordBatch(gstate.col_info, output)) {
		gstate.done = true;
		output.SetCardinality(0);
		return;
	}
}

static InsertionOrderPreservingMap<string> HttpSQLToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<HttpSQLBindData>();
	result["Schema"] = bind_data.table.schema.name;
	result["Table"] = bind_data.table.name;
	return result;
}

static void HttpSQLSerialize(Serializer &, const optional_ptr<FunctionData>, const TableFunction &) {
	throw NotImplementedException("HttpSQLSerialize");
}

static unique_ptr<FunctionData> HttpSQLDeserialize(Deserializer &, TableFunction &) {
	throw NotImplementedException("HttpSQLDeserialize");
}

static BindInfo HttpSQLGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<HttpSQLBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.table;
	return info;
}

TableFunction CreateHttpSQLScanFunction() {
	TableFunction func("httpsql_scan_internal", {}, HttpSQLScan, HttpSQLBind,
	                   HttpSQLInitGlobalState, HttpSQLInitLocalState);
	func.to_string = HttpSQLToString;
	func.serialize = HttpSQLSerialize;
	func.deserialize = HttpSQLDeserialize;
	func.get_bind_info = HttpSQLGetBindInfo;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	return func;
}

} // namespace duckdb
