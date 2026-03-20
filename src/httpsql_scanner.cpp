#include "httpsql_scanner.hpp"
#include "httpsql_catalog.hpp"
#include "httpsql_table_entry.hpp"
#include "httpsql_http_client.hpp"
#include "httpsql_ipc_stream.hpp"

#include "duckdb/function/table/arrow.hpp"
#include "yyjson.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

// ─── HttpSQLBindData ─────────────────────────────────────────────────────────

// stream_factory_ptr points to `this`; the object must be heap-allocated and
// not moved after construction.
HttpSQLBindData::HttpSQLBindData(HttpSQLTableEntry &table_p)
    : ArrowScanFunctionData(HttpSQLProduce, reinterpret_cast<uintptr_t>(this)), table(table_p) {
}

// ─── Filter → SQL WHERE clause ───────────────────────────────────────────────

static string TransformConstant(const Value &val) {
	if (val.IsNull()) {
		return "NULL";
	}
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
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		throw NotImplementedException("Unsupported comparison type");
	}
}

static string TransformFilter(const string &col, const TableFilter &filter);

static string CreateExpression(const string &col, const vector<unique_ptr<TableFilter>> &filters,
                               const string &op) {
	vector<string> parts;
	for (auto &f : filters) {
		auto s = TransformFilter(col, *f);
		if (!s.empty()) {
			parts.push_back(s);
		}
	}
	if (parts.empty()) {
		return "";
	}
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
			if (!in_list.empty()) {
				in_list += ",";
			}
			in_list += TransformConstant(v);
		}
		return col + " IN (" + in_list + ")";
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &of = filter.Cast<OptionalFilter>();
		if (of.child_filter) {
			return TransformFilter(col, *of.child_filter);
		}
		return "";
	}
	default:
		return "";
	}
}

// Build WHERE clause directly from a LogicalGet's table_filters.
// The keys in table_filters.filters are physical column indices (as set by
// DuckDB's FilterPushdown pass).  names must be the full original column list
// (bind_data.all_names).  Called by the optimizer before the agg-pushdown
// column rewrite replaces column_ids and makes the physical indices invalid.
string HttpSQLBuildWhereFromTableFilters(const TableFilterSet &filters, const vector<string> &names) {
	if (filters.filters.empty()) {
		return "";
	}
	vector<string> parts;
	for (auto &kv : filters.filters) {
		idx_t phys_col = kv.first; // physical column index
		if (phys_col == COLUMN_IDENTIFIER_ROW_ID || phys_col >= names.size()) {
			continue;
		}
		string col_name = "`" + names[phys_col] + "`";
		auto s = TransformFilter(col_name, *kv.second);
		if (!s.empty()) {
			parts.push_back(s);
		}
	}
	return parts.empty() ? "" : StringUtil::Join(parts, " AND ");
}

// Build WHERE clause from ArrowStreamParameters::filters.
// filter_to_col maps filter column index → physical schema column index.
static string BuildWhereClause(const ArrowStreamParameters &parameters, const vector<string> &all_names) {
	if (!parameters.filters || parameters.filters->filters.empty()) {
		return "";
	}
	vector<string> parts;
	for (auto &kv : parameters.filters->filters) {
		auto filter_col_idx = kv.first;
		auto &filter = kv.second;
		// Map filter column index → physical column index.
		auto it = parameters.projected_columns.filter_to_col.find(filter_col_idx);
		if (it == parameters.projected_columns.filter_to_col.end()) {
			continue;
		}
		idx_t phys_col = it->second;
		if (phys_col == COLUMN_IDENTIFIER_ROW_ID || phys_col >= all_names.size()) {
			continue;
		}
		string col_name = "`" + all_names[phys_col] + "`";
		auto s = TransformFilter(col_name, *filter);
		if (!s.empty()) {
			parts.push_back(s);
		}
	}
	return parts.empty() ? "" : StringUtil::Join(parts, " AND ");
}

// ─── Produce (called by ArrowScanInitGlobal via ProduceArrowScan) ─────────────

unique_ptr<ArrowArrayStreamWrapper> HttpSQLProduce(uintptr_t factory_ptr, ArrowStreamParameters &parameters) {
	auto &bind = *reinterpret_cast<HttpSQLBindData *>(factory_ptr);
	auto &table = bind.table;
	auto &cat = table.catalog.Cast<HttpSQLCatalog>();

	bool agg_mode = bind.agg_pushdown != nullptr;

	// ── Build columns list ──────────────────────────────────────────────────
	vector<string> col_names;
	if (!agg_mode) {
		// Use column names from projection pushdown.
		for (auto &name : parameters.projected_columns.columns) {
			col_names.push_back(name);
		}
	}

	// ── Build WHERE clause ──────────────────────────────────────────────────
	// In agg mode the optimizer has already rewritten LogicalGet's columns to
	// virtual names (_a0, _g0 …), so parameters.filters / filter_to_col no
	// longer map to real columns.  Use the pre-computed clause saved before the
	// rewrite.  In non-agg mode use the live parameters as usual.
	string where_clause;
	if (agg_mode) {
		where_clause = bind.agg_where_clause;
	} else {
		where_clause = BuildWhereClause(parameters, bind.all_names);
	}

	// ── Assemble JSON request body ─────────────────────────────────────────
	yyjson_mut_doc *jdoc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *jroot = yyjson_mut_obj(jdoc);
	yyjson_mut_doc_set_root(jdoc, jroot);

	yyjson_mut_obj_add_strcpy(jdoc, jroot, "schema", table.schema.name.c_str());
	yyjson_mut_obj_add_strcpy(jdoc, jroot, "table", table.name.c_str());

	yyjson_mut_val *jcols = yyjson_mut_arr(jdoc);
	yyjson_mut_obj_add_val(jdoc, jroot, "columns", jcols);
	for (auto &c : col_names) {
		yyjson_mut_arr_add_strcpy(jdoc, jcols, c.c_str());
	}

	if (!where_clause.empty()) {
		yyjson_mut_obj_add_strcpy(jdoc, jroot, "where", where_clause.c_str());
	}
	if (!bind.limit_clause.empty()) {
		yyjson_mut_obj_add_strcpy(jdoc, jroot, "limit", bind.limit_clause.c_str());
	}
	if (!bind.order_clause.empty()) {
		yyjson_mut_obj_add_strcpy(jdoc, jroot, "order", bind.order_clause.c_str());
	}
	if (agg_mode) {
		auto &pd = *bind.agg_pushdown;
		string agg_select;
		for (idx_t i = 0; i < pd.output_cols.size(); i++) {
			if (i > 0) {
				agg_select += ", ";
			}
			agg_select += pd.output_cols[i].sql_expr;
		}
		yyjson_mut_obj_add_strcpy(jdoc, jroot, "agg_select", agg_select.c_str());
		if (!pd.group_col_names.empty()) {
			string group_by;
			for (idx_t i = 0; i < pd.group_col_names.size(); i++) {
				if (i > 0) {
					group_by += ", ";
				}
				group_by += pd.group_col_names[i];
			}
			yyjson_mut_obj_add_strcpy(jdoc, jroot, "agg_group_by", group_by.c_str());
		}
	}

	size_t json_len = 0;
	char *json_raw = yyjson_mut_write(jdoc, 0, &json_len);
	string body(json_raw, json_len);
	free(json_raw);
	yyjson_mut_doc_free(jdoc);

	auto resp = cat.http.Post("/api/query", body);
	if (!resp.ok()) {
		throw IOException("httpsql: POST /api/query failed (status %d): %s", resp.status_code,
		                  resp.body.empty() ? resp.error : resp.body);
	}

	return HttpSQLMakeIPCStream(std::move(resp.body));
}

// ─── Scan function registration ───────────────────────────────────────────────

static unique_ptr<FunctionData> HttpSQLBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &,
                                             vector<string> &) {
	throw InternalException("HttpSQLBind should not be called directly");
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
	TableFunction func("httpsql_scan_internal", {}, ArrowTableFunction::ArrowScanFunction, HttpSQLBind,
	                   ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	func.to_string = HttpSQLToString;
	func.serialize = HttpSQLSerialize;
	func.deserialize = HttpSQLDeserialize;
	func.get_bind_info = HttpSQLGetBindInfo;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	return func;
}

} // namespace duckdb
