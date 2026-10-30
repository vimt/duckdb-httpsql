#include "httpsql_scanner.hpp"
#include "httpsql_catalog.hpp"
#include "httpsql_table_entry.hpp"
#include "httpsql_http_client.hpp"
#include "httpsql_ipc_stream.hpp"

#include "duckdb/function/table/arrow.hpp"
#include "yyjson.hpp"
#include "nanoarrow/nanoarrow.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

// ─── HttpSQLArrowScanFactory / HttpSQLBindData ───────────────────────────────

// Forward declaration: produce logic is defined after filter helpers below.
static unique_ptr<ArrowArrayStreamWrapper> DoProduceStream(HttpSQLBindData &bind,
                                                           ArrowStreamParameters &parameters);

void HttpSQLArrowScanFactory::GetSchema(ArrowSchema &schema) {
	ArrowSchema &src = bind_data->schema_root.arrow_schema;
	ArrowError err;
	int rc = ArrowSchemaDeepCopy(&src, &schema);
	if (rc != NANOARROW_OK) {
		throw IOException("httpsql: ArrowSchemaDeepCopy failed");
	}
}

unique_ptr<ArrowArrayStreamWrapper> HttpSQLArrowScanFactory::ProduceStream(ArrowStreamParameters &parameters) {
	return DoProduceStream(*bind_data, parameters);
}

HttpSQLBindData::HttpSQLBindData(HttpSQLTableEntry &table_p)
    : ArrowScanFunctionData(make_shared_ptr<HttpSQLArrowScanFactory>()), table(table_p) {
	auto *f = static_cast<HttpSQLArrowScanFactory *>(factory.get());
	f->bind_data = this;
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

// Convert an ExpressionFilter's expression tree to a SQL fragment.
// The column is referenced inside the tree as BoundReferenceExpression(0);
// we substitute the actual column name passed in `col`.
// ValueToSQL and ComparisonOpToSQL are defined later in this file;
// forward-declared here via the lambda wrapper.
static string ExprFilterToSQL(const Expression &expr, const string &col) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_REF:
		return col;
	case ExpressionClass::BOUND_CONSTANT: {
		auto &v = expr.Cast<BoundConstantExpression>().GetValue();
		if (v.IsNull()) return "NULL";
		switch (v.type().id()) {
		case LogicalTypeId::BOOLEAN: return v.GetValue<bool>() ? "1" : "0";
		case LogicalTypeId::TINYINT: case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER: case LogicalTypeId::BIGINT:
		case LogicalTypeId::UTINYINT: case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER: case LogicalTypeId::UBIGINT:
		case LogicalTypeId::FLOAT: case LogicalTypeId::DOUBLE:
			return v.ToString();
		default:
			return "'" + StringUtil::Replace(v.ToString(), "'", "\\'") + "'";
		}
	}
	case ExpressionClass::BOUND_FUNCTION: {
		if (BoundComparisonExpression::IsComparison(expr)) {
			auto &cmp = expr.Cast<BoundFunctionExpression>();
			auto et = expr.GetExpressionType();
			string op;
			switch (et) {
			case ExpressionType::COMPARE_EQUAL:                op = "="; break;
			case ExpressionType::COMPARE_NOTEQUAL:             op = "!="; break;
			case ExpressionType::COMPARE_LESSTHAN:             op = "<"; break;
			case ExpressionType::COMPARE_GREATERTHAN:          op = ">"; break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:    op = "<="; break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO: op = ">="; break;
			default: return "";
			}
			string left  = ExprFilterToSQL(BoundComparisonExpression::Left(cmp), col);
			string right = ExprFilterToSQL(BoundComparisonExpression::Right(cmp), col);
			if (left.empty() || right.empty()) return "";
			return left + " " + op + " " + right;
		}
		if (BoundCastExpression::IsCast(expr)) {
			return ExprFilterToSQL(BoundCastExpression::Child(expr.Cast<BoundFunctionExpression>()), col);
		}
		return "";
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		string op = (conj.GetExpressionType() == ExpressionType::CONJUNCTION_AND) ? " AND " : " OR ";
		vector<string> parts;
		for (auto &child : conj.GetChildren()) {
			string s = ExprFilterToSQL(*child, col);
			if (s.empty()) return "";
			parts.push_back(s);
		}
		if (parts.empty()) return "";
		return "(" + StringUtil::Join(parts, op) + ")";
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		if (op_expr.GetChildren().size() == 1) {
			string c = ExprFilterToSQL(*op_expr.GetChildren()[0], col);
			if (c.empty()) return "";
			if (op_expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) return c + " IS NULL";
			if (op_expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) return c + " IS NOT NULL";
		}
		return "";
	}
	default:
		return "";
	}
}

static string TransformFilter(const string &col, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::LEGACY_IS_NULL:
		return col + " IS NULL";
	case TableFilterType::LEGACY_IS_NOT_NULL:
		return col + " IS NOT NULL";
	case TableFilterType::LEGACY_CONJUNCTION_AND: {
		auto &cf = filter.Cast<LegacyConjunctionAndFilter>();
		return CreateExpression(col, cf.child_filters, "AND");
	}
	case TableFilterType::LEGACY_CONJUNCTION_OR: {
		auto &cf = filter.Cast<LegacyConjunctionOrFilter>();
		return CreateExpression(col, cf.child_filters, "OR");
	}
	case TableFilterType::LEGACY_CONSTANT_COMPARISON: {
		auto &cf = filter.Cast<LegacyConstantFilter>();
		return StringUtil::Format("%s %s %s", col, TransformComparison(cf.comparison_type),
		                          TransformConstant(cf.constant));
	}
	case TableFilterType::LEGACY_IN_FILTER: {
		auto &inf = filter.Cast<LegacyInFilter>();
		string in_list;
		for (auto &v : inf.values) {
			if (!in_list.empty()) {
				in_list += ",";
			}
			in_list += TransformConstant(v);
		}
		return col + " IN (" + in_list + ")";
	}
	case TableFilterType::LEGACY_OPTIONAL_FILTER: {
		auto &of = filter.Cast<LegacyOptionalFilter>();
		if (of.child_filter) {
			return TransformFilter(col, *of.child_filter);
		}
		return "";
	}
	case TableFilterType::EXPRESSION_FILTER: {
		auto &ef = filter.Cast<ExpressionFilter>();
		if (ef.expr) {
			return ExprFilterToSQL(*ef.expr, col);
		}
		return "";
	}
	default:
		return "";
	}
}

// Build WHERE clause directly from a LogicalGet's table_filters.
// In DuckDB v2.0 the filter key is a ProjectionIndex (position in the Get's
// column_ids projection list), NOT a physical column index.  column_ids maps
// projection position → ColumnIndex, whose GetPrimaryIndex() gives the
// physical column.  names must be the full original column list
// (bind_data.all_names).  Called by the optimizer before the agg-pushdown
// column rewrite replaces column_ids and makes the projection indices invalid.
string HttpSQLBuildWhereFromTableFilters(const TableFilterSet &filters, const vector<ColumnIndex> &column_ids,
                                          const vector<string> &names) {
	if (!filters.HasFilters()) {
		return "";
	}
	vector<string> parts;
	for (auto &entry : filters) {
		idx_t proj_idx = entry.GetIndex().GetIndex();
		if (proj_idx == COLUMN_IDENTIFIER_ROW_ID || proj_idx >= column_ids.size()) {
			continue;
		}
		auto &ci = column_ids[proj_idx];
		if (ci.IsRowIdColumn()) {
			continue;
		}
		idx_t phys_col = ci.GetPrimaryIndex();
		if (phys_col >= names.size()) {
			continue;
		}
		string col_name = "`" + names[phys_col] + "`";
		auto s = TransformFilter(col_name, entry.Filter());
		if (!s.empty()) {
			parts.push_back(s);
		}
	}
	return parts.empty() ? "" : StringUtil::Join(parts, " AND ");
}

// Build WHERE clause from ArrowStreamParameters::filters.
// filter_to_col maps filter column index → physical schema column index.
static string BuildWhereClause(const ArrowStreamParameters &parameters, const vector<string> &all_names) {
	if (!parameters.filters || !parameters.filters->HasFilters()) {
		return "";
	}
	vector<string> parts;
	for (auto &entry : *parameters.filters) {
		auto filter_col_idx = entry.GetIndex();
		auto &filter = entry.Filter();
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
		auto s = TransformFilter(col_name, filter);
		if (!s.empty()) {
			parts.push_back(s);
		}
	}
	return parts.empty() ? "" : StringUtil::Join(parts, " AND ");
}

// ─── Produce (called via HttpSQLArrowScanFactory::ProduceStream) ─────────────

static unique_ptr<ArrowArrayStreamWrapper> DoProduceStream(HttpSQLBindData &bind,
                                                           ArrowStreamParameters &parameters) {
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
		if (!bind.having_clause.empty()) {
			yyjson_mut_obj_add_strcpy(jdoc, jroot, "having", bind.having_clause.c_str());
		}
	}

	size_t json_len = 0;
	char *json_raw = yyjson_mut_write(jdoc, 0, &json_len);
	string body(json_raw, json_len);
	free(json_raw);
	yyjson_mut_doc_free(jdoc);

	auto resp = cat.http.PostStreaming("/api/query", body);
	if (!resp.ok()) {
		throw IOException("httpsql: POST /api/query failed (status %d): %s", resp.status_code,
		                  resp.error.c_str());
	}

	return HttpSQLMakeIPCStream(std::move(resp.body));
}

// ─── Scan function registration ───────────────────────────────────────────────

static unique_ptr<FunctionData> HttpSQLBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &,
                                             vector<Identifier> &) {
	throw InternalException("HttpSQLBind should not be called directly");
}

static InsertionOrderPreservingMap<string> HttpSQLToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<HttpSQLBindData>();
	result["Schema"] = bind_data.table.schema.name.GetIdentifierName();
	result["Table"] = bind_data.table.name.GetIdentifierName();
	return result;
}

static void HttpSQLSerialize(Serializer &, const optional_ptr<FunctionData>, const BoundTableFunction &) {
	throw NotImplementedException("HttpSQLSerialize");
}

static unique_ptr<FunctionData> HttpSQLDeserialize(Deserializer &, BoundTableFunction &) {
	throw NotImplementedException("HttpSQLDeserialize");
}

static BindInfo HttpSQLGetBindInfo(TableFunctionGetBindInfoInput &input) {
	BindInfo info(ScanType::EXTERNAL);
	return info;
}

TableFunction CreateHttpSQLScanFunction() {
	TableFunction func("httpsql_scan_internal", {}, ArrowTableFunction::ArrowScanFunction, HttpSQLBind,
	                   ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	func.to_string = HttpSQLToString;
	func.SetSerializeCallback(HttpSQLSerialize);
	func.SetDeserializeCallback(HttpSQLDeserialize);
	func.get_bind_info = HttpSQLGetBindInfo;
	func.projection_pushdown = true;
	func.filter_pushdown = true;
	return func;
}

} // namespace duckdb
