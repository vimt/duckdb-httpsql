#include "httpsql_scanner.hpp"
#include "httpsql_catalog.hpp"
#include "httpsql_table_entry.hpp"
#include "httpsql_http_client.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

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

// ─── Global state: holds the NDJSON response and current position ──────────

struct HttpSQLGlobalState : public GlobalTableFunctionState {
	vector<string> lines;      // data lines (excluding header)
	idx_t current_line = 0;
	vector<LogicalType> server_types;
	bool done = false;
	bool initialized = false;

	idx_t MaxThreads() const override { return 1; }
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

	// Parse response: split into lines
	auto &raw = resp.body;
	idx_t pos = 0;
	bool first_line = true;
	while (pos < raw.size()) {
		auto nl = raw.find('\n', pos);
		string line;
		if (nl == string::npos) {
			line = raw.substr(pos);
			pos = raw.size();
		} else {
			line = raw.substr(pos, nl - pos);
			pos = nl + 1;
		}
		if (line.empty()) continue;

		if (first_line) {
			// Parse column metadata header: {"columns":[{"name":"...","type":"..."},...]}}
			auto *doc = yyjson_read(line.c_str(), line.size(), 0);
			if (doc) {
				auto *root = yyjson_doc_get_root(doc);
				auto *cols = yyjson_obj_get(root, "columns");
				if (cols && yyjson_is_arr(cols)) {
					size_t cidx, cmax;
					yyjson_val *cobj;
					yyjson_arr_foreach(cols, cidx, cmax, cobj) {
						auto *type_val = yyjson_obj_get(cobj, "type");
						string type_str = type_val ? yyjson_get_str(type_val) : "VARCHAR";
						// Map type string to LogicalType
						LogicalType lt = LogicalType::VARCHAR;
						if (type_str == "BIGINT") lt = LogicalType::BIGINT;
						else if (type_str == "INTEGER") lt = LogicalType::INTEGER;
						else if (type_str == "DOUBLE") lt = LogicalType::DOUBLE;
						else if (type_str == "FLOAT") lt = LogicalType::FLOAT;
						else if (type_str == "BOOLEAN") lt = LogicalType::BOOLEAN;
						else if (type_str == "DATE") lt = LogicalType::DATE;
						else if (type_str == "TIMESTAMP") lt = LogicalType::TIMESTAMP;
						else if (type_str == "BLOB") lt = LogicalType::BLOB;
						else if (type_str.substr(0, 7) == "DECIMAL") lt = LogicalType::DOUBLE;
						gstate->server_types.push_back(std::move(lt));
					}
				}
				// Check for error in header
				auto *err = yyjson_obj_get(root, "error");
				if (err && yyjson_is_str(err)) {
					yyjson_doc_free(doc);
					throw IOException("httpsql server error: %s", yyjson_get_str(err));
				}
				yyjson_doc_free(doc);
			}
			first_line = false;
		} else {
			gstate->lines.push_back(std::move(line));
		}
	}

	gstate->initialized = true;
	return std::move(gstate);
}

static unique_ptr<LocalTableFunctionState> HttpSQLInitLocalState(ExecutionContext &, TableFunctionInitInput &,
                                                                    GlobalTableFunctionState *) {
	return make_uniq<HttpSQLLocalState>();
}

// ─── Scan: parse next batch of NDJSON rows into DataChunk ─────────────────

static Value ParseJSONValue(yyjson_val *val, const LogicalType &target_type) {
	if (!val || yyjson_is_null(val)) return Value(target_type);

	// Helper lambdas: yyjson distinguishes integer and real nodes.
	// yyjson_get_real() returns 0.0 for integer nodes, so we must check explicitly.
	auto get_double = [&](yyjson_val *v) -> double {
		if (yyjson_is_real(v)) return yyjson_get_real(v);
		if (yyjson_is_sint(v)) return (double)yyjson_get_sint(v);
		if (yyjson_is_uint(v)) return (double)yyjson_get_uint(v);
		return 0.0;
	};
	auto get_sint = [&](yyjson_val *v) -> int64_t {
		if (yyjson_is_sint(v)) return yyjson_get_sint(v);
		if (yyjson_is_uint(v)) return (int64_t)yyjson_get_uint(v);
		if (yyjson_is_real(v)) return (int64_t)yyjson_get_real(v);
		return 0;
	};
	auto get_uint = [&](yyjson_val *v) -> uint64_t {
		if (yyjson_is_uint(v)) return yyjson_get_uint(v);
		if (yyjson_is_sint(v)) return (uint64_t)yyjson_get_sint(v);
		if (yyjson_is_real(v)) return (uint64_t)yyjson_get_real(v);
		return 0;
	};

	switch (target_type.id()) {
	case LogicalTypeId::BOOLEAN:
		if (yyjson_is_bool(val)) return Value::BOOLEAN(yyjson_get_bool(val));
		if (yyjson_is_num(val)) return Value::BOOLEAN(get_sint(val) != 0);
		break;
	case LogicalTypeId::TINYINT:
		if (yyjson_is_num(val)) return Value::TINYINT((int8_t)get_sint(val));
		break;
	case LogicalTypeId::SMALLINT:
		if (yyjson_is_num(val)) return Value::SMALLINT((int16_t)get_sint(val));
		break;
	case LogicalTypeId::INTEGER:
		if (yyjson_is_num(val)) return Value::INTEGER((int32_t)get_sint(val));
		break;
	case LogicalTypeId::BIGINT:
		if (yyjson_is_num(val)) return Value::BIGINT(get_sint(val));
		break;
	case LogicalTypeId::UTINYINT:
		if (yyjson_is_num(val)) return Value::UTINYINT((uint8_t)get_uint(val));
		break;
	case LogicalTypeId::USMALLINT:
		if (yyjson_is_num(val)) return Value::USMALLINT((uint16_t)get_uint(val));
		break;
	case LogicalTypeId::UINTEGER:
		if (yyjson_is_num(val)) return Value::UINTEGER((uint32_t)get_uint(val));
		break;
	case LogicalTypeId::UBIGINT:
		if (yyjson_is_num(val)) return Value::UBIGINT(get_uint(val));
		break;
	case LogicalTypeId::FLOAT:
		if (yyjson_is_num(val)) return Value::FLOAT((float)get_double(val));
		break;
	case LogicalTypeId::DOUBLE:
		if (yyjson_is_num(val)) return Value::DOUBLE(get_double(val));
		break;
	default:
		break;
	}

	// Fallback: use string representation
	if (yyjson_is_str(val)) return Value(yyjson_get_str(val));
	if (yyjson_is_int(val)) return Value(std::to_string(yyjson_get_sint(val)));
	if (yyjson_is_real(val)) return Value(std::to_string(yyjson_get_real(val)));
	if (yyjson_is_bool(val)) return Value(yyjson_get_bool(val) ? "1" : "0");
	return Value(target_type);
}

static void HttpSQLScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<HttpSQLGlobalState>();
	auto &bind_data = data.bind_data->Cast<HttpSQLBindData>();

	if (gstate.done || gstate.current_line >= gstate.lines.size()) {
		gstate.done = true;
		output.SetCardinality(0);
		return;
	}

	idx_t row_count = 0;
	idx_t batch_end = MinValue<idx_t>(gstate.current_line + STANDARD_VECTOR_SIZE, gstate.lines.size());

	while (gstate.current_line < batch_end) {
		auto &line = gstate.lines[gstate.current_line++];
		if (line.empty()) continue;

		auto *doc = yyjson_read(line.c_str(), line.size(), 0);
		if (!doc) continue;
		auto *root = yyjson_doc_get_root(doc);
		if (!yyjson_is_arr(root)) { yyjson_doc_free(doc); continue; }

		// Fill each column
		for (idx_t col = 0; col < output.ColumnCount(); col++) {
			auto *jval = yyjson_arr_get(root, col);
			auto &out_type = output.data[col].GetType();
			Value v = ParseJSONValue(jval, out_type);

			// Cast if needed (e.g. server returns string for date)
			if (!v.IsNull() && v.type() != out_type) {
				try {
					v = v.CastAs(context, out_type);
				} catch (...) {
					v = Value(out_type);
				}
			}
			output.data[col].SetValue(row_count, v);
		}
		row_count++;
		yyjson_doc_free(doc);
	}

	output.SetCardinality(row_count);
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
