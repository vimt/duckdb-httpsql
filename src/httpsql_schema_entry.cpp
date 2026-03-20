#include "httpsql_schema_entry.hpp"
#include "httpsql_catalog.hpp"
#include "httpsql_table_entry.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

// DuckDBTypeToLogical parses a DuckDB type string supplied by the Go server's
// duckdb_type field (e.g. "DECIMAL(10,4)", "TIMESTAMP", "JSON") into a LogicalType.
static LogicalType DuckDBTypeToLogical(const string &type) {
	string upper = type;
	StringUtil::Trim(upper);
	StringUtil::Upper(upper);
	if (upper == "TINYINT")   return LogicalType::TINYINT;
	if (upper == "SMALLINT")  return LogicalType::SMALLINT;
	if (upper == "INTEGER")   return LogicalType::INTEGER;
	if (upper == "BIGINT")    return LogicalType::BIGINT;
	if (upper == "UTINYINT")  return LogicalType::UTINYINT;
	if (upper == "USMALLINT") return LogicalType::USMALLINT;
	if (upper == "UINTEGER")  return LogicalType::UINTEGER;
	if (upper == "UBIGINT")   return LogicalType::UBIGINT;
	if (upper == "FLOAT")     return LogicalType::FLOAT;
	if (upper == "DOUBLE")    return LogicalType::DOUBLE;
	if (upper == "DATE")      return LogicalType::DATE;
	if (upper == "TIMESTAMP") return LogicalType::TIMESTAMP;
	if (upper == "TIME")      return LogicalType::TIME;
	if (upper == "BLOB")      return LogicalType::BLOB;
	if (upper == "JSON")      return LogicalType::JSON();
	if (upper == "VARCHAR")   return LogicalType::VARCHAR;
	// DECIMAL(precision,scale)
	if (upper.substr(0, 7) == "DECIMAL") {
		auto lp = upper.find('(');
		auto cm = upper.find(',', lp);
		auto rp = upper.find(')', cm);
		if (lp != string::npos && cm != string::npos && rp != string::npos) {
			int64_t p = std::stoll(upper.substr(lp + 1, cm - lp - 1));
			int64_t s = std::stoll(upper.substr(cm + 1, rp - cm - 1));
			return LogicalType::DECIMAL(p, s);
		}
	}
	return LogicalType::VARCHAR;
}

struct TableParseResult {
	string name;
	vector<ColumnInfo> columns;
};

static vector<TableParseResult> ParseTablesJSON(const string &json) {
	vector<TableParseResult> result;
	auto *doc = yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) return result;
	auto *root = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(root)) { yyjson_doc_free(doc); return result; }

	size_t tidx, tmax;
	yyjson_val *tobj;
	yyjson_arr_foreach(root, tidx, tmax, tobj) {
		if (!yyjson_is_obj(tobj)) continue;
		TableParseResult tbl;

		auto *name = yyjson_obj_get(tobj, "name");
		if (!name || !yyjson_is_str(name)) continue;
		tbl.name = yyjson_get_str(name);

		auto *cols = yyjson_obj_get(tobj, "columns");
		if (cols && yyjson_is_arr(cols)) {
			size_t cidx, cmax;
			yyjson_val *cobj;
			yyjson_arr_foreach(cols, cidx, cmax, cobj) {
				if (!yyjson_is_obj(cobj)) continue;
				ColumnInfo col;
				auto *cname       = yyjson_obj_get(cobj, "name");
				auto *nullable    = yyjson_obj_get(cobj, "nullable");
				auto *duckdb_type = yyjson_obj_get(cobj, "duckdb_type");
				if (cname       && yyjson_is_str(cname))       col.name        = yyjson_get_str(cname);
				if (nullable    && yyjson_is_bool(nullable))   col.is_nullable = yyjson_get_bool(nullable);
				if (duckdb_type && yyjson_is_str(duckdb_type)) col.duckdb_type = yyjson_get_str(duckdb_type);
				tbl.columns.push_back(std::move(col));
			}
		}

		result.push_back(std::move(tbl));
	}
	yyjson_doc_free(doc);
	return result;
}

HttpSQLSchemaEntry::HttpSQLSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, int ttl_sec)
    : SchemaCatalogEntry(catalog, info), ttl_sec_(ttl_sec) {
}

void HttpSQLSchemaEntry::EnsureTablesLoaded(ClientContext &context) {
	lock_guard<mutex> l(tables_lock_);
	if (tables_loaded_) {
		if (ttl_sec_ == 0) return;
		auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
		    std::chrono::steady_clock::now() - tables_loaded_at_).count();
		if (elapsed < ttl_sec_) return;
		tables_.clear();
		tables_loaded_ = false;
	}

	auto &cat = catalog.Cast<HttpSQLCatalog>();
	auto resp = cat.http.Get("/api/schemas/" + name + "/tables");
	if (!resp.ok()) {
		throw IOException("httpsql: GET /api/schemas/%s/tables failed (status %d): %s",
		                  name, resp.status_code, resp.error);
	}

	for (auto &tbl : ParseTablesJSON(resp.body)) {
		if (tbl.columns.empty()) continue;

		auto create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)*this, tbl.name);
		for (auto &col : tbl.columns) {
			ColumnDefinition column(col.name, DuckDBTypeToLogical(col.duckdb_type));
			if (!col.is_nullable) {
				auto col_idx = create_info->columns.LogicalColumnCount();
				create_info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(col_idx)));
			}
			create_info->columns.AddColumn(std::move(column));
		}
		if (create_info->columns.LogicalColumnCount() > 0) {
			auto entry = make_uniq<HttpSQLTableEntry>(catalog, *this, *create_info);
			tables_[tbl.name] = std::move(entry);
		}
	}
	tables_loaded_at_ = std::chrono::steady_clock::now();
	tables_loaded_ = true;
}

optional_ptr<CatalogEntry> HttpSQLSchemaEntry::CreateTable(CatalogTransaction, BoundCreateTableInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
optional_ptr<CatalogEntry> HttpSQLSchemaEntry::CreateFunction(CatalogTransaction, CreateFunctionInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
optional_ptr<CatalogEntry> HttpSQLSchemaEntry::CreateIndex(CatalogTransaction, CreateIndexInfo &,
                                                              TableCatalogEntry &) {
	throw BinderException("httpsql catalog is read-only");
}
optional_ptr<CatalogEntry> HttpSQLSchemaEntry::CreateView(CatalogTransaction, CreateViewInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
optional_ptr<CatalogEntry> HttpSQLSchemaEntry::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
optional_ptr<CatalogEntry> HttpSQLSchemaEntry::CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
optional_ptr<CatalogEntry> HttpSQLSchemaEntry::CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
optional_ptr<CatalogEntry> HttpSQLSchemaEntry::CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
optional_ptr<CatalogEntry> HttpSQLSchemaEntry::CreateCollation(CatalogTransaction, CreateCollationInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
optional_ptr<CatalogEntry> HttpSQLSchemaEntry::CreateType(CatalogTransaction, CreateTypeInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
void HttpSQLSchemaEntry::Alter(CatalogTransaction, AlterInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
void HttpSQLSchemaEntry::DropEntry(ClientContext &, DropInfo &) {
	throw BinderException("httpsql catalog is read-only");
}

void HttpSQLSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                 const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) return;
	EnsureTablesLoaded(context);
	for (auto &kv : tables_) callback(*kv.second);
}

void HttpSQLSchemaEntry::Scan(CatalogType, const std::function<void(CatalogEntry &)> &) {
	throw NotImplementedException("Scan without context not supported");
}

optional_ptr<CatalogEntry> HttpSQLSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                               const EntryLookupInfo &lookup_info) {
	auto type = lookup_info.GetCatalogType();
	if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) return nullptr;
	EnsureTablesLoaded(transaction.GetContext());
	auto it = tables_.find(lookup_info.GetEntryName());
	return it == tables_.end() ? nullptr : optional_ptr<CatalogEntry>(it->second.get());
}

} // namespace duckdb
