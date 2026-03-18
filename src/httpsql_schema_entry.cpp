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

static LogicalType TypeNameToDuckDB(const string &type_name, const string &column_type,
                                      int64_t precision, int64_t scale) {
	auto lower = StringUtil::Lower(type_name);
	bool is_unsigned = column_type.find("unsigned") != string::npos;

	if (lower == "tinyint") return is_unsigned ? LogicalType::UTINYINT : LogicalType::TINYINT;
	if (lower == "smallint") return is_unsigned ? LogicalType::USMALLINT : LogicalType::SMALLINT;
	if (lower == "mediumint" || lower == "int" || lower == "integer")
		return is_unsigned ? LogicalType::UINTEGER : LogicalType::INTEGER;
	if (lower == "bigint") return is_unsigned ? LogicalType::UBIGINT : LogicalType::BIGINT;
	if (lower == "float") return LogicalType::FLOAT;
	if (lower == "double" || lower == "real") return LogicalType::DOUBLE;
	if (lower == "decimal" || lower == "numeric") {
		if (precision > 0) {
			return LogicalType::DECIMAL(precision, scale >= 0 ? scale : 0);
		}
		return LogicalType::DOUBLE;
	}
	if (lower == "date") return LogicalType::DATE;
	if (lower == "datetime" || lower == "timestamp") return LogicalType::TIMESTAMP;
	if (lower == "time") return LogicalType::TIME;
	if (lower == "year") return LogicalType::INTEGER;
	if (lower == "bit" || lower == "binary" || lower == "varbinary" ||
	    lower == "tinyblob" || lower == "blob" || lower == "mediumblob" || lower == "longblob")
		return LogicalType::BLOB;
	if (lower == "json") return LogicalType::JSON();
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
				auto *cname     = yyjson_obj_get(cobj, "name");
				auto *type_name = yyjson_obj_get(cobj, "type_name");
				auto *col_type  = yyjson_obj_get(cobj, "column_type");
				auto *nullable  = yyjson_obj_get(cobj, "nullable");
				auto *precision = yyjson_obj_get(cobj, "precision");
				auto *scale     = yyjson_obj_get(cobj, "scale");
				if (cname     && yyjson_is_str(cname))     col.name        = yyjson_get_str(cname);
				if (type_name && yyjson_is_str(type_name)) col.type_name   = yyjson_get_str(type_name);
				if (col_type  && yyjson_is_str(col_type))  col.column_type = yyjson_get_str(col_type);
				if (nullable  && yyjson_is_bool(nullable)) col.is_nullable = yyjson_get_bool(nullable);
				if (precision) col.precision = yyjson_is_null(precision) ? -1 : (int64_t)yyjson_get_sint(precision);
				if (scale)     col.scale     = yyjson_is_null(scale)     ? -1 : (int64_t)yyjson_get_sint(scale);
				tbl.columns.push_back(std::move(col));
			}
		}

		result.push_back(std::move(tbl));
	}
	yyjson_doc_free(doc);
	return result;
}

HttpSQLSchemaEntry::HttpSQLSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info) {
}

void HttpSQLSchemaEntry::EnsureTablesLoaded(ClientContext &context) {
	lock_guard<mutex> l(tables_lock_);
	if (tables_loaded_) return;

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
			auto col_type = TypeNameToDuckDB(col.type_name, col.column_type, col.precision, col.scale);
			ColumnDefinition column(col.name, std::move(col_type));
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
