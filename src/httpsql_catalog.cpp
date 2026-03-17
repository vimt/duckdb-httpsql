#include "httpsql_catalog.hpp"
#include "httpsql_schema_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

HttpSQLCatalog::HttpSQLCatalog(AttachedDatabase &db_p, const string &server_url)
    : Catalog(db_p), http(server_url), server_url_(server_url) {
}

void HttpSQLCatalog::Initialize(bool) {}

static vector<string> ParseStringArray(const string &json) {
	vector<string> result;
	auto *doc = yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) return result;
	auto *root = yyjson_doc_get_root(doc);
	if (yyjson_is_arr(root)) {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(root, idx, max, val) {
			if (yyjson_is_str(val)) {
				result.emplace_back(yyjson_get_str(val));
			}
		}
	}
	yyjson_doc_free(doc);
	return result;
}

void HttpSQLCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto resp = http.Get("/api/schemas");
	if (!resp.ok()) {
		throw IOException("httpsql: GET /api/schemas failed: %s", resp.error);
	}
	auto names = ParseStringArray(resp.body);

	lock_guard<mutex> l(schema_lock_);
	for (auto &name : names) {
		if (schema_entries_.find(name) == schema_entries_.end()) {
			CreateSchemaInfo info;
			info.schema = name;
			schema_entries_[name] = make_uniq<HttpSQLSchemaEntry>(*this, info);
		}
		callback(*schema_entries_[name]);
	}
}

optional_ptr<SchemaCatalogEntry> HttpSQLCatalog::LookupSchema(CatalogTransaction transaction,
                                                                  const EntryLookupInfo &schema_lookup,
                                                                  OnEntryNotFound if_not_found) {
	auto resp = http.Get("/api/schemas");
	if (!resp.ok()) {
		if (if_not_found != OnEntryNotFound::RETURN_NULL) {
			throw IOException("httpsql: GET /api/schemas failed: %s", resp.error);
		}
		return nullptr;
	}
	auto all_schemas = ParseStringArray(resp.body);
	auto schema_name = schema_lookup.GetEntryName();

	if (schema_name == DEFAULT_SCHEMA) {
		if (all_schemas.empty()) {
			if (if_not_found != OnEntryNotFound::RETURN_NULL) {
				throw BinderException("No schemas available in httpsql catalog");
			}
			return nullptr;
		}
		schema_name = all_schemas[0];
	}

	bool found = false;
	for (auto &s : all_schemas) {
		if (s == schema_name) { found = true; break; }
	}
	if (!found) {
		if (if_not_found != OnEntryNotFound::RETURN_NULL) {
			throw BinderException("Schema \"%s\" not found in httpsql catalog", schema_name);
		}
		return nullptr;
	}

	lock_guard<mutex> l(schema_lock_);
	if (schema_entries_.find(schema_name) == schema_entries_.end()) {
		CreateSchemaInfo info;
		info.schema = schema_name;
		schema_entries_[schema_name] = make_uniq<HttpSQLSchemaEntry>(*this, info);
	}
	return schema_entries_[schema_name].get();
}

optional_ptr<CatalogEntry> HttpSQLCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
void HttpSQLCatalog::DropSchema(ClientContext &, DropInfo &) {
	throw BinderException("httpsql catalog is read-only");
}
PhysicalOperator &HttpSQLCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &, LogicalCreateTable &,
                                                       PhysicalOperator &) {
	throw NotImplementedException("httpsql catalog is read-only");
}
PhysicalOperator &HttpSQLCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
                                                optional_ptr<PhysicalOperator>) {
	throw NotImplementedException("httpsql catalog is read-only");
}
PhysicalOperator &HttpSQLCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &,
                                                PhysicalOperator &) {
	throw NotImplementedException("httpsql catalog is read-only");
}
PhysicalOperator &HttpSQLCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &,
                                                PhysicalOperator &) {
	throw NotImplementedException("httpsql catalog is read-only");
}
unique_ptr<LogicalOperator> HttpSQLCatalog::BindCreateIndex(Binder &, CreateStatement &, TableCatalogEntry &,
                                                               unique_ptr<LogicalOperator>) {
	throw NotImplementedException("httpsql catalog is read-only");
}
DatabaseSize HttpSQLCatalog::GetDatabaseSize(ClientContext &) {
	DatabaseSize size;
	memset(&size, 0, sizeof(size));
	return size;
}

} // namespace duckdb
