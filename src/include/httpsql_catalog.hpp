#pragma once
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/mutex.hpp"
#include "httpsql_http_client.hpp"
#include "httpsql_config.hpp"
#include "httpsql_schema_entry.hpp"

namespace duckdb {

class HttpSQLCatalog : public Catalog {
public:
	HttpSQLCatalog(AttachedDatabase &db_p, const string &server_url);
	~HttpSQLCatalog() override = default;

	HttpSQLHttpClient http;

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override { return "httpsql"; }

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction, CreateSchemaInfo &) override;
	void DropSchema(ClientContext &, DropInfo &) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
	                                               const EntryLookupInfo &schema_lookup,
	                                               OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &, LogicalCreateTable &,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
	                             optional_ptr<PhysicalOperator>) override;
	PhysicalOperator &PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &,
	                             PhysicalOperator &) override;
	PhysicalOperator &PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &,
	                             PhysicalOperator &) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &, CreateStatement &, TableCatalogEntry &,
	                                            unique_ptr<LogicalOperator>) override;
	DatabaseSize GetDatabaseSize(ClientContext &) override;
	bool InMemory() override { return false; }
	string GetDBPath() override { return server_url_; }

private:
	string server_url_;
	unordered_map<string, unique_ptr<HttpSQLSchemaEntry>> schema_entries_;
	mutex schema_lock_;
};

} // namespace duckdb
