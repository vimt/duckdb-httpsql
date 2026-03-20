#pragma once
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include <chrono>

namespace duckdb {

class HttpSQLSchemaEntry : public SchemaCatalogEntry {
public:
	HttpSQLSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, int ttl_sec = 60);

	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction, BoundCreateTableInfo &) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction, CreateFunctionInfo &) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction, CreateIndexInfo &, TableCatalogEntry &) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction, CreateViewInfo &) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction, CreateSequenceInfo &) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction, CreateCollationInfo &) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction, CreateTypeInfo &) override;
	void Alter(CatalogTransaction, AlterInfo &) override;
	void Scan(ClientContext &context, CatalogType type,
	          const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &, DropInfo &) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction,
	                                        const EntryLookupInfo &lookup_info) override;

private:
	void EnsureTablesLoaded(ClientContext &context);

	int ttl_sec_;
	unordered_map<string, unique_ptr<CatalogEntry>> tables_;
	bool tables_loaded_ = false;
	std::chrono::steady_clock::time_point tables_loaded_at_;
	mutex tables_lock_;
};

} // namespace duckdb
