#pragma once
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "httpsql_config.hpp"

namespace duckdb {

class HttpSQLTableEntry : public TableCatalogEntry {
public:
	HttpSQLTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);

	const ColumnList &GetColumns() const override {
		return column_list_;
	}
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &, column_t) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &) override;
	void BindUpdateConstraints(Binder &, LogicalGet &, LogicalProjection &, LogicalUpdate &,
	                           ClientContext &) override;

private:
	ColumnList column_list_;
	idx_t row_estimate_ = 0;

public:
	void SetRowEstimate(idx_t n) { row_estimate_ = n; }
};

} // namespace duckdb
