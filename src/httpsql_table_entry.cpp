#include "httpsql_table_entry.hpp"
#include "httpsql_scanner.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

HttpSQLTableEntry::HttpSQLTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                         LogicTableInfo logic_table_info_p)
    : TableCatalogEntry(catalog, schema, info), logic_table_info(std::move(logic_table_info_p)) {
}

unique_ptr<BaseStatistics> HttpSQLTableEntry::GetStatistics(ClientContext &, column_t) {
	return nullptr;
}

void HttpSQLTableEntry::BindUpdateConstraints(Binder &, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                                 ClientContext &) {
}

TableFunction HttpSQLTableEntry::GetScanFunction(ClientContext &, unique_ptr<FunctionData> &bind_data) {
	auto result = make_uniq<HttpSQLBindData>(*this);
	for (auto &col : columns.Logical()) {
		result->types.push_back(col.GetType());
		result->names.push_back(col.GetName());
	}
	bind_data = std::move(result);
	return CreateHttpSQLScanFunction();
}

TableStorageInfo HttpSQLTableEntry::GetStorageInfo(ClientContext &) {
	TableStorageInfo result;
	result.cardinality = logic_table_info.physical_tables.size() * 1000; // rough estimate
	return result;
}

} // namespace duckdb
