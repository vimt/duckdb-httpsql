#include "httpsql_table_entry.hpp"
#include "httpsql_scanner.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

HttpSQLTableEntry::HttpSQLTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
}

unique_ptr<BaseStatistics> HttpSQLTableEntry::GetStatistics(ClientContext &, column_t) {
	return nullptr;
}

void HttpSQLTableEntry::BindUpdateConstraints(Binder &, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                               ClientContext &) {
}

TableFunction HttpSQLTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto result = make_uniq<HttpSQLBindData>(*this);

	// Collect column types and names from the catalog entry.
	vector<LogicalType> types;
	vector<string> names;
	for (auto &col : columns.Logical()) {
		types.push_back(col.GetType());
		names.push_back(col.GetName());
	}
	result->all_names = names;
	result->all_types = types; // used by ArrowScanFunctionData

	// Synthesize an Arrow schema from DuckDB types so that ArrowScanInitGlobal
	// can build the projection map without making a network call at bind time.
	auto options = context.GetClientProperties();
	ArrowConverter::ToArrowSchema(&result->schema_root.arrow_schema, types, names, options);

	// Populate arrow_table so ArrowScanFunction knows types/names.
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table,
	                                              result->schema_root.arrow_schema);

	bind_data = std::move(result);
	return CreateHttpSQLScanFunction();
}

TableStorageInfo HttpSQLTableEntry::GetStorageInfo(ClientContext &) {
	TableStorageInfo result;
	result.cardinality = 0;
	return result;
}

} // namespace duckdb
