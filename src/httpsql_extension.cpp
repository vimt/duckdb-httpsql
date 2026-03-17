#include "httpsql_extension.hpp"
#include "httpsql_storage.hpp"
#include "httpsql_optimizer.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

using namespace duckdb;

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	StorageExtension::Register(config, "httpsql", make_shared_ptr<HttpSQLStorageExtension>());

	OptimizerExtension opt;
	opt.optimize_function = HttpSQLOptimizer::Optimize;
	OptimizerExtension::Register(config, std::move(opt));
}

void HttpSQLExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(httpsql, loader) {
	LoadInternal(loader);
}
}
