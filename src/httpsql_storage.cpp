#include "httpsql_storage.hpp"
#include "httpsql_catalog.hpp"
#include "httpsql_transaction_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {

static unique_ptr<Catalog> HttpSQLAttach(optional_ptr<StorageExtensionInfo>, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &attach_options) {
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("Attaching httpsql databases is disabled through configuration");
	}

	string url = info.path;
	if (url.empty()) {
		throw BinderException("httpsql requires a server URL: ATTACH 'http://host:port' AS name (TYPE httpsql)  "
		                      "or ATTACH 'http+unix:///path/to/socket' AS name (TYPE httpsql)");
	}

	int64_t timeout_sec = 30;
	auto timeout_it = info.options.find("timeout");
	if (timeout_it != info.options.end()) {
		try {
			timeout_sec = timeout_it->second.GetValue<int64_t>();
		} catch (...) {
			throw BinderException("httpsql: 'timeout' option must be an integer (seconds)");
		}
	}

	int64_t schema_ttl_sec = 60;
	auto ttl_it = info.options.find("schema_ttl");
	if (ttl_it != info.options.end()) {
		try {
			schema_ttl_sec = ttl_it->second.GetValue<int64_t>();
			if (schema_ttl_sec < 0) {
				throw BinderException("httpsql: 'schema_ttl' must be >= 0 (0 = never expire)");
			}
		} catch (BinderException &) {
			throw;
		} catch (...) {
			throw BinderException("httpsql: 'schema_ttl' option must be an integer (seconds)");
		}
	}

	attach_options.access_mode = AccessMode::READ_ONLY;
	return make_uniq<HttpSQLCatalog>(db, url, (int)timeout_sec, (int)schema_ttl_sec);
}

static unique_ptr<TransactionManager> HttpSQLCreateTransactionManager(
    optional_ptr<StorageExtensionInfo>, AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<HttpSQLTransactionManager>(db, catalog.Cast<HttpSQLCatalog>());
}

HttpSQLStorageExtension::HttpSQLStorageExtension() {
	attach = HttpSQLAttach;
	create_transaction_manager = HttpSQLCreateTransactionManager;
}

} // namespace duckdb
