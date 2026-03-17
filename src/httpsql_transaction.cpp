#include "httpsql_transaction.hpp"
#include "httpsql_catalog.hpp"

namespace duckdb {

HttpSQLTransaction::HttpSQLTransaction(HttpSQLCatalog &catalog, TransactionManager &manager,
                                           ClientContext &context)
    : Transaction(manager, context), catalog(catalog) {
}
HttpSQLTransaction::~HttpSQLTransaction() = default;
void HttpSQLTransaction::Start() {}
void HttpSQLTransaction::Commit() {}
void HttpSQLTransaction::Rollback() {}

HttpSQLTransaction &HttpSQLTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<HttpSQLTransaction>();
}

} // namespace duckdb
