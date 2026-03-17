#include "httpsql_transaction_manager.hpp"
#include "httpsql_catalog.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

HttpSQLTransactionManager::HttpSQLTransactionManager(AttachedDatabase &db_p, HttpSQLCatalog &catalog)
    : TransactionManager(db_p), catalog_(catalog) {
}

Transaction &HttpSQLTransactionManager::StartTransaction(ClientContext &context) {
	auto txn = make_uniq<HttpSQLTransaction>(catalog_, *this, context);
	txn->Start();
	auto &result = *txn;
	lock_guard<mutex> l(lock_);
	transactions_[result] = std::move(txn);
	return result;
}

ErrorData HttpSQLTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	transaction.Cast<HttpSQLTransaction>().Commit();
	lock_guard<mutex> l(lock_);
	transactions_.erase(transaction);
	return ErrorData();
}

void HttpSQLTransactionManager::RollbackTransaction(Transaction &transaction) {
	try { transaction.Cast<HttpSQLTransaction>().Rollback(); } catch (...) {}
	lock_guard<mutex> l(lock_);
	transactions_.erase(transaction);
}

void HttpSQLTransactionManager::Checkpoint(ClientContext &, bool) {}

} // namespace duckdb
