#pragma once
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "httpsql_transaction.hpp"

namespace duckdb {
class HttpSQLCatalog;

class HttpSQLTransactionManager : public TransactionManager {
public:
	HttpSQLTransactionManager(AttachedDatabase &db_p, HttpSQLCatalog &catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	HttpSQLCatalog &catalog_;
	mutex lock_;
	reference_map_t<Transaction, unique_ptr<HttpSQLTransaction>> transactions_;
};

} // namespace duckdb
