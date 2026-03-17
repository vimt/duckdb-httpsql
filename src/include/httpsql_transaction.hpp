#pragma once
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class HttpSQLCatalog;

class HttpSQLTransaction : public Transaction {
public:
	HttpSQLTransaction(HttpSQLCatalog &catalog, TransactionManager &manager, ClientContext &context);
	~HttpSQLTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static HttpSQLTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	HttpSQLCatalog &catalog;
};

} // namespace duckdb
