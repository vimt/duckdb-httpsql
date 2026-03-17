#pragma once
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class HttpSQLStorageExtension : public StorageExtension {
public:
	HttpSQLStorageExtension();
};

} // namespace duckdb
