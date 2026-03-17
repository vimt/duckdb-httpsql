#pragma once
#include "duckdb.hpp"

namespace duckdb {

class HttpSQLExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override { return "httpsql"; }
	std::string Version() const override { return "v0.0.1"; }
};

} // namespace duckdb
