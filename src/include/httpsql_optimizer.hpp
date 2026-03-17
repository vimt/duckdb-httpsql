#pragma once
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class HttpSQLOptimizer {
public:
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
