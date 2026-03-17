#include "httpsql_optimizer.hpp"
#include "httpsql_scanner.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

static const char *HTTPSQL_SCAN_NAME = "httpsql_scan_internal";

static bool IsHttpSQLScan(const LogicalGet &get) {
	return get.function.name == HTTPSQL_SCAN_NAME;
}

static LogicalOperator &SkipProjections(LogicalOperator &op) {
	LogicalOperator *cur = &op;
	while (cur->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		cur = cur->children[0].get();
	}
	return *cur;
}

static string WriteIdentifier(const string &col) {
	return "`" + col + "`";
}

static string ResolveToColumnName(ColumnBinding binding, LogicalOperator &start_op) {
	LogicalOperator *cur = &start_op;
	while (cur) {
		if (cur->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = cur->Cast<LogicalGet>();
			if (binding.table_index != get.table_index) return "";
			auto &col_ids = get.GetColumnIds();
			if (binding.column_index >= col_ids.size()) return "";
			auto &ci = col_ids[binding.column_index];
			if (ci.IsRowIdColumn() || ci.GetPrimaryIndex() >= get.names.size()) return "";
			return get.names[ci.GetPrimaryIndex()];
		}
		if (cur->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &proj = cur->Cast<LogicalProjection>();
			if (proj.table_index == binding.table_index) {
				if (binding.column_index >= proj.expressions.size()) return "";
				auto *expr = proj.expressions[binding.column_index].get();
				if (expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) return "";
				binding = reinterpret_cast<BoundColumnRefExpression *>(expr)->binding;
			}
		}
		if (cur->children.empty()) break;
		cur = cur->children[0].get();
	}
	return "";
}

static string BuildOrderClause(const vector<BoundOrderByNode> &orders, LogicalOperator &child_of_sort) {
	string parts;
	for (auto &node : orders) {
		if (node.expression->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) return "";
		auto &col_ref = node.expression->Cast<BoundColumnRefExpression>();
		string col_name = ResolveToColumnName(col_ref.binding, child_of_sort);
		if (col_name.empty()) return "";
		if (!parts.empty()) parts += ", ";
		parts += WriteIdentifier(col_name);
		parts += (node.type == OrderType::DESCENDING) ? " DESC" : " ASC";
	}
	return parts.empty() ? "" : " ORDER BY " + parts;
}

static void HandleLimit(LogicalLimit &limit) {
	if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE &&
	    limit.limit_val.Type() != LimitNodeType::UNSET) return;
	if (limit.offset_val.Type() != LimitNodeType::CONSTANT_VALUE &&
	    limit.offset_val.Type() != LimitNodeType::UNSET) return;

	D_ASSERT(!limit.children.empty());
	LogicalOperator &child = SkipProjections(*limit.children[0]);

	if (child.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = child.Cast<LogicalGet>();
		if (!IsHttpSQLScan(get)) return;
		auto &bd = get.bind_data->Cast<HttpSQLBindData>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE)
			bd.limit_clause = " LIMIT " + to_string(limit.limit_val.GetConstantValue());
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE)
			bd.limit_clause += " OFFSET " + to_string(limit.offset_val.GetConstantValue());
		return;
	}

	if (child.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order_op = child.Cast<LogicalOrder>();
		LogicalOperator &order_child = SkipProjections(*order_op.children[0]);
		if (order_child.type != LogicalOperatorType::LOGICAL_GET) return;
		auto &get = order_child.Cast<LogicalGet>();
		if (!IsHttpSQLScan(get)) return;
		auto &bd = get.bind_data->Cast<HttpSQLBindData>();
		string order_clause = BuildOrderClause(order_op.orders, *order_op.children[0]);
		if (!order_clause.empty()) bd.order_clause = order_clause;
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE)
			bd.limit_clause = " LIMIT " + to_string(limit.limit_val.GetConstantValue());
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE)
			bd.limit_clause += " OFFSET " + to_string(limit.offset_val.GetConstantValue());
	}
}

static void HandleTopN(LogicalTopN &top_n) {
	D_ASSERT(!top_n.children.empty());
	LogicalOperator &child = SkipProjections(*top_n.children[0]);
	if (child.type != LogicalOperatorType::LOGICAL_GET) return;
	auto &get = child.Cast<LogicalGet>();
	if (!IsHttpSQLScan(get)) return;
	auto &bd = get.bind_data->Cast<HttpSQLBindData>();
	string order_clause = BuildOrderClause(top_n.orders, *top_n.children[0]);
	if (!order_clause.empty()) bd.order_clause = order_clause;
	if (top_n.limit != NumericLimits<idx_t>::Maximum())
		bd.limit_clause = " LIMIT " + to_string(top_n.limit);
	if (top_n.offset > 0)
		bd.limit_clause += " OFFSET " + to_string(top_n.offset);
}

static void RemapExpressionBindings(Expression &expr, const column_binding_map_t<ColumnBinding> &remap) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		auto it = remap.find(col_ref.binding);
		if (it != remap.end()) col_ref.binding = it->second;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		RemapExpressionBindings(child, remap);
	});
}

static void RemapOperatorBindings(LogicalOperator &op, const column_binding_map_t<ColumnBinding> &remap) {
	for (auto &expr : op.expressions) RemapExpressionBindings(*expr, remap);
}

// ─── Aggregate pushdown ──────────────────────────────────────────────────────

enum class AggKind { COUNT_STAR, COUNT_COL, SUM, MIN, MAX };

struct AggInfo {
	AggKind kind;
	string mysql_expr;
	LogicalType orig_type;
};

static bool ClassifyAggregate(const BoundAggregateExpression &agg_expr, const LogicalGet &get, AggInfo &out) {
	if (agg_expr.IsDistinct() || agg_expr.filter || agg_expr.order_bys) return false;
	const auto &fname = agg_expr.function.name;
	out.orig_type = agg_expr.return_type;

	if (fname == "count_star") {
		out.kind = AggKind::COUNT_STAR;
		out.mysql_expr = "COUNT(*)";
		return true;
	}
	if (agg_expr.children.size() != 1) return false;
	auto &child = *agg_expr.children[0];
	if (child.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) return false;
	auto &col_ref = child.Cast<BoundColumnRefExpression>();
	if (col_ref.binding.table_index != get.table_index) return false;
	auto &col_ids = get.GetColumnIds();
	if (col_ref.binding.column_index >= col_ids.size()) return false;
	auto &ci = col_ids[col_ref.binding.column_index];
	if (ci.IsRowIdColumn() || ci.GetPrimaryIndex() >= get.names.size()) return false;
	string col_name = WriteIdentifier(get.names[ci.GetPrimaryIndex()]);

	if (fname == "count") { out.kind = AggKind::COUNT_COL; out.mysql_expr = "COUNT(" + col_name + ")"; return true; }
	if (fname == "sum")   { out.kind = AggKind::SUM;       out.mysql_expr = "SUM(" + col_name + ")"; return true; }
	if (fname == "min")   { out.kind = AggKind::MIN;       out.mysql_expr = "MIN(" + col_name + ")"; return true; }
	if (fname == "max")   { out.kind = AggKind::MAX;       out.mysql_expr = "MAX(" + col_name + ")"; return true; }
	return false;
}

static AggregateFunction LookupAggFunction(ClientContext &context, const string &name,
                                            const vector<LogicalType> &arg_types) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, name);
	return entry.functions.GetFunctionByArguments(context, arg_types);
}

static column_binding_map_t<ColumnBinding> TryHandleAggregate(OptimizerExtensionInput &input,
                                                               unique_ptr<LogicalOperator> &op) {
	column_binding_map_t<ColumnBinding> remap;
	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) return remap;
	auto &agg = op->Cast<LogicalAggregate>();
	if (agg.grouping_sets.size() > 1 || !agg.grouping_functions.empty()) return remap;
	if (agg.expressions.empty()) return remap;

	LogicalOperator &child = SkipProjections(*agg.children[0]);
	if (child.type != LogicalOperatorType::LOGICAL_GET) return remap;
	auto &get = child.Cast<LogicalGet>();
	if (!IsHttpSQLScan(get)) return remap;
	auto &bind_data = get.bind_data->Cast<HttpSQLBindData>();
	if (bind_data.agg_pushdown) return remap;

	struct GroupInfo { string mysql_expr; LogicalType type; };
	vector<GroupInfo> group_infos;
	auto &col_ids = get.GetColumnIds();
	for (auto &grp_expr : agg.groups) {
		if (grp_expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) return remap;
		auto &col_ref = grp_expr->Cast<BoundColumnRefExpression>();
		if (col_ref.binding.table_index != get.table_index) return remap;
		if (col_ref.binding.column_index >= col_ids.size()) return remap;
		auto &ci = col_ids[col_ref.binding.column_index];
		if (ci.IsRowIdColumn() || ci.GetPrimaryIndex() >= get.names.size()) return remap;
		string col_name = WriteIdentifier(get.names[ci.GetPrimaryIndex()]);
		group_infos.push_back({col_name, col_ref.return_type});
	}

	vector<AggInfo> agg_infos;
	for (auto &expr : agg.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) return remap;
		AggInfo info;
		if (!ClassifyAggregate(expr->Cast<BoundAggregateExpression>(), get, info)) return remap;
		agg_infos.push_back(std::move(info));
	}

	auto pushdown = make_uniq<AggPushdown>();
	idx_t num_groups = group_infos.size();
	idx_t num_aggs = agg_infos.size();
	idx_t total_cols = num_groups + num_aggs;

	vector<LogicalType> virtual_types;
	vector<string> virtual_names;

	for (idx_t i = 0; i < num_groups; i++) {
		virtual_types.push_back(group_infos[i].type);
		virtual_names.push_back("_g" + to_string(i));
		pushdown->output_cols.push_back({group_infos[i].mysql_expr});
		pushdown->group_col_names.push_back(group_infos[i].mysql_expr);
	}

	bool has_count_rewrite = false;
	for (idx_t i = 0; i < num_aggs; i++) {
		auto &info = agg_infos[i];
		LogicalType vtype;
		switch (info.kind) {
		case AggKind::MIN: case AggKind::MAX: case AggKind::SUM:
			vtype = info.orig_type; break;
		case AggKind::COUNT_STAR: case AggKind::COUNT_COL:
			vtype = LogicalType::BIGINT; has_count_rewrite = true; break;
		}
		virtual_types.push_back(vtype);
		virtual_names.push_back("_a" + to_string(i));
		pushdown->output_cols.push_back({info.mysql_expr});
	}

	vector<ColumnIndex> new_col_ids;
	for (idx_t i = 0; i < total_cols; i++) new_col_ids.push_back(ColumnIndex(i));
	get.SetColumnIds(std::move(new_col_ids));
	get.returned_types = virtual_types;
	get.names = virtual_names;
	get.projection_ids.clear();
	bind_data.agg_pushdown = std::move(pushdown);

	for (idx_t i = 0; i < num_groups; i++) {
		agg.groups[i] = make_uniq<BoundColumnRefExpression>(
		    group_infos[i].type, ColumnBinding(get.table_index, i));
	}

	FunctionBinder function_binder(input.context);
	for (idx_t i = 0; i < num_aggs; i++) {
		idx_t vcol = num_groups + i;
		auto &info = agg_infos[i];
		auto &expr = agg.expressions[i];

		switch (info.kind) {
		case AggKind::MIN: case AggKind::MAX: {
			auto &agg_expr = expr->Cast<BoundAggregateExpression>();
			agg_expr.children.clear();
			agg_expr.children.push_back(make_uniq<BoundColumnRefExpression>(
			    info.orig_type, ColumnBinding(get.table_index, vcol)));
			break;
		}
		case AggKind::SUM: {
			auto &vtype = virtual_types[vcol];
			auto sum_func = LookupAggFunction(input.context, "sum", {vtype});
			vector<unique_ptr<Expression>> children;
			children.push_back(make_uniq<BoundColumnRefExpression>(vtype, ColumnBinding(get.table_index, vcol)));
			expr = function_binder.BindAggregateFunction(sum_func, std::move(children), nullptr,
			                                             AggregateType::NON_DISTINCT);
			break;
		}
		case AggKind::COUNT_STAR: case AggKind::COUNT_COL: {
			auto sum_func = LookupAggFunction(input.context, "sum", {LogicalType::BIGINT});
			vector<unique_ptr<Expression>> children;
			children.push_back(make_uniq<BoundColumnRefExpression>(
			    LogicalType::BIGINT, ColumnBinding(get.table_index, vcol)));
			expr = function_binder.BindAggregateFunction(sum_func, std::move(children), nullptr,
			                                             AggregateType::NON_DISTINCT);
			break;
		}
		}
	}

	if (has_count_rewrite) {
		auto proj_index = input.optimizer.binder.GenerateTableIndex();
		vector<unique_ptr<Expression>> proj_exprs;
		for (idx_t i = 0; i < num_groups; i++) {
			proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(
			    group_infos[i].type, ColumnBinding(agg.group_index, i)));
		}
		for (idx_t i = 0; i < num_aggs; i++) {
			auto binding = ColumnBinding(agg.aggregate_index, i);
			auto new_type = agg.expressions[i]->return_type;
			auto &orig_type = agg_infos[i].orig_type;
			auto ref = make_uniq<BoundColumnRefExpression>(new_type, binding);
			if (new_type != orig_type)
				proj_exprs.push_back(BoundCastExpression::AddCastToType(input.context, std::move(ref), orig_type));
			else
				proj_exprs.push_back(std::move(ref));
		}
		for (idx_t i = 0; i < num_groups; i++)
			remap[ColumnBinding(agg.group_index, i)] = ColumnBinding(proj_index, i);
		for (idx_t i = 0; i < num_aggs; i++)
			remap[ColumnBinding(agg.aggregate_index, i)] = ColumnBinding(proj_index, num_groups + i);

		auto proj = make_uniq<LogicalProjection>(proj_index, std::move(proj_exprs));
		if (op->has_estimated_cardinality) proj->SetEstimatedCardinality(op->estimated_cardinality);
		proj->children.push_back(std::move(op));
		op = std::move(proj);
	}
	return remap;
}

// ─── Main optimizer entry ────────────────────────────────────────────────────

static column_binding_map_t<ColumnBinding> OptimizePlan(OptimizerExtensionInput &input,
                                                        unique_ptr<LogicalOperator> &op) {
	column_binding_map_t<ColumnBinding> all_remaps;
	for (auto &child : op->children) {
		auto child_remaps = OptimizePlan(input, child);
		all_remaps.insert(child_remaps.begin(), child_remaps.end());
	}
	if (!all_remaps.empty()) RemapOperatorBindings(*op, all_remaps);

	auto agg_remap = TryHandleAggregate(input, op);
	all_remaps.insert(agg_remap.begin(), agg_remap.end());

	if (op->type == LogicalOperatorType::LOGICAL_LIMIT)
		HandleLimit(op->Cast<LogicalLimit>());
	else if (op->type == LogicalOperatorType::LOGICAL_TOP_N)
		HandleTopN(op->Cast<LogicalTopN>());

	return all_remaps;
}

void HttpSQLOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto remaps = OptimizePlan(input, plan);
	if (!remaps.empty()) RemapOperatorBindings(*plan, remaps);
}

} // namespace duckdb
