#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }
  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
  const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get());
  if (expr != nullptr) {
    if (expr->comp_type_ != ComparisonType::Equal) {
      return optimized_plan;
    }
    const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
    const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
    if (left_expr == nullptr || right_expr == nullptr) {
      return optimized_plan;
    }
    auto left_expr_tuple_0 = std::make_shared<ColumnValueExpression>(left_expr->GetTupleIdx(), left_expr->GetColIdx(),
                                                                     left_expr->GetReturnType());
    auto right_expr_tuple_0 = std::make_shared<ColumnValueExpression>(
        right_expr->GetTupleIdx(), right_expr->GetColIdx(), right_expr->GetReturnType());
    // Tuple index 0 = left side of join, tuple index 1 = right side of join
    if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
      return std::make_shared<HashJoinPlanNode>(
          nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
          std::vector<AbstractExpressionRef>{left_expr_tuple_0}, std::vector<AbstractExpressionRef>{right_expr_tuple_0},
          nlj_plan.GetJoinType());
    }
    if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
      return std::make_shared<HashJoinPlanNode>(
          nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
          std::vector<AbstractExpressionRef>{right_expr_tuple_0}, std::vector<AbstractExpressionRef>{left_expr_tuple_0},
          nlj_plan.GetJoinType());
    }
    return optimized_plan;
  }
  const auto *logic_expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get());
  if (logic_expr != nullptr) {
    if (logic_expr->logic_type_ != LogicType::And) {
      return optimized_plan;
    }
    const auto *left_expr = dynamic_cast<const ComparisonExpression *>(logic_expr->children_[0].get());
    const auto *right_expr = dynamic_cast<const ComparisonExpression *>(logic_expr->children_[1].get());
    if (left_expr == nullptr || right_expr == nullptr) {
      return optimized_plan;
    }
    const auto *left_left_expr = dynamic_cast<const ColumnValueExpression *>(left_expr->children_[0].get());
    const auto *left_right_expr = dynamic_cast<const ColumnValueExpression *>(left_expr->children_[1].get());
    const auto *right_left_expr = dynamic_cast<const ColumnValueExpression *>(right_expr->children_[0].get());
    const auto *right_right_expr = dynamic_cast<const ColumnValueExpression *>(right_expr->children_[1].get());
    if (left_left_expr == nullptr || left_right_expr == nullptr || right_left_expr == nullptr ||
        right_right_expr == nullptr) {
      return optimized_plan;
    }
    std::vector<AbstractExpressionRef> left_expr_ref{};
    std::vector<AbstractExpressionRef> right_expr_ref{};
    auto push_value = [&](const ColumnValueExpression *expr) {
      auto expr_tuple =
          std::make_shared<ColumnValueExpression>(expr->GetTupleIdx(), expr->GetColIdx(), expr->GetReturnType());
      if (expr->GetTupleIdx() == 0) {
        left_expr_ref.push_back(expr_tuple);
      } else {
        right_expr_ref.push_back(expr_tuple);
      }
    };
    push_value(left_left_expr);
    push_value(left_right_expr);
    push_value(right_left_expr);
    push_value(right_right_expr);
    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                              left_expr_ref, right_expr_ref, nlj_plan.GetJoinType());
  }
  return optimized_plan;
}

}  // namespace bustub
