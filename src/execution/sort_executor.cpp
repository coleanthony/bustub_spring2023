#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <utility>
#include "binder/bound_order_by.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    tupleres_.push_back(tuple);
  }
  std::sort(tupleres_.begin(), tupleres_.end(), [&](const Tuple &t1, const Tuple &t2) -> bool {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      if (order_by_type == OrderByType::DESC) {
        if (expr->Evaluate(&t1, GetOutputSchema()).CompareLessThan(expr->Evaluate(&t2, GetOutputSchema())) ==
            CmpBool::CmpTrue) {
          return false;
        }
        if (expr->Evaluate(&t1, GetOutputSchema()).CompareGreaterThan(expr->Evaluate(&t2, GetOutputSchema())) ==
            CmpBool::CmpTrue) {
          return true;
        }
      } else {
        if (expr->Evaluate(&t1, GetOutputSchema()).CompareGreaterThan(expr->Evaluate(&t2, GetOutputSchema())) ==
            CmpBool::CmpTrue) {
          return false;
        }
        if (expr->Evaluate(&t1, GetOutputSchema()).CompareLessThan(expr->Evaluate(&t2, GetOutputSchema())) ==
            CmpBool::CmpTrue) {
          return true;
        }
      }
    }
    return false;
  });
  tupleres_iter_ = tupleres_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tupleres_iter_ == tupleres_.end()) {
    return false;
  }
  *tuple = *tupleres_iter_;
  *rid = tuple->GetRid();
  ++tupleres_iter_;
  return true;
}

}  // namespace bustub
