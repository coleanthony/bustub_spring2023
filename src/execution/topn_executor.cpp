#include "execution/executors/topn_executor.h"
#include <queue>
#include <utility>
#include <vector>
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  // use priority_queue to store;
  Tuple tuple{};
  RID rid{};
  auto cmp = [&](const Tuple &t1, const Tuple &t2) {
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
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq{cmp};
  while (child_executor_->Next(&tuple, &rid)) {
    pq.push(tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }
  // std::cout<<"pq.size():"<<pq.size()<<std::endl;
  while (!pq.empty()) {
    tupleres_.push_front(pq.top());
    pq.pop();
  }
  // std::cout<<"tupleres_.size():"<<tupleres_.size()<<std::endl;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tupleres_.empty()) {
    return false;
  }
  *tuple = tupleres_.front();
  *rid = tuple->GetRid();
  tupleres_.pop_front();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  // std::cout<<"tupleres_.size():"<<tupleres_.size()<<std::endl;
  return tupleres_.size();
};

}  // namespace bustub
