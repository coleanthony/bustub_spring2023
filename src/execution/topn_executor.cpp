#include "execution/executors/topn_executor.h"
#include <queue>
#include <utility>
#include <vector>
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
    child_executor_->Init();
    //use priority_queue to store;
    Tuple tuple{};
    RID rid{};
    auto cmp=[&](const Tuple &t1,const Tuple &t2){
        for (auto [order_by_type,expr]:plan_->GetOrderBy()) {
            if (order_by_type==OrderByType::DESC) {
                if(expr->Evaluate(&t1,GetOutputSchema()).CompareLessThan(expr->Evaluate(&t2, GetOutputSchema()))==CmpBool::CmpTrue){
                    return false;
                }
                if(expr->Evaluate(&t1,GetOutputSchema()).CompareGreaterThan(expr->Evaluate(&t2, GetOutputSchema()))==CmpBool::CmpTrue){
                    return true;
                }
            }else{
                if(expr->Evaluate(&t1,GetOutputSchema()).CompareGreaterThan(expr->Evaluate(&t2, GetOutputSchema()))==CmpBool::CmpTrue){
                    return false;
                }
                if(expr->Evaluate(&t1,GetOutputSchema()).CompareLessThan(expr->Evaluate(&t2, GetOutputSchema()))==CmpBool::CmpTrue){
                    return true;
                }
            }                
        }
        return false;
    };
    std::priority_queue<Tuple,std::vector<Tuple>,decltype(cmp)> pq{cmp};
    auto topn=plan_->GetN();
    while (child_executor_->Next(&tuple, &rid)) {
        pq.emplace(tuple);
        if (pq.size()>topn) {
            pq.pop();
        }
    }
    while (!pq.empty()) {
        tupleres_.push_back(pq.top());
        pq.pop();
    }
    tupleres_iter_=tupleres_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (tupleres_iter_==tupleres_.end()) {
        return false;
    }
    *tuple=*tupleres_iter_;
    *rid=tuple->GetRid();
    ++tupleres_iter_;
    return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    return tupleres_.size();
};

}  // namespace bustub
