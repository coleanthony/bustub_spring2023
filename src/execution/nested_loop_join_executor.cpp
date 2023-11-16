//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <utility>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "execution/expressions/arithmetic_expression.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_executor_(std::move(left_executor)),
    right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID left_rid;
  while (right_id_>=0||left_executor_->Next(&left_tuple_, &left_rid)) {
    if (right_id_<0) {
      right_executor_->Init();
    }
    std::vector<Value> values{};
    for (uint32_t right_cur_id=(right_id_<0?0:right_id_); right_cur_id<right_tuples_.size();right_cur_id++) {
      auto right_tuple=right_tuples_.at(right_cur_id);
      auto eval=plan_->predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple, right_executor_->GetOutputSchema());
      if (!eval.IsNull()&&eval.GetAs<bool>()) {
        for (uint32_t index=0; index<left_executor_->GetOutputSchema().GetColumnCount();index++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), index));
        }
        for (uint32_t index=0; index<right_executor_->GetOutputSchema().GetColumnCount();index++) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), index));
        }
        *tuple=Tuple(values,&GetOutputSchema());
        right_id_=static_cast<int32_t>(right_cur_id)+1;
        return true;
      }
    }
    if (right_id_==-1&&plan_->join_type_==JoinType::LEFT) {
      //right is empty, but the join_type is leftjoin
      for (uint32_t index=0; index<left_executor_->GetOutputSchema().GetColumnCount();index++) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), index));
      }
      //the right is empty
      for (uint32_t index=0; index<right_executor_->GetOutputSchema().GetColumnCount();index++) {
       values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(index).GetType())); 
      }
      *tuple=Tuple(values,&GetOutputSchema());
      //right_executor_->Init();
      return true;
    }
    //right_executor_->Init();
    right_id_=-1;
  }
  return false;
}

}  // namespace bustub
