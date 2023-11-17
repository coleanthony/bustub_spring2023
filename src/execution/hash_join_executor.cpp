//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstdint>
#include <utility>
#include "common/util/hash_util.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_executor_(std::move(left_child)),
    right_executor_(std::move(right_child)){
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  //Hashjoin is a Pipeline Breaker, we need to get all data at the init phase
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple{};
  RID rid{};
  std::unordered_map<hash_t,std::vector<Tuple>> right_join_ht{};
  std::unordered_map<hash_t,std::vector<std::vector<Value>>> right_join_value_ht{};
  while (right_executor_->Next(&tuple, &rid)) {
    std::vector<Value> values;
    hash_t key;
    for (const auto& right_expr:plan_->RightJoinKeyExpressions()) {
      auto val=right_expr->Evaluate(&tuple, right_executor_->GetOutputSchema());
      values.push_back(val);
      key=HashUtil::CombineHashes(key, HashUtil::HashValue(&val));
    }
    right_join_ht[key].emplace_back(tuple);
    right_join_value_ht[key].emplace_back(values);
  }
  while (left_executor_->Next(&tuple, &rid)) {
    hash_t left_key;
    std::vector<Value> left_value;
    for (const auto& left_expr:plan_->LeftJoinKeyExpressions()) {
      auto val=left_expr->Evaluate(&tuple, left_executor_->GetOutputSchema());
      left_value.push_back(val);
      left_key=HashUtil::CombineHashes(left_key, HashUtil::HashValue(&val));
    }
    if (right_join_ht.find(left_key)!=right_join_ht.end()) {
      auto righttuples=right_join_ht[left_key];
      auto rightvalues=right_join_value_ht[left_key];
      //get right tuples and values
      auto rightvaluessize=rightvalues.size();
      for (uint32_t index=0; index<rightvaluessize; index++) {
        auto righttuple=righttuples[index];
        auto rightvalue=rightvalues[index];
        if (rightvalue.size()!=left_value.size()) {
          continue;
        }
        bool judgeequel=true;
        for (uint32_t i=0;i<left_value.size();i++) {
          auto leftval=left_value[i];
          auto rightval=rightvalue[i];
          if (leftval.CompareEquals(rightval)!=CmpBool::CmpTrue) {
            judgeequel=false;
            break;
          }
        }
        if (judgeequel) {
          std::vector<Value> values{};
          for (uint32_t index=0; index<left_executor_->GetOutputSchema().GetColumnCount();index++) {
            values.push_back(tuple.GetValue(&left_executor_->GetOutputSchema(), index));
          }
          for (uint32_t index=0; index<right_executor_->GetOutputSchema().GetColumnCount();index++) {
            values.push_back(righttuple.GetValue(&right_executor_->GetOutputSchema(), index));
          }
          tupleres_.emplace_back(values,&GetOutputSchema());
        }
      }
    }else if (plan_->join_type_==JoinType::LEFT) {
      //right is empty, but the join_type is leftjoin
      std::vector<Value> values{};
      for (uint32_t index=0; index<left_executor_->GetOutputSchema().GetColumnCount();index++) {
        values.push_back(tuple.GetValue(&left_executor_->GetOutputSchema(), index));
      }
      //the right is empty
      for (uint32_t index=0; index<right_executor_->GetOutputSchema().GetColumnCount();index++) {
       values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(index).GetType())); 
      }
      tupleres_.emplace_back(values,&GetOutputSchema());
    }
  }
  std::cout<<tupleres_.size()<<std::endl;
  tupleres_iter_=tupleres_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tupleres_iter_==tupleres_.end()) {
    return false;
  }
  *tuple=*tupleres_iter_;
  ++tupleres_iter_;
  return true; 
}

}  // namespace bustub
