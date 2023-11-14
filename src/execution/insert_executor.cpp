//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan){
        child_executor_=std::move(child_executor);
}

void InsertExecutor::Init() {
    auto table_oid=plan_->TableOid();
    table_info_=exec_ctx_->GetCatalog()->GetTable(table_oid);
    table_indexes_=exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    
    
    return false;
}

}  // namespace bustub
