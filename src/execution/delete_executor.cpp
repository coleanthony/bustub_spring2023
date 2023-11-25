//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  auto table_oid = plan_->TableOid();
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int deleted_count = 0;
  while (child_executor_->Next(tuple, rid)) {
    auto tuplemeta = table_info_->table_->GetTupleMeta(*rid);
    tuplemeta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuplemeta, *rid);
    auto tbl_write_record=TableWriteRecord(table_info_->oid_, *rid, table_info_->table_.get());
    tbl_write_record.wtype_=WType::DELETE;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(tbl_write_record);

    // delete index
    deleted_count++;
    for (auto indexes : table_indexes_) {
      auto key_attr = indexes->index_->GetKeyAttrs();
      auto delete_tuple = tuple->KeyFromTuple(table_info_->schema_, *(indexes->index_->GetKeySchema()), key_attr);
      indexes->index_->DeleteEntry(delete_tuple, *rid, exec_ctx_->GetTransaction());

      auto idx_write_record=IndexWriteRecord(*rid, table_info_->oid_, WType::DELETE, delete_tuple,indexes->index_oid_ ,
                   exec_ctx_->GetCatalog());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(idx_write_record);
    }
  }

  std::vector<Value> values{Value(INTEGER, deleted_count)};
  *tuple = Tuple(values, &GetOutputSchema());

  is_end_ = true;
  return true;
}

}  // namespace bustub
