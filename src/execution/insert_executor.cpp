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
#include <optional>
#include <utility>

#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  auto table_oid = plan_->TableOid();

  try {
    bool getlock = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                          LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid);
    if (!getlock) {
      throw ExecutionException("InsertExecutor try to get IX lock failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("Insert table Transaction Abort");
  }

  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
  is_end_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int inserted_count = 0;
  while (child_executor_->Next(tuple, rid)) {
    TupleMeta tuplemeta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    auto inserted_tuple_rid = table_info_->table_->InsertTuple(tuplemeta, *tuple, exec_ctx_->GetLockManager(),
                                                               exec_ctx_->GetTransaction(), plan_->TableOid());
    if (inserted_tuple_rid.has_value()) {
      // insert data successfully
      *rid = inserted_tuple_rid.value();
      inserted_count++;
      auto tbl_write_record = TableWriteRecord(table_info_->oid_, *rid, table_info_->table_.get());
      tbl_write_record.wtype_ = WType::INSERT;
      exec_ctx_->GetTransaction()->AppendTableWriteRecord(tbl_write_record);

      for (auto indexes : table_indexes_) {
        auto key_attr = indexes->index_->GetKeyAttrs();
        auto index_tuple = tuple->KeyFromTuple(table_info_->schema_, *(indexes->index_->GetKeySchema()), key_attr);
        indexes->index_->InsertEntry(index_tuple, *rid, exec_ctx_->GetTransaction());

        auto idx_write_record = IndexWriteRecord(*rid, table_info_->oid_, WType::INSERT, index_tuple,
                                                 indexes->index_oid_, exec_ctx_->GetCatalog());
        exec_ctx_->GetTransaction()->AppendIndexWriteRecord(idx_write_record);
      }
    }
  }
  std::vector<Value> values;
  values.emplace_back(INTEGER, inserted_count);
  *tuple = Tuple(values, &GetOutputSchema());

  is_end_ = true;
  return true;
}

}  // namespace bustub
