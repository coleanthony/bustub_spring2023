//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <optional>
#include <utility>

#include "common/config.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  auto table_oid=plan_->TableOid();
  table_info_=exec_ctx_->GetCatalog()->GetTable(table_oid);
  table_indexes_=exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  is_end_=false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int updated_count=0;
  while (child_executor_->Next(tuple, rid)) {
    //first delete the affected tuple and then insert a new tuple
    TupleMeta tuplemeta;
    tuplemeta.delete_txn_id_=INVALID_TXN_ID;
    tuplemeta.insert_txn_id_=INVALID_TXN_ID;
    tuplemeta.is_deleted_=true;
    table_info_->table_->UpdateTupleMeta(tuplemeta, *rid);

    tuplemeta.is_deleted_=false;
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (const auto &expr :plan_->target_expressions_) {
      values.push_back(expr->Evaluate(tuple, table_info_->schema_));
    }

    //insert value
    Tuple inserted_tuple=Tuple(values,&table_info_->schema_);
    auto inserted_tuple_rid=table_info_->table_->InsertTuple(tuplemeta, inserted_tuple);
    if (inserted_tuple_rid==std::nullopt) {
      continue;
    }
    updated_count++;
    *rid=inserted_tuple_rid.value();
    for (auto indexes:table_indexes_) {
      auto key_attr=indexes->index_->GetKeyAttrs();
      auto index_tuple=inserted_tuple.KeyFromTuple(table_info_->schema_, *(indexes->index_->GetKeySchema()), key_attr);
      indexes->index_->InsertEntry(index_tuple, *rid,nullptr);
    }
  }
  
  std::vector<Column> columns{};
  columns.emplace_back("id", TypeId::INTEGER);
  Schema sch{columns};
  std::vector<Value> values;
  values.emplace_back(INTEGER,updated_count);
  *tuple=Tuple(values,&sch);
  is_end_=true;

  return true;
}

}  // namespace bustub
