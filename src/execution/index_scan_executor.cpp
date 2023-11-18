//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include "type/type_id.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)),
      index_iter_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())->GetBeginIterator()) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (index_iter_.IsEnd()) {
      return false;
    }
    auto index_rid = (*index_iter_).second;
    auto index_tp = table_info_->table_->GetTuple(index_rid);
    if (index_tp.first.is_deleted_) {
      ++index_iter_;
      continue;
    }
    *tuple = index_tp.second;
    *rid = index_rid;
    break;
  }
  ++index_iter_;
  return true;
}

}  // namespace bustub
