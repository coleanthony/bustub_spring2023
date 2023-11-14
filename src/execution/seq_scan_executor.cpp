//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan) {
    table_info_=exec_ctx->GetCatalog()->GetTable(plan->table_name_);
}

void SeqScanExecutor::Init() { 
    auto &table=table_info_->table_;
    table_iter_=new TableIterator(table->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    while (true) {
        if (table_iter_->IsEnd()) {
            delete table_iter_;
            table_iter_=nullptr;
            return false;
        }
        if (table_iter_->GetTuple().first.is_deleted_) {
            ++(*table_iter_);
            continue;
        }
        *tuple=table_iter_->GetTuple().second;
        *rid=table_iter_->GetRID();
        break;
    }
    ++(*table_iter_);
    return true; 
}

}  // namespace bustub
