//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);
  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  // TODO(cwh): revert all the changes in write set
  while (!txn->GetWriteSet()->empty()) {
    auto table_write_record = txn->GetWriteSet()->back();
    if (table_write_record.wtype_ == WType::INSERT) {
      auto tuplemeta = table_write_record.table_heap_->GetTupleMeta(table_write_record.rid_);
      tuplemeta.is_deleted_ = true;
      table_write_record.table_heap_->UpdateTupleMeta(tuplemeta, table_write_record.rid_);
    } else if (table_write_record.wtype_ == WType::DELETE) {
      auto tuplemeta = table_write_record.table_heap_->GetTupleMeta(table_write_record.rid_);
      tuplemeta.is_deleted_ = false;
      table_write_record.table_heap_->UpdateTupleMeta(tuplemeta, table_write_record.rid_);
    } else {
      // no need in update
    }

    txn->GetWriteSet()->pop_back();
  }
  while (!txn->GetIndexWriteSet()->empty()) {
    auto index_write_record = txn->GetIndexWriteSet()->back();
    if (index_write_record.wtype_ == WType::INSERT) {
      index_write_record.catalog_->GetIndex(index_write_record.index_oid_)
          ->index_->DeleteEntry(index_write_record.tuple_, index_write_record.rid_, txn);
    } else if (index_write_record.wtype_ == WType::DELETE) {
      index_write_record.catalog_->GetIndex(index_write_record.index_oid_)
          ->index_->InsertEntry(index_write_record.tuple_, index_write_record.rid_, txn);
    } else {
      // no need in update
    }

    txn->GetIndexWriteSet()->pop_back();
  }

  ReleaseLocks(txn);
  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
