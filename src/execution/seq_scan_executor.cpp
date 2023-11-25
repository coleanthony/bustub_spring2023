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
#include <cmath>
#include <memory>
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_name_);
  auto txn=exec_ctx_->GetTransaction();
  if (exec_ctx_->IsDelete()) {
    //delete mode, IX lock
    try{
      bool lock_success=exec_ctx_->GetLockManager()->LockTable(txn,LockManager::LockMode::INTENTION_EXCLUSIVE, table_info->oid_);
      if (!lock_success) {
        throw ExecutionException("SeqScanExecutor try to get IX lock failed in delete mode");
      }
    }catch (TransactionAbortException e){
      throw ExecutionException("SeqScan table Transaction Abort in delete mode");
    }
  }else{
    //else, IS mode
    auto isolation_level=exec_ctx_->GetTransaction()->GetIsolationLevel();
    if (isolation_level!=IsolationLevel::READ_UNCOMMITTED) {
      if (exec_ctx_->GetTransaction()->GetExclusiveTableLockSet()->count(table_info->oid_)==0&&exec_ctx_->GetTransaction()->GetIntentionExclusiveTableLockSet()->count(table_info->oid_)==0) {
        try{
          bool lock_success=exec_ctx_->GetLockManager()->LockTable(txn,LockManager::LockMode::INTENTION_SHARED, table_info->oid_);
          if (!lock_success) {
            throw ExecutionException("SeqScanExecutor try to get IS lock failed");
          }
        }catch (TransactionAbortException e){
          throw ExecutionException("SeqScan table Transaction Abort");
        }
      }
    }
  }
  table_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (table_iter_->IsEnd()) {
      //is the end
      if (!exec_ctx_->IsDelete()&&exec_ctx_->GetTransaction()->GetIsolationLevel()==IsolationLevel::READ_COMMITTED) {
        //unlock all slock
        if (exec_ctx_->GetTransaction()->GetIntentionExclusiveTableLockSet()->count(plan_->GetTableOid())==0) {
          try{
            bool unlock_success=exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->GetTableOid());
            if (!unlock_success) {
              throw ExecutionException("SeqScanExecutor try to unlock failed");
            }
          }catch(TransactionAbortException e){
            throw ExecutionException("Unlock SeqScan table Transaction Abort");
          }
        }
      }
      return false;
    }

    //judge is delete or not
    *tuple = table_iter_->GetTuple().second;
    if (exec_ctx_->IsDelete()) {
      try{
        bool lock_success=exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), tuple->GetRid());
        if (!lock_success) {
          throw ExecutionException("SeqScanExecutor lockrow try to get X lock failed in delete mode");
        }
      }catch (TransactionAbortException e){
        throw ExecutionException("SeqScan row Transaction Abort in delete mode");
      }
    }else{
      //get slock
      if (exec_ctx_->GetTransaction()->GetIsolationLevel()!=IsolationLevel::READ_UNCOMMITTED) {
        if (exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->count(plan_->GetTableOid())==0||exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->at(plan_->GetTableOid()).count(tuple->GetRid())==0) {
          try{
            bool lock_success=exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, plan_->GetTableOid(), tuple->GetRid());
            if (!lock_success) {
              throw ExecutionException("SeqScanExecutor lockrow try to get S lock failed");
            }
          }catch (TransactionAbortException e){
            throw ExecutionException("SeqScan row Transaction Abort");
          }
        }
      }
    }

    //tuple is deleted, continue
    if (table_iter_->GetTuple().first.is_deleted_ ||
        (plan_->filter_predicate_ != nullptr &&
         plan_->filter_predicate_->Evaluate(tuple, exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
                 .CompareEquals(ValueFactory::GetBooleanValue(false)) == CmpBool::CmpTrue)) {
      if (exec_ctx_->IsDelete() ||
          exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if (exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->count(plan_->GetTableOid()) == 0 ||
            exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->at(plan_->GetTableOid()).count(tuple->GetRid()) == 0) {
          try{
            bool unlock_success=exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), tuple->GetRid(),true);
            if (!unlock_success) {
              throw ExecutionException("SeqScanExecutor try to unlock row failed");
            }
          }catch(TransactionAbortException e){
              throw ExecutionException("Unlock SeqScan row Transaction Abort");
          }
        }
      }
    }

    if (!exec_ctx_->IsDelete()&&exec_ctx_->GetTransaction()->GetIsolationLevel()==IsolationLevel::READ_COMMITTED) {
      if (exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->count(plan_->GetTableOid()) == 0 ||
          exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->at(plan_->GetTableOid()).count(tuple->GetRid()) == 0) {
        try{
          bool unlock_success=exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), tuple->GetRid());
          if (!unlock_success) {
            throw ExecutionException("SeqScanExecutor try to unlock Slock failed");
          }
        }catch(TransactionAbortException e){
          throw ExecutionException("Unlock SeqScan Slock Transaction Abort");
        }        
      }
    }

    *tuple = table_iter_->GetTuple().second;
    *rid = table_iter_->GetRID();
    break;
  }

  ++(*table_iter_);
  return true;
}

}  // namespace bustub
