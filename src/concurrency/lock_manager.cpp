//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <memory>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  //1.we need to check isolation level
  CheckTransactionLevel(txn, lock_mode);

  //2.get the LockRequestQueue
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid)==table_lock_map_.end()) {
    table_lock_map_.emplace(oid,std::make_shared<LockRequestQueue>());
  }
  auto table_lock_map_iter=table_lock_map_.find(oid);
  auto lock_request_queue=table_lock_map_iter->second;
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  //3.judge if it is a lock_upgrade operation
  for (auto lock_request:lock_request_queue->request_queue_) {
    if (lock_request->txn_id_==txn->GetTransactionId()) {
      if (lock_request->lock_mode_==lock_mode) {
        // If requested lock mode is the same as that of the lock presently held, Lock() should return true since it already has the lock.
        lock_request_queue->latch_.unlock();
        return true;
      }
      //lock mode is different, Lock() should upgrade the lock held by the transaction
      //Multiple concurrent lock upgrades on the same resource should set the TransactionState as ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
      if (lock_request_queue->upgrading_!=INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        lock_request_queue->latch_.unlock();
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::UPGRADE_CONFLICT);
      }

      if (!CheckUpgradeLockLevel(lock_request->lock_mode_,lock_mode)) {
        //upgrade is considered incompatible
        txn->SetState(TransactionState::ABORTED);
        lock_request_queue->latch_.unlock();
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::INCOMPATIBLE_UPGRADE);
      }
      //upgrade the lock
      lock_request_queue->request_queue_.remove(lock_request);
      ModifyTableLocks(txn, std::shared_ptr<LockRequest>(lock_request), false);

      auto update_request=std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      auto iter=lock_request_queue->request_queue_.begin();
      while (iter!=lock_request_queue->request_queue_.end()) {
        if (!(*iter)->granted_) {
          break;
        }
        iter++;
      }
      lock_request_queue->request_queue_.emplace(iter, update_request);
      lock_request_queue->upgrading_=txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_,std::adopt_lock);
      while (!GrantLock(update_request,lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState()==TransactionState::ABORTED) {
          lock_request_queue->request_queue_.remove(update_request.get());
          lock_request_queue->upgrading_=INVALID_TXN_ID;
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      //can get the lock
      update_request->granted_=true;
      lock_request_queue->upgrading_=INVALID_TXN_ID;
      ModifyTableLocks(txn,update_request,true);
      if(lock_mode!=LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  //4.not a lock_upgrade operation, put the lock into the new LockRequestQueue
  auto lock_request=std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.emplace_back(lock_request);
  
  //5.try to get the lock
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_,std::adopt_lock);
  while (!GrantLock(lock_request,lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState()==TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request.get());
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  //can get the lock
  lock_request->granted_=true;
  ModifyTableLocks(txn,lock_request,true);
  if(lock_mode!=LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid)==table_lock_map_.end()) {
    //no lock
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  
  auto txn_srow_set=txn->GetSharedRowLockSet()->find(oid);
  auto txn_xrow_set=txn->GetExclusiveRowLockSet()->find(oid);
  if (!((txn_srow_set==txn->GetSharedRowLockSet()->end()||txn_srow_set->second.empty())&&(txn_xrow_set==txn->GetExclusiveRowLockSet()->end()||txn_xrow_set->second.empty()))) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  auto lock_request_queue=table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for(auto lock_request:lock_request_queue->request_queue_){
    if (lock_request->txn_id_==txn->GetTransactionId()&&lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      //TRANSACTION STATE UPDATE
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::READ_UNCOMMITTED:
          if (lock_request->lock_mode_==LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          if (lock_request->lock_mode_==LockMode::SHARED) {
            txn->SetState(TransactionState::ABORTED);
            throw new Exception("add shared lock before the read is committed");
          }
          break;
        case IsolationLevel::REPEATABLE_READ:
          if (lock_request->lock_mode_==LockMode::EXCLUSIVE||lock_request->lock_mode_==LockMode::SHARED) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::READ_COMMITTED:
          if (lock_request->lock_mode_==LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
      }

      ModifyTableLocks(txn, std::shared_ptr<LockRequest>(lock_request), false);
      return true;
    }
  }
  
  //else no lock held
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  CheckLockRowLockMode(txn, lock_mode);
  CheckLockRowTableIntension(txn, lock_mode,oid);
  CheckTransactionLevel(txn, lock_mode);




  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {



  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

void LockManager::CheckTransactionLevel(Transaction *txn, LockMode lock_mode){
  switch (txn->GetIsolationLevel()){
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode==LockMode::SHARED||lock_mode==LockMode::INTENTION_SHARED||lock_mode==LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if(txn->GetState()!=TransactionState::GROWING){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState()==TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState()==TransactionState::SHRINKING) {
        if (lock_mode!=LockMode::SHARED&&lock_mode!=LockMode::INTENTION_SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    default:
      (void)txn;
  }
}

auto LockManager::CheckUpgradeLockLevel(LockMode transaction_lock_mode,LockMode lock_mode)->bool{
  switch (transaction_lock_mode) {
    case LockMode::SHARED:
      if (lock_mode==LockMode::EXCLUSIVE||lock_mode==LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::EXCLUSIVE:
      break;
    case LockMode::INTENTION_SHARED:
      if (lock_mode==LockMode::SHARED||lock_mode==LockMode::EXCLUSIVE||lock_mode==LockMode::INTENTION_EXCLUSIVE||lock_mode==LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (lock_mode==LockMode::EXCLUSIVE||lock_mode==LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (lock_mode==LockMode::EXCLUSIVE) {
        return true;
      }
      break;
    default:
      break;
  }
  return false;
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,const std::shared_ptr<LockRequestQueue> &lock_request_queue)->bool{
  for (const auto &request:lock_request_queue->request_queue_) {
    if (request->granted_) {
      //Whether the lock has been granted or not
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if (request->lock_mode_==LockMode::EXCLUSIVE||request->lock_mode_==LockMode::SHARED_INTENTION_EXCLUSIVE) {
            break;
          }
          return false;
        case LockMode::EXCLUSIVE:
          return false;
        case LockMode::INTENTION_SHARED:
          if (request->lock_mode_==LockMode::SHARED||request->lock_mode_==LockMode::EXCLUSIVE||request->lock_mode_==LockMode::INTENTION_EXCLUSIVE||request->lock_mode_==LockMode::SHARED_INTENTION_EXCLUSIVE) {
            break;
          }
          return false;
        case LockMode::INTENTION_EXCLUSIVE:
          if (request->lock_mode_==LockMode::EXCLUSIVE||request->lock_mode_==LockMode::SHARED_INTENTION_EXCLUSIVE) {
            break;
          }
          return false;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (request->lock_mode_==LockMode::EXCLUSIVE) {
            break;
          }
          return false;
      }
    }else{
      return request==lock_request.get();
    }
    return false;
  }
  // impossible to get here
  throw new Exception("Grant lock error");
}

void LockManager::ModifyTableLocks(Transaction *txn,const std::shared_ptr<LockRequest> &lock_request,bool is_insert_mode){
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (is_insert_mode) {
        //insert
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      }else{
        //delete
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (is_insert_mode) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      }else{
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (is_insert_mode) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      }else{
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (is_insert_mode) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      }else{
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (is_insert_mode) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      }else{
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}

void LockManager::CheckLockRowLockMode(Transaction *txn, LockMode lock_mode){
  if (lock_mode!=LockMode::SHARED&&lock_mode!=LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
}

void LockManager::CheckLockRowTableIntension(Transaction *txn, LockMode lock_mode,const table_oid_t &oid){
  switch (lock_mode) {
    case LockMode::SHARED:

    case LockMode::EXCLUSIVE:

    default:
      break;
  }
}

}  // namespace bustub
