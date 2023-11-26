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
#include <mutex>  //NOLINT
#include <unordered_set>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 1.we need to check isolation level
  CheckTransactionLevel(txn, lock_mode);

  // 2.get the LockRequestQueue
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // std::cout<<"judge update"<<std::endl;
  // 3.judge if it is a lock_upgrade operation
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (lock_request->lock_mode_ == lock_mode) {
        // If requested lock mode is the same as that of the lock presently held, Lock() should return true since it
        // already has the lock.
        lock_request_queue->latch_.unlock();
        return true;
      }
      // lock mode is different, Lock() should upgrade the lock held by the transaction
      // Multiple concurrent lock upgrades on the same resource should set the TransactionState as ABORTED and throw a
      // TransactionAbortException (UPGRADE_CONFLICT).
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        lock_request_queue->latch_.unlock();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      if (!CheckUpgradeLockLevel(lock_request->lock_mode_, lock_mode)) {
        // upgrade is considered incompatible
        txn->SetState(TransactionState::ABORTED);
        lock_request_queue->latch_.unlock();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // upgrade the lock
      lock_request_queue->request_queue_.remove(lock_request);
      ModifyTableLocks(txn, lock_request, false);

      auto update_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      auto iter = lock_request_queue->request_queue_.begin();
      while (iter != lock_request_queue->request_queue_.end()) {
        if (!(*iter)->granted_) {
          break;
        }
        iter++;
      }
      lock_request_queue->request_queue_.insert(iter, update_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(update_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->request_queue_.remove(update_request);
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      // can get the lock
      update_request->granted_ = true;
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      ModifyTableLocks(txn, update_request, true);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  // std::cout<<"lock insert"<<std::endl;
  // 4.not a lock_upgrade operation, put the lock into the new LockRequestQueue
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  // lock_request_queue->request_queue_.push_back(lock_request.get());
  lock_request_queue->request_queue_.push_back(lock_request);

  // 5.try to get the lock
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      // lock_request_queue->request_queue_.remove(lock_request.get());
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  // can get the lock
  lock_request->granted_ = true;
  ModifyTableLocks(txn, lock_request, true);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    // no lock
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto txn_srow_set = txn->GetSharedRowLockSet();
  auto txn_xrow_set = txn->GetExclusiveRowLockSet();
  if (!((txn_srow_set->find(oid) == txn->GetSharedRowLockSet()->end() || txn_srow_set->at(oid).empty()) &&
        (txn_xrow_set->find(oid) == txn->GetExclusiveRowLockSet()->end() || txn_xrow_set->at(oid).empty()))) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    // std::cout<<"try to remove lock_request"<<std::endl;
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);

      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      // TRANSACTION STATE UPDATE
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::READ_UNCOMMITTED:
          if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          if (lock_request->lock_mode_ == LockMode::SHARED) {
            txn->SetState(TransactionState::ABORTED);
            throw new Exception("add shared lock before the read is committed");
          }
          break;
        case IsolationLevel::REPEATABLE_READ:
          if (lock_request->lock_mode_ == LockMode::EXCLUSIVE || lock_request->lock_mode_ == LockMode::SHARED) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::READ_COMMITTED:
          if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
      }

      ModifyTableLocks(txn, lock_request, false);
      return true;
    }
  }

  // else no lock held
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // just like locktable
  // 1.check
  CheckLockRowLockMode(txn, lock_mode);
  CheckLockRowTableIntension(txn, lock_mode, oid);
  CheckTransactionLevel(txn, lock_mode);
  // std::cout<<"checkok"<<std::endl;

  // 2.get the LockRequestQueue
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }

  auto lock_request_queue = row_lock_map_.find(rid)->second;
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  // 3.judge if it is a lock_upgrade operation
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (lock_request->lock_mode_ == lock_mode) {
        // If requested lock mode is the same as that of the lock presently held, Lock() should return true since it
        // already has the lock.
        lock_request_queue->latch_.unlock();
        return true;
      }
      // lock mode is different, Lock() should upgrade the lock held by the transaction
      // Multiple concurrent lock upgrades on the same resource should set the TransactionState as ABORTED and throw a
      // TransactionAbortException (UPGRADE_CONFLICT).
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        lock_request_queue->latch_.unlock();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      if (!CheckUpgradeLockLevel(lock_request->lock_mode_, lock_mode)) {
        // upgrade is considered incompatible
        txn->SetState(TransactionState::ABORTED);
        lock_request_queue->latch_.unlock();
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // upgrade the lock
      lock_request_queue->request_queue_.remove(lock_request);
      ModifyRowLocks(txn, lock_request, false);

      auto update_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      auto iter = lock_request_queue->request_queue_.begin();
      while (iter != lock_request_queue->request_queue_.end()) {
        if (!(*iter)->granted_) {
          break;
        }
        iter++;
      }
      lock_request_queue->request_queue_.insert(iter, update_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(update_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          // lock_request_queue->request_queue_.remove(update_request.get());
          lock_request_queue->request_queue_.remove(update_request);
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      // can get the lock
      update_request->granted_ = true;
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      ModifyRowLocks(txn, update_request, true);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  // 4.not a lock_upgrade operation, put the lock into the new LockRequestQueue
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);

  // 5.try to get the lock
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      // lock_request_queue->request_queue_.remove(lock_request.get());
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  // can get the lock
  lock_request->granted_ = true;
  ModifyRowLocks(txn, lock_request, true);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    // no lock
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      // TRANSACTION STATE UPDATE
      if (!force) {
        switch (txn->GetIsolationLevel()) {
          case IsolationLevel::READ_UNCOMMITTED:
            if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
              txn->SetState(TransactionState::SHRINKING);
            }
            if (lock_request->lock_mode_ == LockMode::SHARED) {
              txn->SetState(TransactionState::ABORTED);
              throw new Exception("add shared lock before the read is committed");
            }
            break;
          case IsolationLevel::REPEATABLE_READ:
            if (lock_request->lock_mode_ == LockMode::EXCLUSIVE || lock_request->lock_mode_ == LockMode::SHARED) {
              txn->SetState(TransactionState::SHRINKING);
            }
            break;
          case IsolationLevel::READ_COMMITTED:
            if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
              txn->SetState(TransactionState::SHRINKING);
            }
            break;
        }
      }

      ModifyRowLocks(txn, lock_request, false);
      return true;
    }
  }

  // else no lock held
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_.emplace(t1, std::unordered_set<txn_id_t>{});
  }
  waits_for_[t1].insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // store the transaction id of the youngest transaction in the cycle in txn_id and return true.
  // else return false
  // use topological sort
  std::unordered_map<txn_id_t, int> toporeder;
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> for_wait;
  for (auto num : transaction_set_) {
    toporeder.emplace(num, 0);
    for_wait.emplace(num, std::vector<txn_id_t>{});
  }
  for (auto [txn_id, wait_set] : waits_for_) {
    toporeder[txn_id] += wait_set.size();
    for (auto wait_for_id : wait_set) {
      for_wait[wait_for_id].push_back(txn_id);
    }
  }
  std::queue<txn_id_t> q;
  for (auto [t_id, wait_for_num] : toporeder) {
    if (wait_for_num == 0) {
      q.push(t_id);
    }
  }
  while (!q.empty()) {
    auto txn = q.front();
    q.pop();
    for (int nxt_wait : for_wait[txn]) {
      toporeder[nxt_wait]--;
      if (toporeder[nxt_wait] == 0) {
        q.push(nxt_wait);
      }
    }
  }

  std::set<txn_id_t> txnset;
  for (auto &iter : toporeder) {
    if (iter.second != 0) {
      txnset.insert(iter.first);
    }
  }
  if (txnset.empty()) {
    return false;
  }

  // has circle,use dfs to get the cycle_id
  std::unordered_set<txn_id_t> visted;
  std::set<txn_id_t> txncurstk;

  for (auto cur_txn : txnset) {
    if (visted.count(cur_txn) == 0 && DFSCycle(cur_txn, visted, txnset, txncurstk)) {
      *txn_id = cycle_id_;
      return true;
    }
  }
  return false;
}

auto LockManager::DFSCycle(txn_id_t cur_txn, std::unordered_set<txn_id_t> &visted, std::set<txn_id_t> &txnset,
                           std::set<txn_id_t> &txncurstk) -> bool {
  visted.insert(cur_txn);
  txncurstk.insert(cur_txn);
  for (txn_id_t nextid : waits_for_[cur_txn]) {
    if (txnset.count(nextid) != 0) {
      if (visted.count(nextid) == 0) {
        if (DFSCycle(nextid, visted, txnset, txncurstk)) {
          return true;
        }
      } else if (txncurstk.count(nextid) != 0) {
        cycle_id_ = *txncurstk.rbegin();
        // std::cout<<"cycleid"<<cycle_id_<<std::endl;
        return true;
      }
    }
  }
  txncurstk.erase(cur_txn);
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto [txn_id, wait_set] : waits_for_) {
    for (auto wait_for_id : wait_set) {
      edges.emplace_back(txn_id, wait_for_id);
    }
  }
  return edges;
}

void LockManager::DeleteTransactionLocks(txn_id_t txn_id) {
  // update set
  waits_for_.erase(txn_id);
  transaction_set_.erase(txn_id);
  for (auto cur_txnid : transaction_set_) {
    if (waits_for_[cur_txnid].find(txn_id) != waits_for_[cur_txnid].end()) {
      RemoveEdge(cur_txnid, txn_id);
    }
  }

  table_lock_map_latch_.lock();
  for (auto table_id : txn_to_oid_[txn_id]) {
    table_lock_map_[table_id]->cv_.notify_all();
  }
  table_lock_map_latch_.unlock();

  row_lock_map_latch_.lock();
  for (auto row_id : txn_to_rid_[txn_id]) {
    row_lock_map_[row_id]->cv_.notify_all();
  }
  row_lock_map_latch_.unlock();
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      // add edge to wait_for_
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();

      for (auto [table_id, lock_queue] : table_lock_map_) {
        lock_queue->latch_.lock();
        std::unordered_set<txn_id_t> granted;
        for (const auto &lock_request : lock_queue->request_queue_) {
          transaction_set_.insert(lock_request->txn_id_);
          txn_to_oid_[lock_request->txn_id_].push_back(table_id);
          if (lock_request->granted_) {
            granted.insert(lock_request->txn_id_);
          } else {
            for (int granted_txnid : granted) {
              AddEdge(lock_request->txn_id_, granted_txnid);
            }
          }
        }
        lock_queue->latch_.unlock();
      }

      for (auto [row_id, lock_queue] : row_lock_map_) {
        lock_queue->latch_.lock();
        std::unordered_set<txn_id_t> granted;
        for (const auto &lock_request : lock_queue->request_queue_) {
          transaction_set_.insert(lock_request->txn_id_);
          txn_to_rid_[lock_request->txn_id_].push_back(row_id);
          if (lock_request->granted_) {
            granted.insert(lock_request->txn_id_);
          } else {
            for (int granted_txnid : granted) {
              AddEdge(lock_request->txn_id_, granted_txnid);
            }
          }
        }
        lock_queue->latch_.unlock();
      }
      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();

      // std::cout<<"add edges successfully"<<std::endl;
      // for (auto num:transaction_set_) {
      //  std::cout<<num<<" ";
      //}
      // std::cout<<std::endl;

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        auto txn = txn_manager_->GetTransaction(txn_id);
        // std::cout<<"abort txn: "<<txn_id<<std::endl;
        txn->SetState(TransactionState::ABORTED);
        DeleteTransactionLocks(txn_id);
      }

      waits_for_.clear();
      transaction_set_.clear();
      txn_to_oid_.clear();
      txn_to_rid_.clear();
    }
  }
}

void LockManager::CheckTransactionLevel(Transaction *txn, LockMode lock_mode) {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn->GetState() != TransactionState::GROWING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    default:
      (void)txn;
  }
}

auto LockManager::CheckUpgradeLockLevel(LockMode transaction_lock_mode, LockMode lock_mode) -> bool {
  switch (transaction_lock_mode) {
    case LockMode::SHARED:
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::EXCLUSIVE:
      break;
    case LockMode::INTENTION_SHARED:
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
          lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (lock_mode == LockMode::EXCLUSIVE) {
        return true;
      }
      break;
    default:
      break;
  }
  return false;
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->granted_) {
      // Whether the lock has been granted or not
      switch (lock_request->lock_mode_) {
        case LockMode::SHARED:
          if (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::INTENTION_SHARED) {
            break;
          }
          return false;
        case LockMode::EXCLUSIVE:
          return false;
        case LockMode::INTENTION_SHARED:
          if (request->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (request->lock_mode_ == LockMode::INTENTION_SHARED ||
              request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
            break;
          }
          return false;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (request->lock_mode_ == LockMode::INTENTION_SHARED) {
            break;
          }
          return false;
      }
    } else {
      return request.get() == lock_request.get();
    }
  }
  // impossible to get here
  throw new Exception("Grant lock error");
}

void LockManager::ModifyTableLocks(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                   bool is_insert_mode) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (is_insert_mode) {
        // insert
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        // delete
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (is_insert_mode) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (is_insert_mode) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (is_insert_mode) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (is_insert_mode) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}

void LockManager::ModifyRowLocks(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                 bool is_insert_mode) {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
    case LockMode::EXCLUSIVE:
      if (is_insert_mode) {
        InsertRowlocks(txn, lock_request);
      } else {
        EraseRowlocks(txn, lock_request);
      }
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

void LockManager::InsertRowlocks(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  if (lock_request->lock_mode_ == LockMode::SHARED) {
    if (txn->GetSharedRowLockSet()->find(lock_request->oid_) == txn->GetSharedRowLockSet()->end()) {
      txn->GetSharedRowLockSet()->emplace(lock_request->oid_, std::unordered_set<RID>{});
    }
    txn->GetSharedRowLockSet()->find(lock_request->oid_)->second.emplace(lock_request->rid_);
  } else {
    if (txn->GetExclusiveRowLockSet()->find(lock_request->oid_) == txn->GetExclusiveRowLockSet()->end()) {
      txn->GetExclusiveRowLockSet()->emplace(lock_request->oid_, std::unordered_set<RID>{});
    }
    txn->GetExclusiveRowLockSet()->find(lock_request->oid_)->second.emplace(lock_request->rid_);
  }
}

void LockManager::EraseRowlocks(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  if (lock_request->lock_mode_ == LockMode::SHARED) {
    if (txn->GetSharedRowLockSet()->find(lock_request->oid_) == txn->GetSharedRowLockSet()->end()) {
      return;
    }
    txn->GetSharedRowLockSet()->find(lock_request->oid_)->second.erase(lock_request->rid_);
  } else {
    if (txn->GetExclusiveRowLockSet()->find(lock_request->oid_) == txn->GetExclusiveRowLockSet()->end()) {
      txn->GetExclusiveRowLockSet()->emplace(lock_request->oid_, std::unordered_set<RID>{});
    }
    txn->GetExclusiveRowLockSet()->find(lock_request->oid_)->second.erase(lock_request->rid_);
  }
}

void LockManager::CheckLockRowLockMode(Transaction *txn, LockMode lock_mode) {
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
}

void LockManager::CheckLockRowTableIntension(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableSharedLocked(oid) &&
          !txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableExclusiveLocked(oid) &&
          !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
          !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
      break;
    default:
      break;
  }
}

}  // namespace bustub
