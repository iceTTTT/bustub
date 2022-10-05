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

#include <utility>
#include <vector>

#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::GrantS(const RID &rid, txn_id_t txn_id) -> bool {
  for (auto i : lock_table_[rid].request_queue_) {
    if (i.txn_id_ == txn_id) {
      return true;
    }

    if (i.lock_mode_ == LockMode::EXCLUSIVE) {
      return false;
    }
  }
  // Being killed , also jump out wait.
  return true;
}

auto LockManager::GrantX(const RID &rid, txn_id_t txn_id) -> bool {
  return lock_table_[rid].request_queue_.begin()->txn_id_ == txn_id;
}

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock lk(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // Check lock held.
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  // Install request in the back.
  lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  auto thisiter = lock_table_[rid].request_queue_.end();
  --thisiter;
  // Kill all younger W-request.And notify.
  for (auto i = lock_table_[rid].request_queue_.begin(); i != lock_table_[rid].request_queue_.end();) {
    auto trans = TransactionManager::GetTransaction(i->txn_id_);
    if (i->lock_mode_ == LockMode::EXCLUSIVE && i->txn_id_ > txn->GetTransactionId() &&
        trans->GetState() != TransactionState::ABORTED) {
      trans->SetState(TransactionState::ABORTED);
      // Erase. Maybe not exist. Still fine.
      trans->GetExclusiveLockSet()->erase(rid);
      i = lock_table_[rid].request_queue_.erase(i);
      continue;
    }
    ++i;
  }
  lock_table_[rid].cv_.notify_all();
  // Wait for kill,or granted.
  while (txn->GetState() != TransactionState::ABORTED && !GrantS(rid, txn->GetTransactionId())) {
    lock_table_[rid].cv_.wait(lk);
  }
  // Check for kill.
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetSharedLockSet()->emplace(rid);
  thisiter->granted_ = true;
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock lk(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // Check hold the X-lock.
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // Check hold the S-lock.
  if (txn->IsSharedLocked(rid)) {
    // remove share request.
    for (auto i = lock_table_[rid].request_queue_.begin(); i != lock_table_[rid].request_queue_.end(); ++i) {
      if (i->txn_id_ == txn->GetTransactionId()) {
        txn->GetSharedLockSet()->erase(rid);
        lock_table_[rid].request_queue_.erase(i);
        break;
      }
    }
  }
  // Install request in the back.
  lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  auto thisiter = lock_table_[rid].request_queue_.end();
  --thisiter;
  // Kill all younger request. Notify.
  for (auto i = lock_table_[rid].request_queue_.begin(); i != lock_table_[rid].request_queue_.end();) {
    auto trans = TransactionManager::GetTransaction(i->txn_id_);
    if (trans->GetState() != TransactionState::ABORTED && i->txn_id_ > txn->GetTransactionId()) {
      trans->SetState(TransactionState::ABORTED);
      // Erase. Maybe not exsit . Still fine.
      trans->GetSharedLockSet()->erase(rid);
      trans->GetExclusiveLockSet()->erase(rid);
      i = lock_table_[rid].request_queue_.erase(i);
      continue;
    }
    ++i;
  }
  lock_table_[rid].cv_.notify_all();
  // Wait for kill, or granted.
  while (txn->GetState() != TransactionState::ABORTED && !GrantX(rid, txn->GetTransactionId())) {
    lock_table_[rid].cv_.wait(lk);
  }
  // Check for kill.
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  thisiter->granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock lk(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID || txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (!txn->IsSharedLocked(rid)) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // Find the S-request.remove it.
  for (auto i = lock_table_[rid].request_queue_.begin(); i != lock_table_[rid].request_queue_.end(); ++i) {
    if (i->txn_id_ == txn->GetTransactionId()) {
      txn->GetSharedLockSet()->erase(rid);
      lock_table_[rid].request_queue_.erase(i);
      break;
    }
  }
  // Install request in the back.
  lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  auto thisiter = lock_table_[rid].request_queue_.end();
  --thisiter;
  // Kill all younger request. Notify.
  for (auto i = lock_table_[rid].request_queue_.begin(); i != lock_table_[rid].request_queue_.end();) {
    auto trans = TransactionManager::GetTransaction(i->txn_id_);
    if (trans->GetState() != TransactionState::ABORTED && i->txn_id_ > txn->GetTransactionId()) {
      trans->SetState(TransactionState::ABORTED);
      // Erase. Maybe not exsit . Still fine.
      trans->GetSharedLockSet()->erase(rid);
      trans->GetExclusiveLockSet()->erase(rid);
      i = lock_table_[rid].request_queue_.erase(i);
      continue;
    }
    ++i;
  }
  lock_table_[rid].cv_.notify_all();
  // Wait for kill, or granted.
  while (txn->GetState() != TransactionState::ABORTED && !GrantX(rid, txn->GetTransactionId())) {
    lock_table_[rid].cv_.wait(lk);
  }
  // Check for kill.
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  thisiter->granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock lk(latch_);
  // Remove the request record.
  bool unlocked = false;
  auto iter = lock_table_[rid].request_queue_.begin();
  for (; iter != lock_table_[rid].request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      unlocked = true;
      break;
    }
  }
  // What if can not unlock?
  if (!unlocked) {
    txn->SetState(TransactionState::ABORTED);
    LOG_INFO("Unlock fail");
    return false;
  }
  if (txn->GetState() == TransactionState::GROWING &&
      (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ || iter->lock_mode_ == LockMode::EXCLUSIVE)) {
    txn->SetState(TransactionState::SHRINKING);
  }
  txn->GetExclusiveLockSet()->erase(rid);
  txn->GetSharedLockSet()->erase(rid);
  lock_table_[rid].request_queue_.erase(iter);
  lock_table_[rid].cv_.notify_all();
  return true;
}

}  // namespace bustub
