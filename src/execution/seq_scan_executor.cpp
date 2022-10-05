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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iter_(GetExecutorContext()
                ->GetCatalog()
                ->GetTable(plan_->GetTableOid())
                ->table_->Begin(GetExecutorContext()->GetTransaction())) {}

void SeqScanExecutor::Init() {
  tableinfo_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = GetExecutorContext()
              ->GetCatalog()
              ->GetTable(plan_->GetTableOid())
              ->table_->Begin(GetExecutorContext()->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  for (TableIterator i = iter_; i != tableinfo_->table_->End(); i++) {
    auto txn = GetExecutorContext()->GetTransaction();
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
      case IsolationLevel::READ_COMMITTED: {
        if (!GetExecutorContext()->GetLockManager()->LockShared(txn, i->GetRid())) {
          return false;
        }
        break;
      }
      case IsolationLevel::READ_UNCOMMITTED: {
        break;
      }
    }
    Tuple temp;
    if (!tableinfo_->table_->GetTuple(i->GetRid(), &temp, txn)) {
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !txn->IsExclusiveLocked(i->GetRid()) &&
          !GetExecutorContext()->GetLockManager()->Unlock(txn, i->GetRid())) {
        return false;
      }
      continue;
    }
    if (plan_->GetPredicate() != nullptr) {
      if (plan_->GetPredicate()->Evaluate(&(*i), &tableinfo_->schema_).GetAs<bool>()) {
        *rid = i->GetRid();
        auto pre = i;
        *tuple = MakeOutput(*i++);
        iter_ = i;
        return !(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !txn->IsExclusiveLocked(pre->GetRid()) &&
                 !GetExecutorContext()->GetLockManager()->Unlock(txn, pre->GetRid()));
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !txn->IsExclusiveLocked(i->GetRid()) &&
          !GetExecutorContext()->GetLockManager()->Unlock(txn, i->GetRid())) {
        return false;
      }
      continue;
    }
    *rid = i->GetRid();
    auto pre = i;
    *tuple = MakeOutput(*i++);
    iter_ = i;
    return !(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !txn->IsExclusiveLocked(pre->GetRid()) &&
             !GetExecutorContext()->GetLockManager()->Unlock(txn, pre->GetRid()));
  }
  return false;
}
auto SeqScanExecutor::MakeOutput(const Tuple &t) -> Tuple {
  uint32_t c = plan_->OutputSchema()->GetColumnCount();
  std::vector<Value> values;
  for (uint32_t i = 0; i < c; i++) {
    values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(&t, &tableinfo_->schema_));
  }
  return Tuple(values, plan_->OutputSchema());
}
}  // namespace bustub
