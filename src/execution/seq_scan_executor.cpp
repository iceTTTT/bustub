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
    if (plan_->GetPredicate() != nullptr) {
      if (plan_->GetPredicate()->Evaluate(&(*i), &tableinfo_->schema_).GetAs<bool>()) {
        *rid = i->GetRid();
        *tuple = MakeOutput(*i++);
        iter_ = i;
        return true;
      }
      continue;
    }
    *rid = i->GetRid();
    *tuple = MakeOutput(*i++);
    iter_ = i;
    return true;
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
