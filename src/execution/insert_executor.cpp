//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executor_factory.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), subex_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  tableinfo_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(tableinfo_->name_);
  if (plan_->IsRawInsert()) {
    riter_ = plan_->RawValues().begin();
  } else {
    subex_->Init();
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // Raw insert.
  if (plan_->IsRawInsert()) {
    while (riter_ != plan_->RawValues().end()) {
      *tuple = Tuple(*riter_++, &tableinfo_->schema_);
      if (tableinfo_->table_->InsertTuple(*tuple, rid, GetExecutorContext()->GetTransaction())) {
        // Update index after success.
        for (auto *indexinfo : indexes_) {
          indexinfo->index_->InsertEntry(tuple->KeyFromTuple(tableinfo_->schema_, *indexinfo->index_->GetKeySchema(),
                                                             indexinfo->index_->GetKeyAttrs()),
                                         *rid, GetExecutorContext()->GetTransaction());
        }
      }
    }
    return false;
  }
  // Sub insert.
  while (subex_->Next(tuple, rid)) {
    if (tableinfo_->table_->InsertTuple(*tuple, rid, GetExecutorContext()->GetTransaction())) {
      for (auto *indexinfo : indexes_) {
        indexinfo->index_->InsertEntry(tuple->KeyFromTuple(tableinfo_->schema_, *indexinfo->index_->GetKeySchema(),
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, GetExecutorContext()->GetTransaction());
      }
    }
  }
  return false;
}

}  // namespace bustub
