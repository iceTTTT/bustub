//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    table_info_->table_->MarkDelete(*rid, GetExecutorContext()->GetTransaction());
    for (auto *indexinfo : indexes_) {
      indexinfo->index_->DeleteEntry(tuple->KeyFromTuple(table_info_->schema_, *indexinfo->index_->GetKeySchema(),
                                                         indexinfo->index_->GetKeyAttrs()),
                                     *rid, GetExecutorContext()->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
