//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    Tuple upd = GenerateUpdatedTuple(*tuple);
    if (table_info_->table_->UpdateTuple(upd, *rid, GetExecutorContext()->GetTransaction())) {
      for (auto *indexinfo : indexes_) {
        indexinfo->index_->InsertEntry(tuple->KeyFromTuple(table_info_->schema_, *indexinfo->index_->GetKeySchema(),
                                                           indexinfo->index_->GetKeyAttrs()),
                                       *rid, GetExecutorContext()->GetTransaction());
      }
    }
  }
  return false;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
