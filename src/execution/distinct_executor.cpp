//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() { child_executor_->Init(); }

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (child_executor_->Next(tuple, rid)) {
    if (distinct_.find(tuple->ToString(plan_->GetChildPlan()->OutputSchema())) != distinct_.end()) {
      continue;
    }
    distinct_.emplace(tuple->ToString(plan_->GetChildPlan()->OutputSchema()));
    return true;
  }
  return false;
}

}  // namespace bustub
