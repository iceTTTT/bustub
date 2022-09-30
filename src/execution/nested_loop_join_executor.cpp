//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), leftx_(std::move(left_executor)), rightx_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  leftx_->Init();
  rightx_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (shouldfroze_ || leftx_->Next(&ltuple_, &lrid_)) {
    while (rightx_->Next(&rtuple_, &rrid_)) {
      if (plan_->Predicate()
              ->EvaluateJoin(&ltuple_, plan_->GetLeftPlan()->OutputSchema(), &rtuple_,
                             plan_->GetRightPlan()->OutputSchema())
              .GetAs<bool>()) {
        *tuple = GenerateTuple(ltuple_, rtuple_);
        shouldfroze_ = true;
        return true;
      }
    }
    rightx_->Init();
    shouldfroze_ = false;
  }
  return false;
}
auto NestedLoopJoinExecutor::GenerateTuple(const Tuple &l, const Tuple &r) -> Tuple {
  std::vector<Value> values;
  uint32_t c = plan_->OutputSchema()->GetColumnCount();
  for (uint32_t i = 0; i < c; i++) {
    values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->EvaluateJoin(
        &l, plan_->GetLeftPlan()->OutputSchema(), &r, plan_->GetRightPlan()->OutputSchema()));
  }
  return Tuple(values, plan_->OutputSchema());
}
}  // namespace bustub
