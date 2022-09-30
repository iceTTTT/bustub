//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), leftx_(std::move(left_child)), rightx_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  leftx_->Init();
  rightx_->Init();
  while (leftx_->Next(&ltuple_, &lrid_)) {
    ht_.insert(std::make_pair(plan_->LeftJoinKeyExpression()->Evaluate(&ltuple_, leftx_->GetOutputSchema()).ToString(),
                              ltuple_));
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (shouldfroze_ || rightx_->Next(&rtuple_, &rrid_)) {
    if (!shouldfroze_) {
      range_ =
          ht_.equal_range(plan_->RightJoinKeyExpression()->Evaluate(&rtuple_, rightx_->GetOutputSchema()).ToString());
      iter_ = range_.first;
    }
    for (auto i = iter_; i != range_.second; i++) {
      *tuple = GenerateMergeTuple(i->second, rtuple_);
      shouldfroze_ = true;
      iter_ = ++i;
      return true;
    }
    shouldfroze_ = false;
  }
  return false;
}
auto HashJoinExecutor::GenerateMergeTuple(const Tuple &l, const Tuple &r) -> Tuple {
  std::vector<Value> values;
  uint32_t c = plan_->OutputSchema()->GetColumnCount();
  for (uint32_t i = 0; i < c; i++) {
    values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->EvaluateJoin(
        &l, plan_->GetLeftPlan()->OutputSchema(), &r, plan_->GetRightPlan()->OutputSchema()));
  }
  return Tuple(values, plan_->OutputSchema());
}
}  // namespace bustub
