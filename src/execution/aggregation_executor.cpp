//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      ht_(plan->GetAggregates(), plan_->GetAggregateTypes()),
      iterator_(ht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple atuple;
  RID arid;
  while (child_->Next(&atuple, &arid)) {
    ht_.InsertCombine(MakeAggregateKey(&atuple), MakeAggregateValue(&atuple));
  }
  iterator_ = ht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iterator_ != ht_.End()) {
    // Make output.
    uint32_t colcount = plan_->OutputSchema()->GetColumnCount();
    std::vector<Value> values;
    for (uint32_t i = 0; i < colcount; i++) {
      values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->EvaluateAggregate(iterator_.Key().group_bys_,
                                                                                        iterator_.Val().aggregates_));
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    // Check Having clause.
    if (plan_->GetHaving() != nullptr) {
      if (plan_->GetHaving()
              ->EvaluateAggregate(iterator_.Key().group_bys_, iterator_.Val().aggregates_)
              .GetAs<bool>()) {
        ++iterator_;
        return true;
      }
      ++iterator_;
      continue;
    }
    ++iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
