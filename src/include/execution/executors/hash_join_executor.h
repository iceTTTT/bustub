//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

using ITER = std::unordered_multimap<std::string, Tuple>::iterator;
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

  /** Generate merge tuple */
  auto GenerateMergeTuple(const Tuple &left, const Tuple &right) -> Tuple;

 private:
  /** The Hash join plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** Left child */
  std::unique_ptr<AbstractExecutor> leftx_;
  /** Right child */
  std::unique_ptr<AbstractExecutor> rightx_;
  /** Left tuple */
  Tuple ltuple_;
  /** Right tuple */
  Tuple rtuple_;
  /** Left rid */
  RID lrid_;
  /** Right rid */
  RID rrid_;
  /** Sign for left */
  bool shouldfroze_ = false;
  /** Hash table */
  std::unordered_multimap<std::string, Tuple> ht_{};
  /** Iter for probe */
  ITER iter_;
  /** Iter for end */
  std::pair<ITER, ITER> range_;
};

}  // namespace bustub
