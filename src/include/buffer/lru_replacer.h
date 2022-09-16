//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>
#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  struct Dlist {
    Dlist *next_;
    Dlist *prev_;
    frame_id_t frame_;
  };
  Dlist *head_;
  Dlist *rear_;
  size_t size_;
  std::unordered_map<frame_id_t, Dlist *> lrumap_;
  explicit LRUReplacer(size_t num_pages);
  std::mutex latch_;
  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

  void Insert(Dlist *pos, Dlist *target);

  void Ddelete(Dlist *target);

 private:
  // TODO(student): implement me!
};

}  // namespace bustub
