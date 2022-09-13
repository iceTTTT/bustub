//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <stack>
#include "common/logger.h"
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : head_(new Dlist), rear_(new Dlist), size_(num_pages), in_size_(0) {
  head_->next_ = rear_;
  rear_->prev_ = head_;
}

LRUReplacer::~LRUReplacer() {
  std::stack<Dlist *> dstack;
  Dlist *temphead = head_;
  head_ = head_->next_;
  while (head_ != rear_) {
    dstack.push(head_);
    head_ = head_->next_;
  }
  while (!dstack.empty()) {
    delete dstack.top();
    dstack.pop();
  }
  delete temphead;
  delete rear_;
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (in_size_ > 0) {
    Dlist *target = rear_->prev_;
    *frame_id = target->frame_;
    Ddelete(target);
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (lrumap_.find(frame_id) == lrumap_.end()) {
    return;
  }
  Dlist *target = lrumap_[frame_id];
  Ddelete(target);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (in_size_ == size_) {
    return;
  }
  // make new node , hash
  if (lrumap_.find(frame_id) != lrumap_.end()) {
    return;
  }
  auto newnode = new Dlist;
  newnode->frame_ = frame_id;
  lrumap_[frame_id] = newnode;
  // insert in the head
  Insert(head_, newnode);
}
void LRUReplacer::Insert(Dlist *pos, Dlist *target) {
  pos->next_->prev_ = target;
  target->next_ = pos->next_;
  target->prev_ = pos;
  pos->next_ = target;

  // increment size.
  in_size_++;
}

void LRUReplacer::Ddelete(Dlist *target) {
  target->prev_->next_ = target->next_;
  target->next_->prev_ = target->prev_;
  lrumap_.erase(target->frame_);
  delete target;
  // decrement size.
  in_size_--;
}
auto LRUReplacer::Size() -> size_t { return in_size_; }

}  // namespace bustub
