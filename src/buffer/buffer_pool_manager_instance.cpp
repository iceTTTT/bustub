//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  latch_.lock();
  delete[] pages_;
  delete replacer_;
  latch_.unlock();
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);
  pages_[page_table_[page_id]].is_dirty_ = false;
  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  latch_.lock();
  for (auto item : page_table_) {
    latch_.unlock();
    FlushPgImp(item.first);
    latch_.lock();
  }
  latch_.unlock();
  // You can do it!
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  frame_id_t newframe;
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  latch_.lock();
  if (!free_list_.empty()) {
    newframe = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Victim(&newframe)) {
      latch_.unlock();
      return nullptr;
    }
    if (pages_[newframe].is_dirty_) {
      latch_.unlock();
      FlushPgImp(pages_[newframe].page_id_);
      latch_.lock();
    }
    page_table_.erase(pages_[newframe].page_id_);
  }
  *page_id = AllocatePage();
  page_table_[*page_id] = newframe;
  pages_[newframe].ResetMemory();
  pages_[newframe].page_id_ = *page_id;
  pages_[newframe].pin_count_ = 1;
  latch_.unlock();
  return &pages_[newframe];
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    // 1.1    If P exists, pin it and return it immediately.
    if (pages_[page_table_[page_id]].pin_count_ == 0) {
      replacer_->Pin(page_table_[page_id]);
    }

    pages_[page_table_[page_id]].pin_count_++;
    latch_.unlock();
    return &pages_[page_table_[page_id]];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  frame_id_t newframe;
  if (!free_list_.empty()) {
    newframe = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Victim(&newframe)) {
      latch_.unlock();
      return nullptr;
    }
    if (pages_[newframe].is_dirty_) {
      latch_.unlock();
      FlushPgImp(pages_[newframe].page_id_);
      latch_.lock();
    }
    page_table_.erase(pages_[newframe].page_id_);
  }

  page_table_[page_id] = newframe;
  disk_manager_->ReadPage(page_id, pages_[newframe].data_);
  pages_[newframe].page_id_ = page_id;
  pages_[newframe].pin_count_ = 1;
  pages_[newframe].is_dirty_ = false;
  latch_.unlock();
  return &pages_[newframe];
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    // 1.   If P does not exist, return true.
    latch_.unlock();
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (pages_[page_table_[page_id]].pin_count_ == 0) {
    replacer_->Pin(page_table_[page_id]);
    pages_[page_table_[page_id]].ResetMemory();
    pages_[page_table_[page_id]].is_dirty_ = false;
    pages_[page_table_[page_id]].page_id_ = INVALID_PAGE_ID;
    pages_[page_table_[page_id]].pin_count_ = 0;
    free_list_.push_back(page_table_[page_id]);
    page_table_.erase(page_id);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  if (pages_[page_table_[page_id]].pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  pages_[page_table_[page_id]].is_dirty_ |= is_dirty;
  if (--pages_[page_table_[page_id]].pin_count_ == 0) {
    replacer_->Unpin(page_table_[page_id]);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
