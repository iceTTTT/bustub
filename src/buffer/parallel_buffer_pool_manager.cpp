//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"
namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances), start_index_(0), poolsize_(pool_size) {
  // Allocate and create individual BufferPoolManagerInstances
  multibufferpool_ = new BufferPoolManagerInstance *[num_instances_];
  for (size_t i = 0; i < num_instances_; i++) {
    multibufferpool_[i] = new BufferPoolManagerInstance(poolsize_, num_instances_, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
    delete multibufferpool_[i];
  }
  delete[] multibufferpool_;
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * poolsize_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  size_t targetindex = page_id % num_instances_;
  return multibufferpool_[targetindex];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  size_t targetindex = page_id % num_instances_;
  return multibufferpool_[targetindex]->FetchPgImp(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  size_t targetindex = page_id % num_instances_;
  return multibufferpool_[targetindex]->UnpinPgImp(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  size_t targetindex = page_id % num_instances_;
  return multibufferpool_[targetindex]->FlushPgImp(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  size_t start = start_index_;
  Page *ret;
  do {
    ret = multibufferpool_[start_index_]->NewPgImp(page_id);
    start_index_ = (start_index_ + 1) % num_instances_;
  } while (ret == nullptr && start_index_ != start);
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  return ret;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  size_t targetindex = page_id % num_instances_;
  return multibufferpool_[targetindex]->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    multibufferpool_[i]->FlushAllPgsImp();
  }
}

}  // namespace bustub
