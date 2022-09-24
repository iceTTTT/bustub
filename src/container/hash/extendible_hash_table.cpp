//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // allocate a page for directory.
  table_latch_.WLock();
  Page *dp = buffer_pool_manager->NewPage(&directory_page_id_);
  auto rdp = reinterpret_cast<HashTableDirectoryPage *>(dp);
  reinterpret_cast<Page *>(rdp)->WLatch();
  rdp->SetPageId(directory_page_id_);
  // Global Depth equals 0.
  page_id_t targetpage;
  buffer_pool_manager_->NewPage(&targetpage);
  buffer_pool_manager_->UnpinPage(targetpage, false);
  reftopage_[0] = targetpage;
  rdp->SetBucketPageId(0, 0);
  reinterpret_cast<Page *>(rdp)->WUnlatch();
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return reftopage_[dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page))];
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage *dp = FetchDirectoryPage();
  reinterpret_cast<Page *>(dp)->RLatch();
  page_id_t targetpage = KeyToPageId(key, dp);
  HASH_TABLE_BUCKET_TYPE *p = FetchBucketPage(targetpage);
  reinterpret_cast<Page *>(p)->RLatch();
  bool ret = p->GetValue(key, comparator_, result);
  reinterpret_cast<Page *>(p)->RUnlatch();
  reinterpret_cast<Page *>(dp)->RUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(targetpage, false);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage *dp = FetchDirectoryPage();
  Page *pdp = reinterpret_cast<Page *>(dp);
  pdp->RLatch();
  page_id_t targetpage = KeyToPageId(key, dp);
  HASH_TABLE_BUCKET_TYPE *orip = FetchBucketPage(targetpage);
  Page *pop = reinterpret_cast<Page *>(orip);
  pop->WLatch();
  int sign = orip->Insert(key, value, comparator_);
  pop->WUnlatch();
  if (sign == 1) {
    // Succeeded.
    pdp->RUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(targetpage, true);
    return true;
  }
  if (sign == 0 && dp->GetLocalDepth(KeyToDirectoryIndex(key, dp)) < 9) {
    pdp->RUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(targetpage, false);
    return SplitInsert(nullptr, key, value);
  }
  // Duplicate kv pair. or reach maximum depth.
  pdp->RUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(targetpage, false);
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  HashTableDirectoryPage *dp = FetchDirectoryPage();
  page_id_t targetpage = KeyToPageId(key, dp);
  page_id_t newpage;
  uint32_t dindex = KeyToDirectoryIndex(key, dp);
  uint32_t iindex;
  uint32_t thisld = dp->GetLocalDepth(dindex);
  uint32_t gd = dp->GetGlobalDepth();
  // Acquire talbe write lock.
  auto orip = FetchBucketPage(targetpage);
  auto imap = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&newpage));
  if (thisld < gd) {
    // Get image index.
    bool highbit = static_cast<bool>(dp->GetLocalHighBit(dindex));
    if (highbit) {
      iindex = dindex & ~(0x1 << thisld);
    } else {
      iindex = dindex | (0x1 << thisld);
    }
    // Increment local depth of two index, update page in refpage.
    uint32_t premask = dp->GetLocalDepthMask(dindex);
    page_id_t preref = dp->GetBucketPageId(dindex);
    dp->IncrLocalDepth(dindex);
    dp->IncrLocalDepth(iindex);
    uint32_t newmask = dp->GetLocalDepthMask(dindex);
    page_id_t dref = dindex & newmask;
    page_id_t iref = iindex & newmask;
    reftopage_[dref] = reftopage_[preref];
    if (preref != dref) {
      reftopage_.erase(preref);
    }
    dp->SetBucketPageId(dindex, dref);
    // Register new page in refpage for image index.
    reftopage_[iref] = newpage;
    dp->SetBucketPageId(iindex, iref);
    // Move certain k-v to image page.
    for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
      if (!orip->IsOccupied(i)) {
        break;
      }
      if (static_cast<page_id_t>(Hash(orip->KeyAt(i)) & newmask) != dref) {
        orip->RemoveAt(i);
        imap->Insert(orip->KeyAt(i), orip->ValueAt(i), comparator_);
      }
    }
    // Set local depth of all(have same low bit with dindex).
    for (uint32_t idx = 0; idx < dp->Size(); idx++) {
      if (static_cast<page_id_t>(idx & premask) == preref) {
        dp->SetLocalDepth(idx, thisld + 1);
        if (static_cast<page_id_t>(idx & newmask) == iref) {
          dp->SetBucketPageId(idx, iref);
        } else {
          dp->SetBucketPageId(idx, dref);
        }
      }
    }
  } else {
    // To increment the globaldepth , we need copy things to the next generation.
    uint32_t plusv = 1;
    for (uint32_t i = 0; i < gd; i++) {
      plusv *= 2;
    }
    for (uint32_t idx = 0; idx < dp->Size(); idx++) {
      dp->SetLocalDepth(idx + plusv, dp->GetLocalDepth(idx));
      dp->SetBucketPageId(idx + plusv, dp->GetBucketPageId(idx));
    }
    dp->IncrGlobalDepth();
    // Get image index. Increment relative local depth.
    iindex = dindex | (0x1 << thisld);
    dp->IncrLocalDepth(dindex);
    dp->IncrLocalDepth(iindex);
    // Set new  bucketpageid.
    uint32_t newmask = dp->GetLocalDepthMask(dindex);
    page_id_t dref = dindex & newmask;
    page_id_t iref = iindex & newmask;
    page_id_t preref = dp->GetBucketPageId(dindex);
    reftopage_[dref] = reftopage_[preref];
    if (dref != preref) {
      reftopage_.erase(preref);
    }
    dp->SetBucketPageId(dindex, dref);
    // Register new page for iindex.
    reftopage_[iref] = newpage;
    dp->SetBucketPageId(iindex, iref);
    // Move certain k-v to image page.
    for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
      if (!orip->IsOccupied(i)) {
        break;
      }
      if ((Hash(orip->KeyAt(i)) & newmask) != static_cast<uint32_t>(dref)) {
        orip->RemoveAt(i);
        imap->Insert(orip->KeyAt(i), orip->ValueAt(i), comparator_);
      }
    }
  }
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(newpage, true);
  buffer_pool_manager_->UnpinPage(targetpage, true);
  return Insert(nullptr, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage *dp = FetchDirectoryPage();
  Page *pdp = reinterpret_cast<Page *>(dp);
  pdp->RLatch();
  page_id_t targetpage = KeyToPageId(key, dp);
  HASH_TABLE_BUCKET_TYPE *p = FetchBucketPage(targetpage);
  Page *pop = reinterpret_cast<Page *>(p);
  pop->WLatch();
  if (p->Remove(key, value, comparator_)) {
    // Remove successfully and  merge when necessary.
    pop->WUnlatch();
    pdp->RUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(targetpage, false);
    Merge(nullptr, key, value);
    return true;
  }
  if (p->IsEmpty()) {
    pop->WUnlatch();
    pdp->RUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(targetpage, false);
    Merge(nullptr, key, value);
    return false;
  }
  // fail to remove
  pop->WUnlatch();
  pdp->RUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(targetpage, false);
  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dp = FetchDirectoryPage();
  uint32_t dindex = KeyToDirectoryIndex(key, dp);
  uint32_t iindex;
  uint32_t tld = dp->GetLocalDepth(dindex);
  page_id_t targetpage = KeyToPageId(key, dp);
  HASH_TABLE_BUCKET_TYPE *p = FetchBucketPage(targetpage);
  // Get the image index.
  bool highbit = static_cast<bool>((dindex >> (tld - 1)) & 0x1);
  if (highbit) {
    iindex = dindex & ~(0x1 << (tld - 1));
  } else {
    iindex = dindex | (0x1 << (tld - 1));
  }
  if (tld > 0 && p->IsEmpty() && dp->GetLocalDepth(iindex) == tld) {
    // Should merge.
    dp->DecrLocalDepth(dindex);
    dp->DecrLocalDepth(iindex);
    uint32_t lowmask = dp->GetLocalDepthMask(dindex);
    page_id_t lowpageref = dindex & lowmask;
    page_id_t predref = dp->GetBucketPageId(dindex);
    page_id_t preiref = dp->GetBucketPageId(iindex);
    reftopage_.erase(predref);
    dp->SetBucketPageId(dindex, lowpageref);
    dp->SetBucketPageId(iindex, lowpageref);
    reftopage_[lowpageref] = reftopage_[preiref];
    if (preiref != lowpageref) {
      reftopage_.erase(preiref);
    }
    // Cast changes to all have the same lowpageref.
    // Check if can shrink. If all local depth is smaller than global depth, then shrink.
    bool shrink = true;
    uint32_t gd = dp->GetGlobalDepth();
    for (uint32_t i = 0; i < dp->Size(); i++) {
      if (dp->GetLocalDepth(i) == gd) {
        shrink = false;
      }
      if (static_cast<page_id_t>(i & lowmask) == lowpageref) {
        dp->SetBucketPageId(i, lowpageref);
        dp->SetLocalDepth(i, tld - 1);
      }
    }
    if (shrink) {
      dp->DecrGlobalDepth();
    }
  }
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(targetpage, true);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
