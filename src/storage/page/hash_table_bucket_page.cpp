//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  auto da = const_cast<MappingType *>(array_);
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && !cmp(key, da[i].first)) {
      result->push_back(da[i].second);
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> int {
  auto da = const_cast<MappingType *>(array_);
  int64_t firsttomb = -1;
  int64_t firstempty = -1;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      firstempty = static_cast<int64_t>(i);
      break;
    }
    if (IsReadable(i)) {
      if (!cmp(da[i].first, key) && da[i].second == value) {
        return -1;
      }
      continue;
    }
    firsttomb = static_cast<int64_t>(i);
  }
  if (firsttomb != -1) {
    da[firsttomb].first = key;
    da[firsttomb].second = value;
    SetReadable(static_cast<uint32_t>(firsttomb));
    return 1;
  }
  if (firstempty != -1) {
    da[firstempty].first = key;
    da[firstempty].second = value;
    SetReadable(static_cast<uint32_t>(firstempty));
    SetOccupied(static_cast<uint32_t>(firstempty));
    return 1;
  }
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  auto da = const_cast<MappingType *>(array_);
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && !cmp(da[i].first, key) && value == da[i].second) {
      // Remove the target kv pair.
      ResetRead(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  auto da = const_cast<MappingType *>(array_);
  return da[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  auto da = const_cast<MappingType *>(array_);
  return da[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  if (!IsOccupied(bucket_idx)) {
    return;
  }
  ResetRead(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  uint32_t oindex = bucket_idx / 8;
  uint32_t iindex = bucket_idx % 8;
  return (occupied_[oindex] >> iindex) & 0x1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t oindex = bucket_idx / 8;
  uint32_t iindex = bucket_idx % 8;
  occupied_[oindex] |= (0x1 << iindex);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  uint32_t rindex = bucket_idx / 8;
  uint32_t iindex = bucket_idx % 8;
  return (readable_[rindex] >> iindex) & 0x1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t rindex = bucket_idx / 8;
  uint32_t iindex = bucket_idx % 8;
  readable_[rindex] |= (0x1 << iindex);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::ResetRead(uint32_t bucket_idx) {
  uint32_t rindex = bucket_idx / 8;
  uint32_t iindex = bucket_idx % 8;
  readable_[rindex] &= ~(0x1 << iindex);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  size_t bitmapsize = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  size_t nums = BUCKET_ARRAY_SIZE % 8;
  size_t fulltotal = (bitmapsize - 1) * size_t(0x11111111) + size_t((0x1 << nums) - 1);
  size_t thistotal = 0;
  for (uint32_t i = 0; i < bitmapsize; i++) {
    thistotal += size_t(readable_[i]);
  }
  return fulltotal == thistotal;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  size_t bitmapsize = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  uint32_t ret = 0;
  for (uint32_t i = 0; i < bitmapsize; i++) {
    for (uint32_t j = 0; j < 8; j++) {
      if ((readable_[i] >> j) & 0x1) {
        ret++;
      }
    }
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  size_t bitmapsize = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  for (uint32_t i = 0; i < bitmapsize; i++) {
    if (readable_[i] != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
