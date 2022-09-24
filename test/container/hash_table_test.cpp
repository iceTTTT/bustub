//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <random>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "buffer/parallel_buffer_pool_manager.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE

// NOLINTNEXTLINE
TEST(HashTableTest, MassiveSampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  int ivalue = 100;
  for (int i = 0; i < ivalue; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();
  // check if the inserted values are all there
  for (int i = 0; i < ivalue; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();
  // insert one more value for each key
  for (int i = 0; i < ivalue; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }
  ht.VerifyIntegrity();
  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  // EXPECT_EQ(0, res.size());
  // delete some values
  for (int i = 0; i < ivalue; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();
  // delete all values
  for (int i = 0; i < ivalue; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

void Thread1(ExtendibleHashTable<int, int, IntComparator> &ht) {
  for (int i = 50000; i < 100000; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }
}

TEST(HashTableTest, ConcurrentMassiveTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t num_instances = 5;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);
  // Parallel_Buffer_Pool_Manager.ExtendibleHashTable.
  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new ParallelBufferPoolManager(num_instances, buffer_pool_size, disk_manager);
  // 496 k-v pair in a page.
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  std::thread t1(Thread1, std::ref(ht));
  for (int i = 0; i < 50000; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  t1.join();
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
