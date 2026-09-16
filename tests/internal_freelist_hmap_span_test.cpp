// Regression tests for HashMapFreeList::Allocate span-split correctness.
//
// These tests target the bug where Allocate called `delSpan(pid, uint64_t(n))`
// instead of `delSpan(pid, size)` when serving a request for `n` pages from a
// located span of `size` pages (with `n < size`). The buggy delSpan removed the
// wrong bucket key from freemaps and erased an absent key from backwardMap,
// leaving a stale orphan span in freemaps that forwardMap/backwardMap no longer
// tracked. That orphan could be re-selected by a later Allocate, handing out a
// page id that was already in use.
//
// The fix restores the documented invariant (freelist.h:40-43) that freemaps,
// forwardMap, and backwardMap always track the *same* set of spans.
//
// This file is built as its own xmake target and includes freelist_hmap.cpp
// directly (the established "unity-include" convention used by the sibling
// hashmap/array/allocate freelist tests). FreeList is a struct, so its
// freemaps/forwardMap/backwardMap members are publicly inspectable here.

#include <cstdint>
#include <map>
#include <set>
#include <vector>

#include <gtest/gtest.h>

#include "test_common.h"
#include "../freelist.h"
#include "../freelist_hmap.cpp"
#include "../page.h"
#include "../logger.h"

using namespace bboltpp;

namespace {

// Build a contiguous pgid_vec for pages [first .. last] inclusive.
std::vector<pgid_t> contiguousPages(pgid_t first, pgid_t last) {
  std::vector<pgid_t> v;
  for (pgid_t p = first; p <= last; ++p) v.push_back(p);
  return v;
}

// ReadIDs() takes a non-const reference, so take the vector by value here to
// accept temporaries and forward an lvalue into the freelist.
void loadIDs(HashMapFreeList &f, std::vector<pgid_t> ids) { f.ReadIDs(ids); }

// Assert the freelist.h:40-43 invariant: freemaps, forwardMap and backwardMap
// must describe exactly the same set of spans, and FreeCount() (which sums
// forwardMap sizes) must agree with the aggregate span size.
::testing::AssertionResult indexesConsistent(FreeList &f) {
  // forwardMap <-> backwardMap must agree on span count.
  if (f.forwardMap.size() != f.backwardMap.size()) {
    return ::testing::AssertionFailure()
           << "forwardMap size (" << f.forwardMap.size() << ") != backwardMap size ("
           << f.backwardMap.size() << ")";
  }

  // Every forwardMap span must have matching freemaps + backwardMap entries.
  for (const auto &[start, size] : f.forwardMap) {
    auto fmIt = f.freemaps.find(size);
    if (fmIt == f.freemaps.end()) {
      return ::testing::AssertionFailure()
             << "freemaps missing bucket for size=" << size << " (start=" << start << ")";
    }
    if (fmIt->second.count(start) == 0) {
      return ::testing::AssertionFailure()
             << "freemaps[" << size << "] does not contain start=" << start;
    }
    pgid_t end = start + pgid_t(size - 1);
    auto bwIt = f.backwardMap.find(end);
    if (bwIt == f.backwardMap.end()) {
      return ::testing::AssertionFailure()
             << "backwardMap missing end=" << end << " for span start=" << start
             << " size=" << size;
    }
    if (bwIt->second != size) {
      return ::testing::AssertionFailure()
             << "backwardMap[" << end << "] = " << bwIt->second << " != size=" << size;
    }
  }

  // Every freemaps span must have matching forwardMap + backwardMap entries,
  // and the total span count across all freemaps buckets must equal
  // forwardMap.size() (no orphan entries in freemaps).
  std::size_t freemaps_span_count = 0;
  for (const auto &[size, pidSet] : f.freemaps) {
    for (const auto &[start, _] : pidSet) {
      ++freemaps_span_count;
      auto fwIt = f.forwardMap.find(start);
      if (fwIt == f.forwardMap.end()) {
        return ::testing::AssertionFailure()
               << "forwardMap missing start=" << start << " (present in freemaps[" << size
               << "]) -> stale orphan in freemaps";
      }
      if (fwIt->second != size) {
        return ::testing::AssertionFailure()
               << "forwardMap[" << start << "] = " << fwIt->second
               << " != freemaps size=" << size;
      }
      pgid_t end = start + pgid_t(size - 1);
      auto bwIt = f.backwardMap.find(end);
      if (bwIt == f.backwardMap.end() || bwIt->second != size) {
        return ::testing::AssertionFailure()
               << "backwardMap[" << end << "] inconsistent for span start=" << start
               << " size=" << size;
      }
    }
  }
  if (freemaps_span_count != f.forwardMap.size()) {
    return ::testing::AssertionFailure()
           << "freemaps span count (" << freemaps_span_count << ") != forwardMap size ("
           << f.forwardMap.size() << ") -> stale orphan span in freemaps";
  }

  // FreeCount() sums forwardMap sizes (see HashMapFreeList::FreeCount).
  int expectedFreeCount = 0;
  for (const auto &[_, size] : f.forwardMap) expectedFreeCount += int(size);
  if (f.FreeCount() != expectedFreeCount) {
    return ::testing::AssertionFailure()
           << "FreeCount() = " << f.FreeCount()
           << " != sum of forwardMap sizes = " << expectedFreeCount;
  }

  return ::testing::AssertionSuccess();
}

#define ASSERT_INDEXES_CONSISTENT(f) ASSERT_TRUE(indexesConsistent(f))

}  // namespace

class HashMapFreeListSpanTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cleanupTestFiles();
    bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
  }
  void TearDown() override { cleanupTestFiles(); }
};

// Allocating n < size pages from a strictly larger span must remove the whole
// located span from all three indexes and leave only the remainder span.
// Before the fix, delSpan(pid, uint64_t(n)) erased the wrong freemaps bucket
// and an absent backwardMap key, leaving a stale orphan in freemaps.
TEST_F(HashMapFreeListSpanTest, AllocateSplitRemovesWholeLocatedSpan) {
  auto f = std::make_unique<HashMapFreeList>();
  loadIDs(*f, contiguousPages(2, 10));  // 9-page span at page 2.
  ASSERT_INDEXES_CONSISTENT(*f);
  ASSERT_EQ(9, f->FreeCount());
  ASSERT_EQ(1u, f->freemaps.count(9));
  ASSERT_EQ(1u, f->freemaps[9].count(2));

  pgid_t pid = f->Allocate(1, 4);  // request 4 from a 9-page span.
  ASSERT_EQ(2, pid);

  // Remainder span is pages [6..10], i.e. start=6 size=5.
  ASSERT_EQ(1u, f->freemaps.size()) << "freemaps must contain exactly one span (no orphans)";
  ASSERT_EQ(1u, f->freemaps.count(5));
  ASSERT_EQ(1u, f->freemaps[5].count(6));
  ASSERT_EQ(1u, f->forwardMap.size());
  ASSERT_EQ(5, f->forwardMap[6]);
  ASSERT_EQ(1u, f->backwardMap.size());
  ASSERT_EQ(5, f->backwardMap[10]);
  // The original 9-page span must be gone from every index. Its forwardMap
  // start key (2) and its freemaps size bucket (9) must not survive.
  EXPECT_EQ(0u, f->freemaps.count(9));
  EXPECT_EQ(0u, f->forwardMap.count(2));
  ASSERT_EQ(5, f->FreeCount());
  ASSERT_INDEXES_CONSISTENT(*f);
}

// The user-visible effect of the bug: a stale freemaps orphan could be
// re-selected by a later Allocate, handing out a page already in use.
// After two 4-page allocations from a 9-page span, only a single page
// remains; a third 4-page request must fail (return 0) rather than re-issue
// page 6, which the second allocation already handed out.
TEST_F(HashMapFreeListSpanTest, NoHandedOutPageIsEverReallocated) {
  auto f = std::make_unique<HashMapFreeList>();
  loadIDs(*f, contiguousPages(2, 10));  // 9-page span [2..10].
  ASSERT_INDEXES_CONSISTENT(*f);

  std::set<pgid_t> handedOut;
  auto recordAndCheck = [&](pgid_t pid, int n) {
    if (pid != 0) {
      for (int i = 0; i < n; ++i) {
        ASSERT_TRUE(handedOut.insert(pid + pgid_t(i)).second)
            << "page " << (pid + i) << " reallocated while still in use";
      }
    }
    ASSERT_INDEXES_CONSISTENT(*f);
  };

  {
    pgid_t pid = f->Allocate(1, 4);
    EXPECT_EQ(2, pid);   // uses pages 2..5
    recordAndCheck(pid, 4);
  }
  {
    pgid_t pid = f->Allocate(2, 4);
    EXPECT_EQ(6, pid);   // uses pages 6..9 (remainder of 5 split)
    recordAndCheck(pid, 4);
  }
  {
    pgid_t pid = f->Allocate(3, 4);
    EXPECT_EQ(0, pid);   // only page 10 left; a 4-page request fails.
    recordAndCheck(pid, 4);
  }
}

// Splitting one span must not corrupt the index entries of any other span.
// With the bug, the wrong backwardMap key was erased and an absent freemaps
// bucket was touched, perturbing unrelated spans' consistency.
TEST_F(HashMapFreeListSpanTest, AllocatesFromSmallestLargeEnoughSpanNoCrossCorruption) {
  auto f = std::make_unique<HashMapFreeList>();

  // Two spans: [2..6] (size 5) and [20..25] (size 6).
  std::vector<pgid_t> ids;
  for (pgid_t p = 2; p <= 6; ++p) ids.push_back(p);
  for (pgid_t p = 20; p <= 25; ++p) ids.push_back(p);
  f->ReadIDs(ids);
  ASSERT_INDEXES_CONSISTENT(*f);
  ASSERT_EQ(2u, f->freemaps.size());
  ASSERT_EQ(11, f->FreeCount());

  // Request 5: exact match against the size-5 span [2..6] (lower_bound picks 5
  // before 6). Must remove only that span; the size-6 span stays untouched.
  EXPECT_EQ(2, f->Allocate(1, 5));
  ASSERT_INDEXES_CONSISTENT(*f);
  ASSERT_EQ(1u, f->freemaps.size());
  ASSERT_EQ(1u, f->freemaps.count(6));
  ASSERT_EQ(0u, f->freemaps.count(5));
  ASSERT_EQ(6, f->FreeCount());

  // Request 4 from the size-6 span [20..25]: split, remainder [24..25] size 2.
  EXPECT_EQ(20, f->Allocate(2, 4));
  ASSERT_INDEXES_CONSISTENT(*f);
  ASSERT_EQ(1u, f->freemaps.size());
  ASSERT_EQ(1u, f->freemaps.count(2));
  ASSERT_EQ(1u, f->freemaps[2].count(24));
  ASSERT_EQ(2, f->FreeCount());

  // Request 2: exact match against the size-2 remainder [24..25].
  EXPECT_EQ(24, f->Allocate(3, 2));
  ASSERT_INDEXES_CONSISTENT(*f);
  EXPECT_TRUE(f->freemaps.empty());
  EXPECT_EQ(0, f->FreeCount());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
