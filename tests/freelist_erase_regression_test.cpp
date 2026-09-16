#include <gtest/gtest.h>
#include "../freelist.h"
#include "../page.h"
#include "../logger.h"
#include <memory>
#include <vector>

using namespace bboltpp;

namespace {

// Constructs a standalone page used to feed FreeList::free without requiring a
// real mmap'd buffer. Only the id/flags/overflow fields are read by free().
bboltpp::Page makePage(bboltpp::pgid_t id, uint32_t flags = 0, uint32_t overflow = 0) {
  bboltpp::Page page;
  page.id = id;
  page.flags = flags;
  page.count = 0;
  page.overflow = overflow;
  return page;
}

// Build a freelist of the requested backend type and initialise it as empty.
std::unique_ptr<bboltpp::FreeList> newEmptyFreelist(bboltpp::FreelistType type) {
  std::unique_ptr<bboltpp::FreeList> f;
  if (type == bboltpp::FreelistType::FreelistMapType) {
    f = std::make_unique<bboltpp::HashMapFreeList>();
  } else {
    f = std::make_unique<bboltpp::ArrayFreeList>();
  }
  std::vector<bboltpp::pgid_t> empty;
  f->ReadIDs(empty);
  return f;
}

// FreeList::release must not use-after-free when multiple consecutive pending
// entries are drained and erased in one range-for. Three pending entries are
// populated; releasing a txid that drains two of them forces pending.erase(tid)
// to run for the currently-iterated node, and the range-for then has to
// advance past the freed node. Before the fix this tripped the use-after-free
// in std::_Rb_tree_increment (the range-for ++ site).
void runReleaseDrainsMultiple(bboltpp::FreelistType type) {
  auto f = newEmptyFreelist(type);

  auto p1 = makePage(5);
  auto p2 = makePage(7);
  auto p3 = makePage(9);
  f->free(10, &p1);
  f->free(20, &p2);
  f->free(30, &p3);
  ASSERT_EQ(3, f->pending_count());

  // Drains pending[tid] for tid <= 25, i.e. entries 10 and 20.
  f->release(25);
  EXPECT_EQ(1, f->pending_count());

  // A second release that drains the remaining entry exercises the erase
  // branch again (this time erasing the *last* node mid-iteration).
  f->release(30);
  EXPECT_EQ(0, f->pending_count());
}

// FreeList::releaseRange must not use-after-free when its erase branch
// (txp.ids.size() == 0) runs mid-range-for. Each pending entry holds a single
// freelist page whose alloctx is txid-1; releaseRange(begin, end) drains
// entries whose alloctx falls in [begin,end] and erases the entry when its
// ids become empty. Two matching entries are placed back-to-back so the
// range-for must continue past a freshly erased node.
void runReleaseRangeErasesMidIteration(bboltpp::FreelistType type) {
  auto f = newEmptyFreelist(type);

  // freelistPageFlag forces alloctx = txid - 1 inside FreeList::free, so the
  // releaseRange drain condition (alloctx in [begin,end]) matches below.
  auto p1 = makePage(5, bboltpp::freelistPageFlag);
  auto p2 = makePage(7, bboltpp::freelistPageFlag);
  auto p3 = makePage(9, bboltpp::freelistPageFlag);
  f->free(20, &p1);  // pending[20] = { ids:[5],  alloctx:[19] }
  f->free(30, &p2);  // pending[30] = { ids:[7],  alloctx:[29] }
  f->free(40, &p3);  // pending[40] = { ids:[9],  alloctx:[39] }
  ASSERT_EQ(3, f->pending_count());

  // [29, 50] covers tid 30 and 40. Their alloctx (29 and 39) are both in range,
  // so both pages are drained and both entries erased. tid 20 (alloctx 19) is
  // out of range and survives.
  f->releaseRange(29, 50);
  EXPECT_EQ(1, f->pending_count());

  // Drain the remaining entry: alloctx 19 < 50 so begin must be <= 19.
  f->releaseRange(19, 50);
  EXPECT_EQ(0, f->pending_count());
}

}  // namespace

class FreelistEraseRegression : public ::testing::Test {
 protected:
  void SetUp() override {
    bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
  }
};

// Guards against re-introducing the erase-during-range-for use-after-free in
// FreeList::release. Crashes (SIGSEGV in std::_Rb_tree_increment) on the
// unfixed codebase.
TEST_F(FreelistEraseRegression, DirectReleaseDrainsMultiplePending_Array) {
  ASSERT_NO_FATAL_FAILURE(runReleaseDrainsMultiple(bboltpp::FreelistType::FreelistArrayType));
}

TEST_F(FreelistEraseRegression, DirectReleaseDrainsMultiplePending_HashMap) {
  ASSERT_NO_FATAL_FAILURE(runReleaseDrainsMultiple(bboltpp::FreelistType::FreelistMapType));
}

// Guards against re-introducing the erase-during-range-for use-after-free in
// FreeList::releaseRange. Crashes (SIGSEGV in std::_Rb_tree_increment) on the
// unfixed codebase.
TEST_F(FreelistEraseRegression, DirectReleaseRangeErasesMidIteration_Array) {
  ASSERT_NO_FATAL_FAILURE(runReleaseRangeErasesMidIteration(bboltpp::FreelistType::FreelistArrayType));
}

TEST_F(FreelistEraseRegression, DirectReleaseRangeErasesMidIteration_HashMap) {
  ASSERT_NO_FATAL_FAILURE(runReleaseRangeErasesMidIteration(bboltpp::FreelistType::FreelistMapType));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
