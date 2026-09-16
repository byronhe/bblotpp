#include <gtest/gtest.h>
#include "../freelist.h"
#include "../page.h"
#include "../logger.h"
#include "test_common.h"

using namespace bboltpp;

static bboltpp::Page makePage(bboltpp::pgid_t id) {
  bboltpp::Page p;
  p.id = id;
  p.flags = 0;
  p.count = 0;
  p.overflow = 0;
  return p;
}

// G5: the genuine double-free guard (freelist.cpp:127-129) must STILL abort when
// a page that is genuinely already free (cache[id] == true) is freed again.
// This proves the fix did not weaken the real safety guard. With the fix every
// cache entry is true (only free()/reindex() insert them), so reaching this
// guard means a real double free.
TEST(FreeListDoubleFreeGuard, GenuineDoubleFreeAborts) {
  SetLogger(std::make_unique<DefaultLogger>());
  auto f = std::make_unique<ArrayFreeList>();
  auto p5 = makePage(5);
  f->free(100, &p5);          // legitimate: cache[5] == true
  ASSERT_EQ(f->cache.at(5), true);
  // Freeing the same page a second time must panic("page 5 already freed").
  EXPECT_DEATH(f->free(100, &p5), ".*");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
