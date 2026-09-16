#include <gtest/gtest.h>
#include "../db.h"
#include "../bucket.h"
#include "../node.h"
#include "../page.h"
#include "../logger.h"
#include "test_common.h"
#include <cstdio>
#include <memory>
#include <string>

using namespace bboltpp;

// Reproducer for the heap-use-after-free in Node::spill()'s child-iteration
// loop (node.cpp).
//
// A branch B is constructed by hand and kept alive by the test as a shared_ptr,
// so splitTwo()'s parent.lock() resolves to B and takes the push_back branch
// (node.cpp:314-315) rather than the new-parent branch. B owns two oversized
// leaf children. Calling B->spill() drives the child-spill loop. Each child's
// spill() splits the leaf (split -> splitTwo), and splitTwo() executes
// parent_ptr->children.push_back(next) onto B's `children` vector while B is
// mid-iteration of that same vector.
//
// With the buggy range-for the cached end() iterator dangles after the
// push_back reallocates B->children; the next loop step reads freed memory
// (heap-use-after-free under ASAN, SIGSEGV otherwise). The index-based loop
// re-reads children.size() and indexes the live vector each iteration, so it
// is safe and also spills the freshly materialized siblings (matching upstream
// Go bbolt's `for i := 0; i < len(n.children); i++`).

class NodeSpillReproTest : public ::testing::Test {
 protected:
  void SetUp() override {
    bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
    cleanupTestFiles();
  }
  void TearDown() override { cleanupTestFiles(); }
};

static void FillLeaf(Node *n, const std::string &prefix, int count, int valSize) {
  char buf[16];
  for (int i = 0; i < count; ++i) {
    std::snprintf(buf, sizeof(buf), "%03d", i);
    auto key = prefix + "-k" + buf;
    n->put(key, key, std::string(valSize, 'x'), 0, 0);
  }
}

TEST_F(NodeSpillReproTest, SpillIteratesChildrenSafelyWhenSplitTwoPushesBack) {
  auto [db, oerr] = DB::Open("test_spill_repro.db", 0644, nullptr);
  ASSERT_TRUE(oerr.OK());
  auto [tx, terr] = db->Begin(true);
  ASSERT_TRUE(terr.OK());
  auto [bucket, berr] = tx->CreateBucket("test");
  ASSERT_EQ(ErrorCode::OK, berr);
  auto bucket_wp = std::weak_ptr<Bucket>(bucket);

  // Branch B: subtree root with no parent. Held by a shared_ptr so it stays
  // alive and splitTwo()'s parent.lock() resolves to it -> push_back branch.
  auto B = std::make_shared<Node>();
  B->isLeaf = false;
  B->bucket = bucket_wp;

  // Two oversized leaf children. Each is large enough that its own spill()
  // splits it into multiple pages, so splitTwo() push_backs new siblings onto
  // B->children while B is iterating that vector.
  auto L1 = std::make_shared<Node>();
  L1->isLeaf = true;
  L1->bucket = bucket_wp;
  L1->parent = B;
  FillLeaf(L1.get(), "l1", 16, 600);  // size ~ 16 + 16*(16+8+600) = 10000 > 4096

  auto L2 = std::make_shared<Node>();
  L2->isLeaf = true;
  L2->bucket = bucket_wp;
  L2->parent = B;
  FillLeaf(L2.get(), "l2", 16, 600);

  B->children = {L1, L2};

  // B->spill() must not crash and must return OK.
  auto err = B->spill();
  EXPECT_EQ(ErrorCode::OK, err);

  // Each oversized leaf must have split at least once during the loop.
  EXPECT_GE(tx->stats.Split, 2);

  tx->Rollback();
  db->Close();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
