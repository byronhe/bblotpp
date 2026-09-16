#include <gtest/gtest.h>
#include "test_common.h"

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "../bucket.h"
#include "../cursor.h"
#include "../db.h"
#include "../logger.h"

using namespace bboltpp;

namespace {

constexpr const char* kBucket = "bucket";

}  // namespace

// Regression coverage for Cursor::Prev(). Before the fix, calling Prev() when
// the cursor was already on the first key used an unsigned loop index, so
// `i >= 0` was a tautology, `i` underflowed to SIZE_MAX, and `stack[SIZE_MAX]`
// was read out of bounds (heap-buffer-overflow under AddressSanitizer). The
// documented behavior — matching Go bbolt — is to return an empty key/value
// and leave the cursor "before first", harmlessly.
class CursorPrevTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cleanupTestFiles();
    SetLogger(std::make_unique<DiscardLogger>());
  }

  void TearDown() override {
    if (db_) {
      db_->Close();
      db_.reset();
    }
    cleanupTestFiles();
  }

  // Open a fresh DB, create `kBucket`, insert the given items in a writable tx,
  // commit, and return a read-only transaction over the same DB for cursor
  // reads. The returned tx (and its bucket/cursor) are valid for the test body.
  std::shared_ptr<Tx> openWith(const std::vector<std::pair<std::string, std::string>>& items,
                                int page_size = 0) {
    path_ = "test_cursor_prev_" + std::to_string(++fileIndex_) + ".db";
    std::filesystem::remove(path_);

    Options opts;
    if (page_size > 0) opts.PageSize = page_size;

    auto [db, oerr] = DB::Open(path_, 0600, page_size > 0 ? &opts : nullptr);
    EXPECT_TRUE(oerr.OK()) << "DB::Open failed";
    db_ = db;

    {
      auto [tx, txerr] = db_->Begin(true);
      EXPECT_TRUE(txerr.OK());
      auto [bucket, berr] = tx->CreateBucket(kBucket);
      EXPECT_EQ(ErrorCode::OK, berr);
      for (const auto& [k, v] : items) {
        EXPECT_EQ(ErrorCode::OK, bucket->Put(k, v)) << "Put failed for " << k;
      }
      EXPECT_EQ(ErrorCode::OK, tx->Commit());
    }

    auto [rtx, rerr] = db_->Begin(false);
    EXPECT_TRUE(rerr.OK());
    return rtx;
  }

  std::shared_ptr<DB> db_;
  std::string path_;
  int fileIndex_ = 0;
};

// The exact reported bug: Prev() while the cursor is on the first key must
// return an empty key/value and must not read out of bounds. Under ASan the
// pre-fix code aborts here; without ASan it either aborts (std::length_error
// from resize(SIZE_MAX)) or silently corrupts memory and returns junk.
TEST_F(CursorPrevTest, PrevAtFirstReturnsEmpty) {
  auto tx = openWith({{"k000", "v000"}, {"k001", "v001"}, {"k002", "v002"}});
  auto bucket = tx->FindBucketByName(kBucket);
  auto cursor = bucket->NewCursor();

  auto kv = cursor->First();
  ASSERT_EQ(std::string(kv.key), "k000");
  ASSERT_EQ(std::string(kv.value), "v000");

  kv = cursor->Prev();  // <-- previously out-of-bounds read
  EXPECT_TRUE(kv.key.empty());
  EXPECT_TRUE(kv.value.empty());
  EXPECT_TRUE(cursor->stack.empty());  // cursor is now "before first"
}

// Stepping past the first key must be idempotent: further Prev() calls keep
// returning an empty key/value and the cursor stays "before first".
TEST_F(CursorPrevTest, PrevBeforeFirstIsIdempotent) {
  auto tx = openWith({{"k000", "v000"}, {"k001", "v001"}, {"k002", "v002"}});
  auto bucket = tx->FindBucketByName(kBucket);
  auto cursor = bucket->NewCursor();

  cursor->First();
  cursor->Prev();  // past first -> before first

  for (int i = 0; i < 3; ++i) {
    auto kv = cursor->Prev();
    EXPECT_TRUE(kv.key.empty());
    EXPECT_TRUE(kv.value.empty());
    EXPECT_TRUE(cursor->stack.empty());
  }
}

// After Prev() empties the stack, First() must still reposition the cursor
// onto the first key — i.e. no permanent corruption from the bad path.
TEST_F(CursorPrevTest, RepositionAfterPrevBeforeFirst) {
  auto tx = openWith({{"k000", "v000"}, {"k001", "v001"}, {"k002", "v002"}});
  auto bucket = tx->FindBucketByName(kBucket);
  auto cursor = bucket->NewCursor();

  cursor->First();
  auto past = cursor->Prev();
  ASSERT_TRUE(past.key.empty());

  auto kv = cursor->First();
  EXPECT_EQ(std::string(kv.key), "k000");
  EXPECT_EQ(std::string(kv.value), "v000");
}

// Full reverse scan: Last() then repeated Prev() must yield every key in
// descending order and finally an empty key/value past the first element.
TEST_F(CursorPrevTest, ReverseIterationFromLast) {
  const std::vector<std::pair<std::string, std::string>> items = {
      {"k000", "v000"}, {"k001", "v001"}, {"k002", "v002"},
      {"k003", "v003"}, {"k004", "v004"},
  };
  auto tx = openWith(items);
  auto bucket = tx->FindBucketByName(kBucket);
  auto cursor = bucket->NewCursor();

  std::vector<std::pair<std::string, std::string>> collected;
  for (auto kv = cursor->Last(); !kv.key.empty(); kv = cursor->Prev()) {
    collected.emplace_back(std::string(kv.key), std::string(kv.value));
  }
  ASSERT_EQ(items.size(), collected.size());
  for (size_t i = 0; i < items.size(); ++i) {
    EXPECT_EQ(items[items.size() - 1 - i].first, collected[i].first);
    EXPECT_EQ(items[items.size() - 1 - i].second, collected[i].second);
  }
  EXPECT_TRUE(cursor->stack.empty());
}

// Forward iteration must be unaffected (no regression in the sibling next()
// path or in First()/Next() contracts).
TEST_F(CursorPrevTest, ForwardNextNoRegression) {
  const std::vector<std::pair<std::string, std::string>> items = {
      {"k000", "v000"}, {"k001", "v001"}, {"k002", "v002"},
      {"k003", "v003"}, {"k004", "v004"},
  };
  auto tx = openWith(items);
  auto bucket = tx->FindBucketByName(kBucket);
  auto cursor = bucket->NewCursor();

  std::vector<std::pair<std::string, std::string>> collected;
  for (auto kv = cursor->First(); !kv.key.empty(); kv = cursor->Next()) {
    collected.emplace_back(std::string(kv.key), std::string(kv.value));
  }
  ASSERT_EQ(items.size(), collected.size());
  for (size_t i = 0; i < items.size(); ++i) {
    EXPECT_EQ(items[i].first, collected[i].first);
    EXPECT_EQ(items[i].second, collected[i].second);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
