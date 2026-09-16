// Regression tests for the durability/grow bug in Tx::Commit.
//
// Background (boltdb/bolt issue #284):
//   On Linux the commit path flushes dirty pages and the meta page with
//   fdatasync(2), which flushes page data but NOT the file-size metadata
//   (i_size) after an extending pwrite. DB::grow() issues Truncate()+fsync()
//   to durably flush the file size *before* any page is written past the
//   previous EOF, so the new file size is durable.
//
//   The C++ port had moved the only grow() call (and its high-water-mark
//   baseline) into Tx::commitFreelist(), which (a) runs *after* spill and
//   (b) is skipped entirely when NoFreelistSync is set. As a result a growing
//   commit on the NoFreelistSync=true path never called grow() at all, and
//   spill-induced growth was invisible to the post-spill baseline on the
//   default path. A crash before the filesystem journal committed i_size
//   could revert the on-disk file size and corrupt the database.
//
//   The fix restores the upstream flow: capture the high-water mark (opgid)
//   *before* spill, and call DB::grow() in the Tx::Commit() body (for both
//   NoFreelistSync branches) immediately before write().
//
// Deterministic signal used by these tests:
//   DB::filesz (db.h) is assigned in exactly two places: DB::init() (db.cpp,
//   the very first pageSize*4 bytes) and DB::grow() (db.cpp). It is the
//   in-memory record of the on-disk file size that has been truncate+fsync'd.
//   Before the fix, on the NoFreelistSync=true path grow() was never called
//   during a commit, so filesz stayed pinned at pageSize*4 even as meta->pgid
//   grew far past it. After the fix, filesz is advanced by grow() to cover
//   the high-water mark on every growing commit, regardless of NoFreelistSync.
//
// Test shape notes:
//   - Each open performs growth as a SINGLE write transaction. This sidesteps
//     two pre-existing, UNRELATED bugs in FreeList (a release() that erases a
//     std::map node during iteration, and a freelist `ids` accumulation issue
//     in the default-options + long-reader combination) that would otherwise
//     crash a multi-commit reader-less session. A single Begin() per open
//     sees an empty pending map, so neither bug is reached. These freelist
//     bugs are not what is under test here.
//   - A large Options::InitialMmapSize keeps the mmap oversized so allocate()
//     never remaps during the growing commit (a remap would dangle node
//     string_views into the old mmap and crash spill/split), making growth
//     deterministic and crash-free.

#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <string>

#include "../bucket.h"
#include "../db.h"
#include "../logger.h"
#include "test_common.h"

using namespace bboltpp;

namespace {

// Oversized initial mmap so allocate() does not remap during the growing
// commit; growth therefore comes solely from the commit-time grow().
// 4 MiB gives headroom for thousands of pages before any remap would be needed.
constexpr int kInitialMmapSize = 1 << 22;  // 4 MiB

class DurabilityGrowTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cleanupTestFiles();
    bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
    test_dir_ = std::filesystem::temp_directory_path() / "bbolt_durability_grow_test";
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    cleanupTestFiles();
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  std::string TempFile() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    std::string filename = "durability_grow_";
    for (int i = 0; i < 8; ++i) {
      filename += "0123456789abcdef"[dis(gen)];
    }
    filename += ".db";
    return (test_dir_ / filename).string();
  }

  std::filesystem::path test_dir_;
};

std::shared_ptr<DB> openDb(const std::string& path, const Options* options) {
  auto [db, err] = DB::Open(path, 0600, options);
  EXPECT_TRUE(err.OK()) << "DB::Open failed";
  EXPECT_NE(nullptr, db);
  return db;
}

// Perform a SINGLE growing write transaction: create a bucket and insert nPuts
// keys with valSize-byte values, sized so the root leaf overflows exactly once
// and splits into SMALL nodes. This advances the high-water mark (meta->pgid)
// past the initial 4-page file so the commit-time grow() fires, while staying
// clear of a pre-existing Node::splitTwo bug that only triggers for large-node
// splits. One Begin() per open also keeps the freelist pending map empty at
// begin time (sidestepping an unrelated FreeList::release erase-during-map-
// iteration crash). Uses ASSERT_* (void helper) so a failure stops here.
//
// The reader-less, single-commit shape means these tests exercise the
// durability grow() path for BOTH NoFreelistSync branches without tripping any
// of the pre-existing, unrelated freelist/node bugs described above.
void growOnce(std::shared_ptr<DB>& db, const std::string& bucketName, int nPuts, int valSize) {
  auto [tx, txErr] = db->Begin(true);
  ASSERT_TRUE(txErr.OK());
  std::shared_ptr<Bucket> bucket;
  {
    auto [b, bErr] = tx->CreateBucketIfNotExists(bucketName);
    ASSERT_EQ(bErr, ErrorCode::OK);
    bucket = b;
  }
  for (int i = 0; i < nPuts; ++i) {
    std::string val(static_cast<size_t>(valSize), 'v');
    val += std::to_string(i);
    ASSERT_EQ(bucket->Put("k" + std::to_string(i), val), ErrorCode::OK);
  }
  ASSERT_EQ(tx->Commit(), ErrorCode::OK);
}

// Minimum durable on-disk file size required to cover the high-water mark: all
// referenced pages have ids in [0, meta->pgid), i.e. bytes [0, pgid*pageSize).
// grow() truncates to the (oversized) mmap size, so filesz must be >= this.
int64_t minDurableSize(DB& db) {
  return (int64_t(db.GetMeta()->pgid) + 1) * int64_t(db.pageSize);
}

// ===== Primary regression test (NoFreelistSync = true) ====================
// Before the fix: commitFreelist() is skipped, so no grow() is ever called on
// this branch and filesz stays at pageSize*4 while meta->pgid grows. After the
// fix: grow() runs in the Commit body and advances filesz to cover the
// high-water mark. The on-disk file is truncate+fsync'd to (>=) that size
// before write()'s fdatasync, so the new file size is durable.
TEST_F(DurabilityGrowTest, NoFreelistSyncGrowingCommitDurablyGrowsFileSize) {
  std::string path = TempFile();
  Options options;
  options.NoFreelistSync = true;          // skips commitFreelist() entirely
  options.NoGrowSync = false;             // required for grow()'s truncate+fsync
  options.InitialMmapSize = kInitialMmapSize;

  auto db = openDb(path, &options);
  ASSERT_TRUE(db);

  const int64_t initial_filesz = db->filesz;
  const pgid_t initial_pgid = db->GetMeta()->pgid;
  // A freshly-initialized DB has pageSize*4 bytes on disk (2 meta + freelist +
  // root leaf) and a high-water mark of 4.
  ASSERT_EQ(initial_filesz, int64_t(db->pageSize) * 4);
  ASSERT_EQ(initial_pgid, pgid_t(4));

  // A single growing commit advances the high-water mark well past the initial
  // file. The oversized mmap means allocate() does not remap during spill.
  growOnce(db, "widgets", 4, 1500);

  // Sanity: the transaction actually grew the high-water mark (proves we
  // exercised the bug path; if pgid did not move the test would be a no-op).
  ASSERT_GT(db->GetMeta()->pgid, initial_pgid);

  // Primary regression assertion: DB::grow() must have run during Commit and
  // advanced the durable file-size record to cover the high-water mark.
  // Before the fix filesz stayed at pageSize*4 and this FAILED.
  EXPECT_GT(int64_t(db->filesz), initial_filesz);
  EXPECT_GE(int64_t(db->filesz), minDurableSize(*db));

  // End-to-end: the on-disk file has been truncate+fsync'd by grow() to a size
  // that covers every page referenced by the committed meta. Before the fix the
  // on-disk file was only extended by pwrite to meta->pgid*pageSize (one page
  // short of the high-water mark) and never fsync'd as a size flush.
  std::error_code ec;
  auto on_disk = int64_t(std::filesystem::file_size(path, ec));
  ASSERT_FALSE(ec) << "stat failed: " << ec.message();
  EXPECT_GE(on_disk, minDurableSize(*db));

  // Data is intact and readable after the growing commit.
  {
    auto [tx, txErr] = db->Begin(false);
    ASSERT_TRUE(txErr.OK());
    auto bucket = tx->FindBucketByName("widgets");
    ASSERT_NE(nullptr, bucket);
    auto v = bucket->Get("k0");
    ASSERT_TRUE(v.has_value());
    EXPECT_FALSE(v->empty());
    ASSERT_EQ(tx->Rollback(), ErrorCode::OK);
  }

  ASSERT_TRUE(db->Close().OK());
}

// ===== Default options (NoFreelistSync = false) ==========================
// Confirms the default/commitFreelist path still grows durably and is not
// regressed by moving grow() out of commitFreelist() into the Commit body.
TEST_F(DurabilityGrowTest, DefaultOptionsGrowingCommitDurablyGrowsFileSize) {
  std::string path = TempFile();
  Options options;                       // NoFreelistSync=false, NoGrowSync=false
  options.InitialMmapSize = kInitialMmapSize;
  auto db = openDb(path, &options);
  ASSERT_TRUE(db);

  const int64_t initial_filesz = db->filesz;
  ASSERT_EQ(initial_filesz, int64_t(db->pageSize) * 4);

  growOnce(db, "widgets", 4, 1500);

  ASSERT_GT(db->GetMeta()->pgid, pgid_t(4));
  EXPECT_GT(int64_t(db->filesz), initial_filesz);
  EXPECT_GE(int64_t(db->filesz), minDurableSize(*db));

  std::error_code ec;
  auto on_disk = int64_t(std::filesystem::file_size(path, ec));
  ASSERT_FALSE(ec);
  EXPECT_GE(on_disk, minDurableSize(*db));

  ASSERT_TRUE(db->Close().OK());
}

// ===== Non-growing commit must NOT call grow() spuriously ================
// Guards the restored `if (meta->pgid > opgid)` condition: a commit that does
// not advance the high-water mark must not truncate+fsync. After reopen of an
// existing file filesz is 0 (init() is not re-run), and an empty commit (no
// writes) must leave filesz at 0 (grow()'s guard is false). pending is empty on
// reopen of a NoFreelistSync DB (freelist rebuilt from a scan), so the empty
// commit does not reach the pre-existing FreeList::release bug.
TEST_F(DurabilityGrowTest, NoFreelistSyncNonGrowingCommitDoesNotGrow) {
  std::string path = TempFile();
  Options options;
  options.NoFreelistSync = true;
  options.NoGrowSync = false;
  options.InitialMmapSize = kInitialMmapSize;

  // Establish a durable size with one growing commit, then close.
  {
    auto db = openDb(path, &options);
    ASSERT_TRUE(db);
    growOnce(db, "widgets", 4, 1500);
    ASSERT_GT(db->GetMeta()->pgid, pgid_t(4));
    ASSERT_GT(int64_t(db->filesz), int64_t(db->pageSize) * 4);
    ASSERT_TRUE(db->Close().OK());
  }

  // Reopen: filesz resets to 0 (init() not re-run on existing file).
  auto db2 = openDb(path, &options);
  ASSERT_TRUE(db2);
  ASSERT_EQ(db2->filesz, 0);
  const pgid_t pgid_before_empty = db2->GetMeta()->pgid;

  // An empty write transaction: no allocations, no spill growth. The Commit
  // body's `meta->pgid > opgid` guard must be false -> grow() must NOT run.
  {
    auto [tx, txErr] = db2->Begin(true);
    ASSERT_TRUE(txErr.OK());
    ASSERT_EQ(tx->Commit(), ErrorCode::OK);
  }

  EXPECT_EQ(db2->GetMeta()->pgid, pgid_before_empty)
      << "empty commit must not advance the high-water mark";
  EXPECT_EQ(db2->filesz, 0)
      << "non-growing commit must not invoke DB::grow()";

  ASSERT_TRUE(db2->Close().OK());
}

// ===== End-to-end: durable size persists across close/reopen ==============
// After reopen, the in-memory filesz resets to 0 (init() is not re-run), but
// the on-disk file size that grow()'s Truncate()+fsync() persisted must still
// cover the high-water mark. This is the crux of the fix: on the NoFreelistSync
// path the un-fixed commit never grow()ed, so the on-disk file was only the
// pwrite extent (meta->pgid*pageSize, one page short of the high-water mark)
// and never fsync'd as a size flush -- after reopen the page at the high-water
// mark would be missing.
TEST_F(DurabilityGrowTest, NoFreelistSyncGrowPersistsAcrossReopen) {
  std::string path = TempFile();
  Options options;
  options.NoFreelistSync = true;
  options.NoGrowSync = false;
  options.InitialMmapSize = kInitialMmapSize;

  {
    auto db = openDb(path, &options);
    ASSERT_TRUE(db);
    growOnce(db, "widgets", 4, 1500);
    ASSERT_GT(db->GetMeta()->pgid, pgid_t(4));
    EXPECT_GE(int64_t(db->filesz), minDurableSize(*db));  // grow() fired in tx
    ASSERT_TRUE(db->Close().OK());
  }

  // Reopen: the durable file size from grow()'s truncate+fsync must persist and
  // still cover the high-water mark. The reader-less Begin here is safe because
  // a NoFreelistSync reopen rebuilds the freelist from a scan (pending empty).
  auto db = openDb(path, &options);
  ASSERT_TRUE(db);
  ASSERT_GT(db->GetMeta()->pgid, pgid_t(4));  // meta carried over
  ASSERT_EQ(db->filesz, 0);                  // in-memory record resets on reopen

  std::error_code ec;
  auto on_disk = int64_t(std::filesystem::file_size(path, ec));
  ASSERT_FALSE(ec) << "stat failed: " << ec.message();
  // Primary durability-persistence assertion: the durable on-disk file size
  // (written by grow()'s Truncate()+fsync()) survives close/reopen and covers
  // every page referenced by the committed meta. Before the fix this FAILED
  // (on-disk was only meta->pgid*pageSize, one page short of the high-water
  // mark, and never fsync'd as a size operation).
  EXPECT_GE(on_disk, minDurableSize(*db));

  // Previously committed data is intact and readable.
  {
    auto [tx, txErr] = db->Begin(false);
    ASSERT_TRUE(txErr.OK());
    auto bucket = tx->FindBucketByName("widgets");
    ASSERT_NE(nullptr, bucket);
    auto v = bucket->Get("k0");
    ASSERT_TRUE(v.has_value());
    EXPECT_FALSE(v->empty());
    ASSERT_EQ(tx->Rollback(), ErrorCode::OK);
  }

  ASSERT_TRUE(db->Close().OK());
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
