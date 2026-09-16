#include <gtest/gtest.h>
#include "../db.h"
#include "../tx.h"
#include "../page.h"
#include "../logger.h"
#include "test_common.h"
#include <filesystem>
#include <memory>
#include <string>

using namespace bboltpp;

class TxGetPageInfoTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cleanupTestFiles();
    SetLogger(std::make_unique<DefaultLogger>(&std::cout));
    test_dir_ = std::filesystem::temp_directory_path() / "bbolt_tx_getpageinfo_test";
    // Remove any stale dir left by a previously crashed run, then (re)create it.
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    cleanupTestFiles();
  }

  std::string TempFile() {
    return (test_dir_ / "getpageinfo_test.db").string();
  }

  std::filesystem::path test_dir_;
};

// Regression test for the false-positive double-free abort triggered by
// Tx::GetPageInfo. Tx::GetPageInfo queries the shared DB freelist via
// FreeList::freed() for every page it inspects. With the buggy
// `return cache[pgid];` (operator[]), a miss inserted a spurious {pgid:false}
// entry into the long-lived DB::freelist->cache. A subsequent Tx::Commit
// always frees the current meta->freelist page, so calling GetPageInfo on
// that same page id before Commit made the "already freed" guard abort()
// the process.
//
// With the fix, freed() is a read-only lookup and must not mutate the cache.
TEST_F(TxGetPageInfoTest, GetPageInfoOnFreelistPageDoesNotPoisonCacheThenCommitSucceeds) {
  std::string path = TempFile();

  auto [db, err] = DB::Open(path, 0600, nullptr);
  ASSERT_TRUE(err.OK()) << "Failed to open database: " << err.err_msg_;

  auto [tx, txErr] = db->Begin(true);
  ASSERT_TRUE(txErr.OK()) << "Failed to begin writable transaction: " << txErr.err_msg_;

  // The page id that this Tx::Commit will unconditionally free in default mode
  // (NoFreelistSync == false): the current freelist page.
  const pgid_t freelistPgid = tx->meta->freelist;
  ASSERT_NE(freelistPgid, pgidNoFreelist)
      << "Default-mode commit must free a real freelist page for this test to be meaningful";

  // Sanity: the freelist page itself is not recorded as free in the cache.
  ASSERT_EQ(db->freelist->cache.count(freelistPgid), 0u);

  // Exercise the public diagnostic API on the page that Commit will free.
  auto [info, infoErr] = tx->GetPageInfo(int(freelistPgid));
  ASSERT_EQ(infoErr, ErrorCode::OK);
  ASSERT_NE(info, nullptr);
  EXPECT_EQ(info->ID, int(freelistPgid));

  // FIX GUARANTEE: the read-only GetPageInfo call must not have poisoned the
  // shared cache with a {freelistPgid:false} entry. With the bug this count
  // would be 1 (and the stored value false).
  EXPECT_EQ(db->freelist->cache.count(freelistPgid), 0u)
      << "GetPageInfo must be read-only; cache must not contain a poisoned false entry";

  // The commit frees freelistPgid; with the bug the "already freed" guard
  // (cache.find(id) != end) would trip on the poisoned entry and abort().
  // With the fix this returns OK.
  auto commitErr = tx->Commit();
  EXPECT_EQ(commitErr, ErrorCode::OK)
      << "Commit after GetPageInfo on the freelist page must not abort";

  auto closeErr = db->Close();
  EXPECT_TRUE(closeErr.OK()) << "Failed to close database: " << closeErr.err_msg_;
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
