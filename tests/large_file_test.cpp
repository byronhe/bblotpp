#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "test_common.h"
#include "../db.h"
#include "../logger.h"

using namespace bboltpp;

namespace {

// Resize the file at `path` to exactly `new_size` bytes. Extension is sparse
// (ftruncate) and does not consume disk space. Returns true on success.
bool ResizeFile(const std::string& path, int64_t new_size) {
  int fd = ::open(path.c_str(), O_WRONLY);
  if (fd < 0) {
    return false;
  }
  int rc = ::ftruncate(fd, new_size);
  ::close(fd);
  return rc == 0;
}

// Returns the on-disk size in bytes of the file at `path` (or -1 on error).
int64_t FileSize(const std::string& path) {
  struct stat st {};
  if (::stat(path.c_str(), &st) != 0) {
    return -1;
  }
  return static_cast<int64_t>(st.st_size);
}

}  // namespace

// Regression tests for the "file size too small" guard in DB::mmap (db.cpp).
//
// The guard previously compared `int(info.Size()) < pageSize * 2`, which
// narrows the 64-bit `size_t` file size to a 32-bit signed `int`. For file sizes
// in periodic windows (the first being [2 GiB, 4 GiB + 8 KiB)) the narrowed
// value is < 8192, so valid externally-produced databases were rejected with a
// misleading "file size too small" error. The fix compares in a width-
// preserving type: `info.Size() < size_t(pageSize) * 2`.
class LargeFileTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cleanupTestFiles();
    bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
  }
  void TearDown() override { cleanupTestFiles(); }

  // Creates a small, valid database with a single bucket "b" holding key "k"
  // -> "v", then closes it. The resulting on-disk file is only a handful of
  // pages (well under 32 KB) so the real data lives in the first few KB.
  void CreateValidSmallDb(const std::string& path) {
    auto [db, err] = DB::Open(path, 0644, nullptr);
    ASSERT_TRUE(err.OK()) << "failed to create db: " << err.err_msg_;

    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    auto [bucket, bErr] = tx->CreateBucket("b");
    ASSERT_EQ(ErrorCode::OK, bErr);
    bucket->Put("k", "v");
    tx->Commit();
    db->Close();
  }

  // Re-opens an existing database file read-only.
  std::tuple<std::shared_ptr<DB>, Error> OpenReadOnly(const std::string& path) {
    Options opts;
    opts.ReadOnly = true;
    return DB::Open(path, 0644, &opts);
  }
};

// A valid database whose file size is in the middle of the first firing
// window (3 GiB) must open and its data must round-trip. Before the fix this
// returned "file size too small" because int(size_t(3GiB)) narrows to a
// negative 32-bit value. The existing test suite only exercises files < 2 GiB,
// where the narrowing is a no-op, so this case is the regression guard.
TEST_F(LargeFileTest, OpensValidFileInFiringWindowAndRoundTrips) {
  const std::string path = "test_large_3gib.db";
  CreateValidSmallDb(path);
  ASSERT_GT(FileSize(path), 0);

  constexpr int64_t k3GiB = 3LL * 1024 * 1024 * 1024;  // 3221225472
  ASSERT_TRUE(ResizeFile(path, k3GiB)) << "ftruncate failed";
  ASSERT_EQ(FileSize(path), k3GiB);

  auto [db, err] = OpenReadOnly(path);
  ASSERT_TRUE(err.OK()) << "3 GiB db should open, got: " << err.err_msg_;

  // The real data resides in the first few KB; it must remain readable.
  auto [tx, txErr] = db->Begin(false);
  ASSERT_TRUE(txErr.OK());
  auto bucket = tx->FindBucketByName("b");
  ASSERT_NE(nullptr, bucket);
  auto val = bucket->Get("k");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(std::string(val.value()), "v");
  tx->Rollback();
  db->Close();
  std::filesystem::remove(path);
}

// The guard must still reject a file strictly smaller than pageSize * 2
// (8191 bytes: one byte below the threshold). This guards against the fix
// accidentally loosening the under-size check; for files < 2 GiB the narrowing
// is a no-op, so the fix must preserve the original rejection behavior.
TEST_F(LargeFileTest, StillRejectsFileBelowTwoPageMinimum) {
  const std::string path = "test_large_8191.db";
  CreateValidSmallDb(path);
  ASSERT_GT(FileSize(path), 8191);

  ASSERT_TRUE(ResizeFile(path, 8191)) << "ftruncate failed";
  ASSERT_EQ(FileSize(path), 8191);

  auto [db, err] = OpenReadOnly(path);
  EXPECT_FALSE(err.OK()) << "8191-byte file should be rejected";
  EXPECT_NE(err.err_msg_.find("file size too small"), std::string::npos)
      << "expected 'file size too small', got: " << err.err_msg_;
  if (db) db->Close();
  std::filesystem::remove(path);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
