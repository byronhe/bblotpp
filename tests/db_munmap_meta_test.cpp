#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../errors.h"
#include "../logger.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

using namespace bboltpp;

namespace {
constexpr int kTestPageSize = 4096;
}

// Regression coverage for the "dangling meta pointers after munmap" defect
// (mirrors Go bbolt's invalidate(), which resets meta0/meta1 to nil inside
// (*DB).munmap). The C++ port's DB::munmap previously cleared dataref/data/datasz
// but NOT meta0/meta1, so any error return inside DB::mmap() between the initial
// munmap() and the success-path meta reassignment (e.g. ::mlock ENOMEM when
// Mlock is on) left meta0/meta1 pointing into the unmapped region. The next
// GetMeta() (reached during Tx::rollback() -> hasSyncedFreelist()) then
// dereferenced freed memory.
class DBMunmapMetaTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
    }

    void TearDown() override {
        cleanupTestFiles();
    }

    std::shared_ptr<DB> openTestDB(const std::string& filename, Options* opts = nullptr) {
        auto [db, err] = DB::Open(filename, 0600, opts);
        EXPECT_TRUE(err.OK()) << "Open failed";
        EXPECT_NE(nullptr, db);
        return db;
    }
};

// Core guarantee of the fix: DB::munmap() must reset meta0/meta1 to nullptr
// alongside dataref/data/datasz. Before the fix these stayed pointing into the
// now-unmapped region (use-after-unmap). This test fails on the buggy code
// (meta0 != nullptr after munmap) and passes on the fixed code.
TEST_F(DBMunmapMetaTest, TestMunmap_ClearsMetaPointers) {
    const std::string dbFile = "test_munmap_clear_meta.db";
    Options opts;
    opts.PageSize = kTestPageSize;
    auto db = openTestDB(dbFile, &opts);
    ASSERT_NE(nullptr, db);

    ASSERT_FALSE(db->dataref.empty());
    ASSERT_NE(nullptr, db->meta0);
    ASSERT_NE(nullptr, db->meta1);

    auto merr = db->munmap();
    EXPECT_TRUE(merr.OK());

    EXPECT_EQ(nullptr, db->meta0);
    EXPECT_EQ(nullptr, db->meta1);
    EXPECT_TRUE(db->dataref.empty());
    EXPECT_TRUE(db->data.empty());
    EXPECT_EQ(0, db->datasz);

    auto cerr = db->Close();
    EXPECT_TRUE(cerr.OK());
    db.reset();
    std::filesystem::remove(dbFile);
}

// Regression: the success path of a remap (DB::mmap() called again) must still
// reassign meta0/meta1 after the initial munmap() nulls them. This verifies the
// fix did not break the happy path (mmap -> munmap -> remap -> valid metas).
TEST_F(DBMunmapMetaTest, TestMmap_RemapSuccess_ReassignsMetaPointers) {
    const std::string dbFile = "test_remap_success.db";
    Options opts;
    opts.PageSize = kTestPageSize;
    auto db = openTestDB(dbFile, &opts);
    ASSERT_NE(nullptr, db);
    ASSERT_NE(nullptr, db->meta0);
    ASSERT_NE(nullptr, db->meta1);

    auto merr = db->mmap(0);
    EXPECT_TRUE(merr.OK());

    EXPECT_NE(nullptr, db->meta0);
    EXPECT_NE(nullptr, db->meta1);
    EXPECT_FALSE(db->dataref.empty());
    EXPECT_GT(db->datasz, 0);
    EXPECT_EQ(ErrorCode::OK, db->meta0->validate());
    EXPECT_EQ(ErrorCode::OK, db->meta1->validate());

    auto cerr = db->Close();
    EXPECT_TRUE(cerr.OK());
    db.reset();
    std::filesystem::remove(dbFile);
}

// The headline bug scenario: an mmap *remap* whose ::mlock(fileSize) fails with
// ENOMEM (the realistic trigger when Mlock is on and the on-disk file grew past
// RLIMIT_MEMLOCK). DB::mmap() runs munmap() first (freeing the old mapping),
// then ::mmap() succeeds and dataref is recorded, then mlock() fails and mmap()
// returns BEFORE reassigning meta0/meta1.
//
// Before the fix: meta0/meta1 still pointed at the freed old mapping ->
// use-after-unmap on the subsequent GetMeta()/hasSyncedFreelist() in rollback.
// After the fix: munmap() nulled meta0/meta1, so the failed remap leaves them
// clean (nullptr) and the new mapping is tracked in dataref (not leaked).
//
// To make this deterministic in any environment (including ones with
// CAP_IPC_LOCK, where RLIMIT_MEMLOCK cannot force ::mlock to fail), the failing
// ::mlock is injected via the db.ops.mlockHook test seam (the same kind of
// indirection already used for Options.openFile and db.ops.writeAt). The hook
// defaults to the real ::mlock, so production behavior is unchanged.
TEST_F(DBMunmapMetaTest, TestMmap_MlockFailOnRemap_LeavesNullMetaNotDangling) {
    const std::string dbFile = "test_remap_mlock_fail.db";
    Options opts;
    opts.Mlock = true;
    opts.PageSize = kTestPageSize;

    auto [db, err] = DB::Open(dbFile, 0600, &opts);
    if (!err.OK()) {
        GTEST_SKIP() << "Open with Mlock failed in this environment "
                        "(RLIMIT_MEMLOCK too small for the initial file); "
                        "nothing to assert";
    }
    ASSERT_NE(nullptr, db);
    ASSERT_NE(nullptr, db->meta0);
    ASSERT_NE(nullptr, db->meta1);
    ASSERT_FALSE(db->dataref.empty());

    db->ops.mlockHook = [](const void* /*addr*/, size_t /*len*/) -> int {
        errno = ENOMEM;
        return -1;
    };

    auto merr = db->mmap(0);
    EXPECT_FALSE(merr.OK()) << "remap is expected to fail because mlock returns ENOMEM";

    EXPECT_EQ(nullptr, db->meta0);
    EXPECT_EQ(nullptr, db->meta1);

    EXPECT_FALSE(db->dataref.empty());
    EXPECT_FALSE(db->data.empty());
    EXPECT_GT(db->datasz, 0);

    auto cerr = db->Close();
    EXPECT_TRUE(cerr.OK()) << "close after a failed remap must not crash";
    db.reset();
    std::filesystem::remove(dbFile);
}

// Site B: madvise failure on a remap. DB::mmap() does munmap() (frees the old
// mapping, metas nulled), then ::mmap() creates a NEW region, then ::madvise()
// fails. Before the fix the new region was leaked (created but never tracked in
// dataref nor freed). After the fix the new region is ::munmap()'d before the
// error return. We detect the leak by counting mappings of the db file in
// /proc/self/maps: with the fix, after the failed remap there must be ZERO
// mappings of the db file (the initial Open mapping was freed by munmap(), and
// the new region was freed by the fix's ::munmap(ret,size)); without the fix the
// new region would remain (a leaked mapping). meta0/meta1 must be nullptr.
TEST_F(DBMunmapMetaTest, TestMmap_MadviseFailOnRemap_NoLeak) {
    const std::string dbFile = "test_remap_madvise_fail.db";
    Options opts;
    opts.PageSize = kTestPageSize;
    auto db = openTestDB(dbFile, &opts);
    ASSERT_NE(nullptr, db);
    ASSERT_NE(nullptr, db->meta0);
    ASSERT_NE(nullptr, db->meta1);
    ASSERT_FALSE(db->dataref.empty());

    auto countDbMappings = [&dbFile]() -> int {
        std::ifstream maps("/proc/self/maps");
        std::string line;
        int n = 0;
        std::string name = std::filesystem::path(dbFile).filename().string();
        while (std::getline(maps, line)) {
            if (line.find(name) != std::string::npos) ++n;
        }
        return n;
    };

    // Initial Open created exactly one mapping of the db file.
    ASSERT_EQ(1, countDbMappings()) << "Open should have one db-file mapping";

    // Inject a madvise that always fails with EINVAL (non-ENOSYS), so mmap()
    // takes the madvise-failure error path (site B) on the remap below.
    db->ops.madviseHook = [](void* /*addr*/, size_t /*len*/, int /*advice*/) -> int {
        errno = EINVAL;
        return -1;
    };

    auto merr = db->mmap(0);
    EXPECT_FALSE(merr.OK()) << "remap is expected to fail because madvise returns EINVAL";

    EXPECT_EQ(nullptr, db->meta0);
    EXPECT_EQ(nullptr, db->meta1);
    EXPECT_TRUE(db->dataref.empty()) << "madvise fails before dataref is assigned";

    // The fix ::munmap()'s the just-mapped region on the madvise-fail path, so
    // there must be no leaked db-file mapping. Without the fix this is >0.
    EXPECT_EQ(0, countDbMappings()) << "leaked mmap region on madvise-fail path";

    auto cerr = db->Close();
    EXPECT_TRUE(cerr.OK()) << "close after a failed remap must not crash";
    db.reset();
    std::filesystem::remove(dbFile);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
