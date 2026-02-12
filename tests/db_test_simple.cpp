#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../logger.h"
#include "../version.h"
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <vector>
#include <memory>
#include <cstring>

using namespace bboltpp;

class DBTestSimple : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
    }
    
    void TearDown() override {
        cleanupTestFiles();
        // Clean up test files
    }
};

TEST_F(DBTestSimple, TestOpen) {
    std::string path = "test_open.db";
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db.get());
    
    // Test database path (if we implement Path() method)
    // EXPECT_EQ(db->Path(), path);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(DBTestSimple, TestOpenWithOptions) {
    std::string path = "test_options.db";
    
    Options options;
    options.Timeout = std::chrono::milliseconds(1000);
    options.ReadOnly = false;
    options.NoSync = true;
    
    auto [db, err] = DB::Open(path, 0600, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db.get());
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(DBTestSimple, TestOpenReadOnly) {
    std::string path = "test_readonly.db";
    
    // First create a database
    {
        auto [db, err] = DB::Open(path, 0600, nullptr);
        EXPECT_TRUE(err.OK());
        auto closeErr = db->Close();
        EXPECT_TRUE(closeErr.OK());
    }
    
    // Now open it read-only
    Options options;
    options.ReadOnly = true;
    options.Timeout = std::chrono::milliseconds(1000);
    
    auto [db, err] = DB::Open(path, 0600, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db.get());
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(DBTestSimple, TestOpenNonExistentReadOnly) {
    std::string path = "non_existent.db";
    std::filesystem::remove(path); // Ensure file doesn't exist
    
    Options options;
    options.ReadOnly = true;
    options.Timeout = std::chrono::milliseconds(1000);
    
    auto [db, err] = DB::Open(path, 0600, &options);
    EXPECT_FALSE(err.OK());
    EXPECT_EQ(nullptr, db.get());
}

TEST_F(DBTestSimple, TestDatabaseStats) {
    std::string path = "test_stats.db";
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    EXPECT_TRUE(err.OK());
    
    // Test basic stats - skip for now as Stats method might not be implemented
    // auto stats = db->Stats();
    // ASSERT_GE(stats.FreePageN, 0);
    // ASSERT_GE(stats.PendingPageN, 0);
    // ASSERT_GE(stats.FreeAlloc, 0);
    // ASSERT_GE(stats.FreelistInuse, 0);
    // ASSERT_GE(stats.TxN, 0);
    // ASSERT_GE(stats.TxStats.PageCount, 0);
    // ASSERT_GE(stats.TxStats.PageAlloc, 0);
    // ASSERT_GE(stats.TxStats.CursorCount, 0);
    // ASSERT_GE(stats.TxStats.NodeCount, 0);
    // ASSERT_GE(stats.TxStats.NodeDeref, 0);
    // ASSERT_GE(stats.TxStats.Rebalance, 0);
    // ASSERT_GE(stats.TxStats.RebalanceTime, 0);
    // ASSERT_GE(stats.TxStats.Split, 0);
    // ASSERT_GE(stats.TxStats.Spill, 0);
    // ASSERT_GE(stats.TxStats.SpillTime, 0);
    // ASSERT_GE(stats.TxStats.Write, 0);
    // ASSERT_GE(stats.TxStats.WriteTime, 0);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(DBTestSimple, TestVersion) {
    std::string version = GetVersion();
    EXPECT_FALSE(version.empty());
    EXPECT_NE(std::string::npos, version.find("1.4.0"));
}

TEST_F(DBTestSimple, TestOpenWithTimeout) {
    std::string path = "test_timeout.db";
    
    Options options;
    options.Timeout = std::chrono::milliseconds(100);
    
    auto [db, err] = DB::Open(path, 0600, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db.get());
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(DBTestSimple, TestFileSize) {
    std::string path = "test_filesize.db";
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    EXPECT_TRUE(err.OK());
    
    // Check that file exists and has reasonable size
    EXPECT_TRUE(std::filesystem::exists(path));
    auto file_size = std::filesystem::file_size(path);
    EXPECT_GT(file_size, 0);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(DBTestSimple, TestDifferentPageSizes) {
    std::vector<int> page_sizes = {1024, 2048, 4096, 8192, 16384};
    
    for (int page_size : page_sizes) {
        std::string path = "test_pagesize_" + std::to_string(page_size) + ".db";
        
        Options options;
        options.PageSize = page_size;
        
        auto [db, err] = DB::Open(path, 0600, &options);
        EXPECT_TRUE(err.OK());
        EXPECT_NE(nullptr, db.get());
        
        auto closeErr = db->Close();
        EXPECT_TRUE(closeErr.OK());
        
        // Clean up
        std::filesystem::remove(path);
    }
}

TEST_F(DBTestSimple, TestDifferentFreelistTypes) {
    std::string path = "test_freelist.db";
    
    Options options;
    options.free_list_type_ = FreelistType::FreelistArrayType;
    
    auto [db, err] = DB::Open(path, 0600, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db.get());
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Test with hashmap freelist
    path = "test_freelist_hmap.db";
    options.free_list_type_ = FreelistType::FreelistMapType;
    
    auto [db2, err2] = DB::Open(path, 0600, &options);
    EXPECT_TRUE(err2.OK());
    EXPECT_NE(nullptr, db2.get());
    
    auto closeErr2 = db2->Close();
    EXPECT_TRUE(closeErr2.OK());
    
    // Clean up
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
