#include <gtest/gtest.h>
#include "test_common.h"
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <vector>
#include <chrono>
#include <memory>
#include <cstring>

#include "../../db.h"
#include "../../logger.h"
#include "../../version.h"

using namespace bboltpp;

class DBTest {
protected:
    void SetUp() {
        cleanupTestFiles();
        // Initialize logger
        SetLogger(std::make_unique<DefaultLogger>(&std::cout));
        
        // Create temporary directory for test files
        test_dir_ = std::filesystem::temp_directory_path() / "bbolt_test";
        std::filesystem::create_directories(test_dir_);
    }
    
    void TearDown() {
        cleanupTestFiles();
        // Clean up test files
        if (std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }
    }
    
    std::string TempFile() {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_int_distribution<> dis(0, 15);
        
        std::string filename = "test_";
        for (int i = 0; i < 8; ++i) {
            filename += "0123456789abcdef"[dis(gen)];
        }
        filename += ".db";
        
        return (test_dir_ / filename).string();
    }
    
    std::filesystem::path test_dir_;
};

// Test basic database opening
TEST_F(DBTest, Open) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK()) << "Failed to open database: " << err.err_msg_;
    ASSERT_NE(db, nullptr) << "Expected non-null database";
    
    // Test database path (if we implement Path() method)
    // EXPECT_EQ(db->Path(), path);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK()) << "Failed to close database: " << closeErr.err_msg_;
}

// Test multiple threads opening database simultaneously
TEST_F(DBTest, OpenMultipleThreads) {
    if (testing::GTEST_FLAG(short_test)) {
        GTEST_SKIP() << "Skipping test in short mode";
    }
    
    const int instances = 30;
    const int iterations = 30;
    std::string path = TempFile();
    
    std::vector<std::thread> threads;
    std::vector<Error> errors;
    std::mutex errors_mutex;
    
    for (int iteration = 0; iteration < iterations; ++iteration) {
        for (int instance = 0; instance < instances; ++instance) {
            threads.emplace_back([&]() {
                auto [db, err] = DB::Open(path, 0600, nullptr);
                if (!err.OK()) {
                    std::lock_guard<std::mutex> lock(errors_mutex);
                    errors.push_back(err);
                    return;
                }
                
                auto closeErr = db->Close();
                if (!closeErr.OK()) {
                    std::lock_guard<std::mutex> lock(errors_mutex);
                    errors.push_back(closeErr);
                }
            });
        }
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_TRUE(errors.empty()) << "Expected no errors, got " << errors.size() << " errors";
}

// Test opening database with options
TEST_F(DBTest, OpenWithOptions) {
    std::string path = TempFile();
    
    Options options;
    options.Timeout = std::chrono::milliseconds(1000);
    options.ReadOnly = false;
    options.NoSync = true;
    
    auto [db, err] = DB::Open(path, 0600, &options);
    ASSERT_TRUE(err.OK()) << "Failed to open database with options: " << err.err_msg_;
    ASSERT_NE(db, nullptr);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test opening read-only database
TEST_F(DBTest, OpenReadOnly) {
    std::string path = TempFile();
    
    // First create a database
    {
        auto [db, err] = DB::Open(path, 0600, nullptr);
        ASSERT_TRUE(err.OK());
        auto closeErr = db->Close();
        ASSERT_TRUE(closeErr.OK());
    }
    
    // Now open it read-only
    Options options;
    options.ReadOnly = true;
    options.Timeout = std::chrono::milliseconds(1000);
    
    auto [db, err] = DB::Open(path, 0600, &options);
    ASSERT_TRUE(err.OK()) << "Failed to open read-only database: " << err.err_msg_;
    ASSERT_NE(db, nullptr);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test opening non-existent file in read-only mode
TEST_F(DBTest, OpenNonExistentReadOnly) {
    std::string path = TempFile();
    std::filesystem::remove(path); // Ensure file doesn't exist
    
    Options options;
    options.ReadOnly = true;
    options.Timeout = std::chrono::milliseconds(1000);
    
    auto [db, err] = DB::Open(path, 0600, &options);
    EXPECT_FALSE(err.OK()) << "Expected error when opening non-existent read-only file";
    EXPECT_EQ(db, nullptr);
}

// Test database statistics
TEST_F(DBTest, Stats) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    // Test basic stats
    auto stats = db->Stats();
    EXPECT_GE(stats.FreePageN, 0);
    EXPECT_GE(stats.PendingPageN, 0);
    EXPECT_GE(stats.FreeAlloc, 0);
    EXPECT_GE(stats.FreelistInuse, 0);
    EXPECT_GE(stats.TxN, 0);
    EXPECT_GE(stats.TxStats.PageCount, 0);
    EXPECT_GE(stats.TxStats.PageAlloc, 0);
    EXPECT_GE(stats.TxStats.CursorCount, 0);
    EXPECT_GE(stats.TxStats.NodeCount, 0);
    EXPECT_GE(stats.TxStats.NodeDeref, 0);
    EXPECT_GE(stats.TxStats.Rebalance, 0);
    EXPECT_GE(stats.TxStats.RebalanceTime, 0);
    EXPECT_GE(stats.TxStats.Split, 0);
    EXPECT_GE(stats.TxStats.Spill, 0);
    EXPECT_GE(stats.TxStats.SpillTime, 0);
    EXPECT_GE(stats.TxStats.Write, 0);
    EXPECT_GE(stats.TxStats.WriteTime, 0);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test database version
TEST_F(DBTest, Version) {
    std::string version = GetVersion();
    EXPECT_FALSE(version.empty()) << "Version should not be empty";
    EXPECT_NE(version.find("1.4.0"), std::string::npos) << "Version should contain 1.4.0";
}

// Test database with timeout
TEST_F(DBTest, OpenWithTimeout) {
    std::string path = TempFile();
    
    Options options;
    options.Timeout = std::chrono::milliseconds(100);
    
    auto [db, err] = DB::Open(path, 0600, &options);
    ASSERT_TRUE(err.OK()) << "Failed to open database with timeout: " << err.err_msg_;
    ASSERT_NE(db, nullptr);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test database file size
TEST_F(DBTest, FileSize) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    // Check that file exists and has reasonable size
    EXPECT_TRUE(std::filesystem::exists(path));
    auto file_size = std::filesystem::file_size(path);
    EXPECT_GT(file_size, 0) << "Database file should have non-zero size";
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test database with different page sizes
TEST_F(DBTest, DifferentPageSizes) {
    std::vector<int> page_sizes = {1024, 2048, 4096, 8192, 16384};
    
    for (int page_size : page_sizes) {
        std::string path = TempFile();
        
        Options options;
        options.PageSize = page_size;
        
        auto [db, err] = DB::Open(path, 0600, &options);
        ASSERT_TRUE(err.OK()) << "Failed to open database with page size " << page_size;
        ASSERT_NE(db, nullptr);
        
        auto closeErr = db->Close();
        EXPECT_TRUE(closeErr.OK());
    }
}

// Test database with freelist type
TEST_F(DBTest, DifferentFreelistTypes) {
    std::string path = TempFile();
    
    Options options;
    options.free_list_type_ = FreelistType::FreelistArrayType;
    
    auto [db, err] = DB::Open(path, 0600, &options);
    ASSERT_TRUE(err.OK()) << "Failed to open database with array freelist";
    ASSERT_NE(db, nullptr);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Test with hashmap freelist
    path = TempFile();
    options.free_list_type_ = FreelistType::FreelistHashMapType;
    
    auto [db2, err2] = DB::Open(path, 0600, &options);
    ASSERT_TRUE(err2.OK()) << "Failed to open database with hashmap freelist";
    ASSERT_NE(db2, nullptr);
    
    auto closeErr2 = db2->Close();
    EXPECT_TRUE(closeErr2.OK());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
