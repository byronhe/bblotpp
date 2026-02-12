#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../bucket.h"
#include "../logger.h"
#include "../utils.h"
#include <iostream>
#include <memory>
#include <random>
#include <thread>
#include <vector>
#include <filesystem>

using namespace bboltpp;

class ManyDbsTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
    }
    
    void TearDown() override {
        cleanupTestFiles();
        // Clean up test files
        for (int i = 0; i < 20; i++) {
            char filename[64];
            snprintf(filename, sizeof(filename), "test_manydbs_%03d.db", i);
            std::filesystem::remove(filename);
        }
    }
};

// Test creating and using many databases
TEST_F(ManyDbsTest, TestManyDatabases) {
    std::cout << "Testing many databases..." << std::endl;
    
    const int numDatabases = 10;
    const int keysPerDatabase = 100;
    std::vector<std::string> dbFiles;
    
    // Create multiple databases
    for (int i = 0; i < numDatabases; i++) {
        char filename[64];
        snprintf(filename, sizeof(filename), "test_manydbs_%03d.db", i);
        dbFiles.push_back(filename);
        
        auto [db, err] = DB::Open(filename, 0644, nullptr);
        EXPECT_TRUE(err.OK());
        
        // Add some data to each database
        auto [tx, txErr] = db->Begin(true);
        EXPECT_TRUE(txErr.OK());
        
        auto [bucket, bucketErr] = tx->CreateBucket("bucket");
        EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        
        for (int j = 0; j < keysPerDatabase; j++) {
            char key[16], value[32];
            snprintf(key, sizeof(key), "key%03d", j);
            snprintf(value, sizeof(value), "value%03d_%03d", i, j);
            bucket->Put(key, value);
        }
        
        tx->Commit();
        db->Close();
    }
    
    // Verify all databases
    for (int i = 0; i < numDatabases; i++) {
        auto [db, err] = DB::Open(dbFiles[i], 0644, nullptr);
        EXPECT_TRUE(err.OK());
        
        auto [tx, txErr] = db->Begin(false);
        EXPECT_TRUE(txErr.OK());
        
        auto bucket = tx->FindBucketByName("bucket");
        // bucketErr not needed since FindBucketByName returns pointer
        EXPECT_NE(nullptr, bucket);
        
        // Check some keys
        for (int j = 0; j < 10; j++) {
            char key[16], expectedValue[32];
            snprintf(key, sizeof(key), "key%03d", j);
            snprintf(expectedValue, sizeof(expectedValue), "value%03d_%03d", i, j);
            
            auto value = bucket->Get(key);
            EXPECT_TRUE(value.has_value());
            EXPECT_STREQ(std::string(value.value()).c_str(), expectedValue);
        }
        
        tx->Rollback();
        db->Close();
    }
    
    // Cleanup
    for (const auto& filename : dbFiles) {
        std::filesystem::remove(filename);
    }
}

// Test concurrent access to multiple databases
TEST_F(ManyDbsTest, TestConcurrentManyDatabases) {
    std::cout << "Testing concurrent access to many databases..." << std::endl;
    
    const int numDatabases = 5;
    const int numThreads = 10;
    const int operationsPerThread = 20;
    std::vector<std::string> dbFiles;
    
    // Create databases
    for (int i = 0; i < numDatabases; i++) {
        char filename[64];
        snprintf(filename, sizeof(filename), "test_concurrent_manydbs_%03d.db", i);
        dbFiles.push_back(filename);
        
        auto [db, err] = DB::Open(filename, 0644, nullptr);
        EXPECT_TRUE(err.OK());
        db->Close();
    }
    
    std::atomic<int> successCount(0);
    std::vector<std::thread> threads;
    
    // Start worker threads
    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back([&dbFiles, &successCount, operationsPerThread, t]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dbDist(0, dbFiles.size() - 1);
            
            for (int i = 0; i < operationsPerThread; i++) {
                int dbIndex = dbDist(gen);
                const std::string& filename = dbFiles[dbIndex];
                
                auto [db, err] = DB::Open(filename, 0644, nullptr);
                if (err.OK()) {
                    auto [tx, txErr] = db->Begin(true);
                    if (txErr.OK()) {
                        auto [bucket, bucketErr] = tx->CreateBucket("concurrent_test");
                        if (bucketErr == ErrorCode::OK) {
                            char key[32], value[64];
                            snprintf(key, sizeof(key), "thread%d_op%03d", t, i);
                            snprintf(value, sizeof(value), "thread%d_value%03d", t, i);
                            
                            auto putErr = bucket->Put(key, value);
                            if (putErr == ErrorCode::OK) {
                                auto commitErr = tx->Commit();
                                if (commitErr == ErrorCode::OK) {
                                    successCount++;
                                }
                            } else {
                                tx->Rollback();
                            }
                        } else {
                            tx->Rollback();
                        }
                    }
                    db->Close();
                }
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "Successful operations: " << successCount.load() << std::endl;
    EXPECT_GT(successCount.load(), 0);
    
    // Verify data in all databases
    for (int i = 0; i < numDatabases; i++) {
        auto [db, err] = DB::Open(dbFiles[i], 0644, nullptr);
        EXPECT_TRUE(err.OK());
        
        auto [tx, txErr] = db->Begin(false);
        EXPECT_TRUE(txErr.OK());
        
        auto bucket = tx->FindBucketByName("concurrent_test");
        if (bucket) {
            // Check that we can read some data
            auto value = bucket->Get("thread0_op000");
            if (value.has_value()) {
                std::cout << "Found value in database " << i << ": " << value.value() << std::endl;
            }
        }
        
        tx->Rollback();
        db->Close();
    }
    
    // Cleanup
    for (const auto& filename : dbFiles) {
        std::filesystem::remove(filename);
    }
}

// Test database file size with many databases
TEST_F(ManyDbsTest, TestManyDatabasesFileSize) {
    std::cout << "Testing database file size with many databases..." << std::endl;
    
    const int numDatabases = 5;
    std::vector<std::string> dbFiles;
    std::vector<size_t> fileSizes;
    
    // Create databases with different amounts of data
    for (int i = 0; i < numDatabases; i++) {
        char filename[64];
        snprintf(filename, sizeof(filename), "test_size_manydbs_%03d.db", i);
        dbFiles.push_back(filename);
        
        auto [db, err] = DB::Open(filename, 0644, nullptr);
        EXPECT_TRUE(err.OK());
        
        auto [tx, txErr] = db->Begin(true);
        EXPECT_TRUE(txErr.OK());
        
        auto [bucket, bucketErr] = tx->CreateBucket("size_test");
        EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        
        // Add different amounts of data to each database
        int keysToAdd = (i + 1) * 50; // 50, 100, 150, 200, 250 keys
        for (int j = 0; j < keysToAdd; j++) {
            char key[16], value[128];
            snprintf(key, sizeof(key), "key%03d", j);
            snprintf(value, sizeof(value), "value%03d_%03d_with_more_data_to_make_it_larger", i, j);
            bucket->Put(key, value);
        }
        
        tx->Commit();
        db->Close();
        
        // Get file size
        auto [size, sizeErr] = GetFileSize(filename);
        fileSizes.push_back(size);
        std::cout << "Database " << i << " size: " << size << " bytes" << std::endl;
    }
    
    // Verify file sizes are reasonable and increasing
    for (size_t i = 1; i < fileSizes.size(); i++) {
        EXPECT_GT(fileSizes[i], fileSizes[i-1]);
    }
    
    // Cleanup
    for (const auto& filename : dbFiles) {
        std::filesystem::remove(filename);
    }
}

// Test database operations with different page sizes
TEST_F(ManyDbsTest, TestManyDatabasesDifferentPageSizes) {
    std::cout << "Testing many databases with different page sizes..." << std::endl;
    
    std::vector<int> pageSizes = {1024, 2048, 4096, 8192, 16384};
    std::vector<std::string> dbFiles;
    
    for (size_t i = 0; i < pageSizes.size(); i++) {
        char filename[64];
        snprintf(filename, sizeof(filename), "test_pagesize_%d.db", pageSizes[i]);
        dbFiles.push_back(filename);
        
        Options options;
        options.PageSize = pageSizes[i];
        
        auto [db, err] = DB::Open(filename, 0644, &options);
        EXPECT_TRUE(err.OK());
        
        auto [tx, txErr] = db->Begin(true);
        EXPECT_TRUE(txErr.OK());
        
        auto [bucket, bucketErr] = tx->CreateBucket("pagesize_test");
        EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        
        // Add some data
        for (int j = 0; j < 100; j++) {
            char key[16], value[64];
            snprintf(key, sizeof(key), "key%03d", j);
            snprintf(value, sizeof(value), "value%03d_pagesize_%d", j, pageSizes[i]);
            bucket->Put(key, value);
        }
        
        tx->Commit();
        db->Close();
        
        // Verify the database
        auto [verifyDb, verifyErr] = DB::Open(filename, 0644, &options);
        EXPECT_TRUE(verifyErr.OK());
        
        auto [verifyTx, verifyTxErr] = verifyDb->Begin(false);
        EXPECT_TRUE(verifyTxErr.OK());
        
        auto verifyBucket = verifyTx->FindBucketByName("pagesize_test");
        EXPECT_NE(nullptr, verifyBucket);
        
        auto value = verifyBucket->Get("key000");
        EXPECT_TRUE(value.has_value());
        
        verifyTx->Rollback();
        verifyDb->Close();
    }
    
    // Cleanup
    for (const auto& filename : dbFiles) {
        std::filesystem::remove(filename);
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
