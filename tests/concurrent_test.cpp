#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../bucket.h"
#include "../logger.h"
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <atomic>
#include <filesystem>

using namespace bboltpp;

class ConcurrentTest : public ::testing::Test {
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

// Test concurrent read and write operations
TEST_F(ConcurrentTest, TestConcurrentReadAndWrite) {
    std::cout << "Testing concurrent read and write operations..." << std::endl;
    
    auto [db, err] = DB::Open("test_concurrent.db", 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    // Create initial data
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test");
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    
    // Add some initial data
    for (int i = 0; i < 100; i++) {
        char key[16], value[32];
        snprintf(key, sizeof(key), "key%03d", i);
        snprintf(value, sizeof(value), "value%03d", i);
        bucket->Put(key, value);
    }
    
    tx->Commit();
    
    // Test concurrent operations
    const int numThreads = 10;
    const int operationsPerThread = 50;
    std::atomic<int> readCount(0);
    std::atomic<int> writeCount(0);
    std::atomic<bool> stopFlag(false);
    
    std::vector<std::thread> threads;
    
    // Start reader threads
    for (int t = 0; t < numThreads / 2; t++) {
        threads.emplace_back([&db, &readCount, &stopFlag, operationsPerThread]() {
            for (int i = 0; i < operationsPerThread && !stopFlag; i++) {
                auto [tx, err] = db->Begin(false);
                if (err.OK()) {
                    auto bucket = tx->FindBucketByName("test");
                    if (bucket) {
                        // Read a random key
                        char key[16];
                        snprintf(key, sizeof(key), "key%03d", i % 100);
                        auto value = bucket->Get(key);
                        if (value.has_value()) {
                            readCount++;
                        }
                    }
                    tx->Rollback();
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }
    
    // Start writer threads
    for (int t = 0; t < numThreads / 2; t++) {
        threads.emplace_back([&db, &writeCount, &stopFlag, operationsPerThread]() {
            for (int i = 0; i < operationsPerThread && !stopFlag; i++) {
                auto [tx, err] = db->Begin(true);
                if (err.OK()) {
                    auto bucket = tx->FindBucketByName("test");
                    if (bucket) {
                        // Write a random key
                        char key[16], value[32];
                        snprintf(key, sizeof(key), "key%03d", i % 100);
                        snprintf(value, sizeof(value), "new_value%03d", i);
                        auto putErr = bucket->Put(key, value);
                        if (putErr == ErrorCode::OK) {
                            tx->Commit();
                            writeCount++;
                        } else {
                            tx->Rollback();
                        }
                    } else {
                        tx->Rollback();
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "Read operations: " << readCount.load() << std::endl;
    std::cout << "Write operations: " << writeCount.load() << std::endl;
    
    // Verify data integrity
    auto [verifyTx, verifyErr] = db->Begin(false);
    EXPECT_TRUE(verifyErr.OK());
    
    auto verifyBucket = verifyTx->FindBucketByName("test");
    // verifyBucketErr not needed since FindBucketByName returns pointer
    EXPECT_NE(nullptr, verifyBucket);
    
    // Check that we can still read data
    auto testValue = verifyBucket->Get("key000");
    EXPECT_TRUE(testValue.has_value());
    
    verifyTx->Rollback();
    db->Close();
}

// Test concurrent transactions
TEST_F(ConcurrentTest, TestConcurrentTransactions) {
    std::cout << "Testing concurrent transactions..." << std::endl;
    
    auto [db, err] = DB::Open("test_concurrent_tx.db", 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    const int numThreads = 5;
    const int operationsPerThread = 20;
    std::atomic<int> successCount(0);
    
    std::vector<std::thread> threads;
    
    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back([&db, &successCount, operationsPerThread, t]() {
            for (int i = 0; i < operationsPerThread; i++) {
                auto [tx, err] = db->Begin(true);
                if (err.OK()) {
                    auto [bucket, bucketErr] = tx->CreateBucket("test");
                    if (bucketErr == ErrorCode::OK) {
                        char key[16], value[32];
                        snprintf(key, sizeof(key), "thread%d_key%03d", t, i);
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
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "Successful transactions: " << successCount.load() << std::endl;
    EXPECT_GT(successCount.load(), 0);
    
    // Verify data integrity
    auto [verifyTx, verifyErr] = db->Begin(false);
    EXPECT_TRUE(verifyErr.OK());
    
    auto verifyBucket = verifyTx->FindBucketByName("test");
    // verifyBucketErr not needed since FindBucketByName returns pointer
    EXPECT_NE(nullptr, verifyBucket);
    
    // Check some of the written data
    for (int t = 0; t < numThreads; t++) {
        char key[16];
        snprintf(key, sizeof(key), "thread%d_key000", t);
        auto value = verifyBucket->Get(key);
        if (value.has_value()) {
            std::cout << "Found key: " << key << " = " << value.value() << std::endl;
        }
    }
    
    verifyTx->Rollback();
    db->Close();
}

// Test concurrent bucket operations
TEST_F(ConcurrentTest, TestConcurrentBucketOperations) {
    std::cout << "Testing concurrent bucket operations..." << std::endl;
    
    auto [db, err] = DB::Open("test_concurrent_buckets.db", 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    const int numThreads = 3;
    const int bucketsPerThread = 5;
    std::atomic<int> successCount(0);
    
    std::vector<std::thread> threads;
    
    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back([&db, &successCount, bucketsPerThread, t]() {
            for (int i = 0; i < bucketsPerThread; i++) {
                auto [tx, err] = db->Begin(true);
                if (err.OK()) {
                    char bucketName[32];
                    snprintf(bucketName, sizeof(bucketName), "thread%d_bucket%03d", t, i);
                    
                    auto [bucket, bucketErr] = tx->CreateBucket(bucketName);
                    if (bucketErr == ErrorCode::OK) {
                        // Add some data to the bucket
                        for (int j = 0; j < 10; j++) {
                            char key[16], value[32];
                            snprintf(key, sizeof(key), "key%03d", j);
                            snprintf(value, sizeof(value), "value%03d", j);
                            bucket->Put(key, value);
                        }
                        
                        auto commitErr = tx->Commit();
                        if (commitErr == ErrorCode::OK) {
                            successCount++;
                        }
                    } else {
                        tx->Rollback();
                    }
                }
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "Successful bucket creations: " << successCount.load() << std::endl;
    EXPECT_GT(successCount.load(), 0);
    
    // Verify data integrity
    auto [verifyTx, verifyErr] = db->Begin(false);
    EXPECT_TRUE(verifyErr.OK());
    
    // Check some of the created buckets
    for (int t = 0; t < numThreads; t++) {
        char bucketName[32];
        snprintf(bucketName, sizeof(bucketName), "thread%d_bucket000", t);
        auto bucket = verifyTx->FindBucketByName(bucketName);
        if (bucket) {
            auto value = bucket->Get("key000");
            if (value.has_value()) {
                std::cout << "Found bucket: " << bucketName << " with key000 = " << value.value() << std::endl;
            }
        }
    }
    
    verifyTx->Rollback();
    db->Close();
}

// Test stress with many concurrent operations
TEST_F(ConcurrentTest, TestConcurrentStress) {
    std::cout << "Testing concurrent stress..." << std::endl;
    
    auto [db, err] = DB::Open("test_concurrent_stress.db", 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    const int numThreads = 20;
    const int operationsPerThread = 100;
    std::atomic<int> successCount(0);
    std::atomic<int> errorCount(0);
    
    std::vector<std::thread> threads;
    
    for (int t = 0; t < numThreads; t++) {
        threads.emplace_back([&db, &successCount, &errorCount, operationsPerThread, t]() {
            for (int i = 0; i < operationsPerThread; i++) {
                try {
                    auto [tx, err] = db->Begin(true);
                    if (err.OK()) {
                        auto [bucket, bucketErr] = tx->CreateBucket("stress_test");
                        if (bucketErr == ErrorCode::OK) {
                            char key[32], value[64];
                            snprintf(key, sizeof(key), "thread%d_op%03d", t, i);
                            snprintf(value, sizeof(value), "thread%d_value%03d", t, i);
                            
                            auto putErr = bucket->Put(key, value);
                            if (putErr == ErrorCode::OK) {
                                auto commitErr = tx->Commit();
                                if (commitErr == ErrorCode::OK) {
                                    successCount++;
                                } else {
                                    errorCount++;
                                }
                            } else {
                                errorCount++;
                                tx->Rollback();
                            }
                        } else {
                            errorCount++;
                            tx->Rollback();
                        }
                    } else {
                        errorCount++;
                    }
                } catch (...) {
                    errorCount++;
                }
                
                // Small delay to prevent overwhelming the system
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "Successful operations: " << successCount.load() << std::endl;
    std::cout << "Error operations: " << errorCount.load() << std::endl;
    
    // Verify data integrity
    auto [verifyTx, verifyErr] = db->Begin(false);
    EXPECT_TRUE(verifyErr.OK());
    
    auto verifyBucket = verifyTx->FindBucketByName("stress_test");
    // verifyBucketErr not needed since FindBucketByName returns pointer
    EXPECT_NE(nullptr, verifyBucket);
    
    // Check that we can read some data
    auto testValue = verifyBucket->Get("thread0_op000");
    if (testValue.has_value()) {
        std::cout << "Found test value: " << testValue.value() << std::endl;
    }
    
    verifyTx->Rollback();
    db->Close();
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
