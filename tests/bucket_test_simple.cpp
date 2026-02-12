#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../bucket.h"
#include "../logger.h"
#include <filesystem>
#include <fstream>
#include <random>
#include <vector>
#include <memory>
#include <cstring>

using namespace bboltpp;

class BucketTestSimple : public ::testing::Test {
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

TEST_F(BucketTestSimple, TestCreateBucket) {
    std::string path = "test_create_bucket.db";
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    EXPECT_NE(nullptr, bucket);
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(BucketTestSimple, TestGetBucket) {
    std::string path = "test_get_bucket.db";
    
    // First create a bucket
    {
        auto [db, err] = DB::Open(path, 0600, nullptr);
        EXPECT_TRUE(err.OK());
        
        auto [tx, txErr] = db->Begin(true);
        EXPECT_TRUE(txErr.OK());
        
        auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
        EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
        
        auto closeErr = db->Close();
        EXPECT_TRUE(closeErr.OK());
    }
    
    // Now try to get the bucket
    auto [db, err] = DB::Open(path, 0600, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(false);
    EXPECT_TRUE(txErr.OK());
    
    auto bucket = tx->FindBucketByName("test_bucket");
    EXPECT_NE(nullptr, bucket);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(BucketTestSimple, TestPutAndGet) {
    std::string path = "test_put_get.db";
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    
    // Put a key-value pair
    auto putErr = bucket->Put("key1", "value1");
    EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
    
    // Get the value
    auto value = bucket->Get("key1");
    EXPECT_TRUE(value.has_value());
    EXPECT_STREQ("value1", std::string(value.value()).c_str());
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(BucketTestSimple, TestDelete) {
    std::string path = "test_delete.db";
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    
    // Put a key-value pair
    auto putErr = bucket->Put("key1", "value1");
    EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
    
    // Verify it exists
    auto value = bucket->Get("key1");
    EXPECT_TRUE(value.has_value());
    
    // Delete the key
    auto delErr = bucket->Delete("key1");
    EXPECT_EQ(bboltpp::ErrorCode::OK, delErr);
    
    // Verify it's gone
    auto valueAfter = bucket->Get("key1");
    EXPECT_FALSE(valueAfter.has_value());
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(BucketTestSimple, TestBucketStats) {
    std::string path = "test_bucket_stats.db";
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("stats_bucket");
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    
    // Add some data
    for (int i = 0; i < 10; ++i) {
        auto putErr = bucket->Put("key" + std::to_string(i), "value" + std::to_string(i));
        EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
    }
    
    // Get bucket stats
    auto stats = bucket->Stats();
    EXPECT_GE(stats.BranchPageN, 0);
    EXPECT_GE(stats.BranchOverflowN, 0);
    EXPECT_GE(stats.LeafPageN, 0);
    EXPECT_GE(stats.LeafOverflowN, 0);
    EXPECT_GE(stats.KeyN, 0);
    EXPECT_GE(stats.Depth, 0);
    EXPECT_GE(stats.BranchInuse, 0);
    EXPECT_GE(stats.LeafInuse, 0);
    EXPECT_GE(stats.BranchAlloc, 0);
    EXPECT_GE(stats.LeafAlloc, 0);
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(BucketTestSimple, TestBucketForEach) {
    std::string path = "test_bucket_foreach.db";
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("foreach_bucket");
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    
    // Add test data
    std::vector<std::pair<std::string, std::string>> test_data = {
        {"a", "1"},
        {"b", "2"},
        {"c", "3"},
        {"d", "4"},
        {"e", "5"}
    };
    
    for (const auto& [key, value] : test_data) {
        auto putErr = bucket->Put(key, value);
        EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
    }
    
    // Test ForEach
    std::vector<std::pair<std::string, std::string>> collected_data;
    auto forEachErr = bucket->ForEach([&](std::string_view key, std::string_view value) -> ErrorCode {
        collected_data.emplace_back(std::string(key), std::string(value));
        return ErrorCode::OK; // Continue iteration
    });
    
    EXPECT_EQ(bboltpp::ErrorCode::OK, forEachErr);
    EXPECT_EQ(test_data.size(), collected_data.size());
    
    // Sort collected data for comparison
    std::sort(collected_data.begin(), collected_data.end());
    std::sort(test_data.begin(), test_data.end());
    
    for (size_t i = 0; i < test_data.size(); ++i) {
        EXPECT_STREQ(test_data[i].first.c_str(), collected_data[i].first.c_str());
        EXPECT_STREQ(test_data[i].second.c_str(), collected_data[i].second.c_str());
    }
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

TEST_F(BucketTestSimple, TestNestedBuckets) {
    std::string path = "test_nested_buckets.db";
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    // Create root bucket
    auto [rootBucket, rootBucketErr] = tx->CreateBucket("root");
    EXPECT_EQ(bboltpp::ErrorCode::OK, rootBucketErr);
    
    // Create nested bucket
    auto [nestedBucket, nestedBucketErr] = rootBucket->CreateBucket("nested");
    EXPECT_EQ(bboltpp::ErrorCode::OK, nestedBucketErr);
    
    // Put data in nested bucket
    auto putErr = nestedBucket->Put("key", "value");
    EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
    
    // Get data from nested bucket
    auto value = nestedBucket->Get("key");
    EXPECT_TRUE(value.has_value());
    EXPECT_STREQ("value", std::string(value.value()).c_str());
    
    // Test bucket hierarchy
    auto foundBucket = rootBucket->FindBucketByName("nested");
    EXPECT_NE(nullptr, foundBucket);
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
    
    // Clean up
    std::filesystem::remove(path);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
