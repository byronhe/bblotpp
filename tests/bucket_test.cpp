#include <gtest/gtest.h>
#include "test_common.h"
#include <filesystem>
#include <fstream>
#include <random>
#include <vector>
#include <memory>
#include <cstring>

#include "../../db.h"
#include "../../bucket.h"
#include "../../logger.h"

using namespace bboltpp;

class BucketTest : public ::testing::Test {
protected:
    void SetUp() {
        cleanupTestFiles();
        // Initialize logger
        SetLogger(std::make_unique<DefaultLogger>(&std::cout));
        
        // Create temporary directory for test files
        test_dir_ = std::filesystem::temp_directory_path() / "bbolt_bucket_test";
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
        
        std::string filename = "bucket_test_";
        for (int i = 0; i < 8; ++i) {
            filename += "0123456789abcdef"[dis(gen)];
        }
        filename += ".db";
        
        return (test_dir_ / filename).string();
    }
    
    std::filesystem::path test_dir_;
};

// Test getting non-existent key returns empty
TEST_F(BucketTest, GetNonExistent) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("widgets");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    auto value = bucket->Get("foo");
    EXPECT_FALSE(value.has_value()) << "Expected empty value for non-existent key";
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test getting value from node before flush
TEST_F(BucketTest, GetFromNode) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("widgets");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    auto putErr = bucket->Put("foo", "bar");
    EXPECT_EQ(putErr, ErrorCode::OK);
    
    auto value = bucket->Get("foo");
    EXPECT_TRUE(value.has_value()) << "Expected value for key 'foo'";
    if (value.has_value()) {
        EXPECT_EQ(value.value(), "bar") << "Expected value 'bar' for key 'foo'";
    }
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test getting incompatible value returns empty
TEST_F(BucketTest, GetIncompatibleValue) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("widgets");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    // Create a sub-bucket with the same name as a key
    auto [subBucket, subBucketErr] = bucket->CreateBucket("foo");
    ASSERT_ERROR_CODE_OK(subBucketErr);
    
    // Getting "foo" as a key should return empty since it's a bucket
    auto value = bucket->Get("foo");
    EXPECT_FALSE(value.has_value()) << "Expected empty value for bucket name used as key";
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test basic put and get operations
TEST_F(BucketTest, PutAndGet) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    // Test multiple key-value pairs
    std::vector<std::pair<std::string, std::string>> test_data = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
        {"", "empty_key_value"},
        {"empty_value", ""},
        {"long_key_" + std::string(100, 'x'), "long_value_" + std::string(200, 'y')}
    };
    
    for (const auto& [key, value] : test_data) {
        auto putErr = bucket->Put(key, value);
        EXPECT_EQ(putErr, ErrorCode::OK) << "Failed to put key: " << key;
        
        auto getValue = bucket->Get(key);
        EXPECT_TRUE(getValue.has_value()) << "Failed to get key: " << key;
        if (getValue.has_value()) {
            EXPECT_EQ(getValue.value(), value) << "Value mismatch for key: " << key;
        }
    }
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test delete operations
TEST_F(BucketTest, Delete) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    // Put a key-value pair
    auto putErr = bucket->Put("key1", "value1");
    EXPECT_EQ(putErr, ErrorCode::OK);
    
    // Verify it exists
    auto value = bucket->Get("key1");
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(value.value(), "value1");
    
    // Delete the key
    auto delErr = bucket->Delete("key1");
    EXPECT_EQ(delErr, ErrorCode::OK);
    
    // Verify it's gone
    auto valueAfter = bucket->Get("key1");
    EXPECT_FALSE(valueAfter.has_value()) << "Key should be deleted";
    
    // Delete non-existent key should not error
    auto delNonExistentErr = bucket->Delete("non_existent");
    EXPECT_EQ(delNonExistentErr, ErrorCode::OK);
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test bucket creation and deletion
TEST_F(BucketTest, CreateAndDeleteBucket) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    // Create bucket
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    ASSERT_ERROR_CODE_OK(bucketErr);
    ASSERT_NE(bucket, nullptr);
    
    // Try to create same bucket again should fail
    auto [bucket2, bucketErr2] = tx->CreateBucket("test_bucket");
    EXPECT_NE(bucketErr2, ErrorCode::OK) << "Creating duplicate bucket should fail";
    
    // Create bucket if not exists should succeed
    auto [bucket3, bucketErr3] = tx->CreateBucketIfNotExists("test_bucket");
    EXPECT_EQ(bucketErr3, ErrorCode::OK) << "CreateBucketIfNotExists should succeed for existing bucket";
    
    // Create new bucket
    auto [bucket4, bucketErr4] = tx->CreateBucketIfNotExists("new_bucket");
    EXPECT_EQ(bucketErr4, ErrorCode::OK) << "CreateBucketIfNotExists should succeed for new bucket";
    
    // Delete bucket
    auto delBucketErr = tx->DeleteBucket("test_bucket");
    EXPECT_EQ(delBucketErr, ErrorCode::OK) << "Delete bucket should succeed";
    
    // Try to delete non-existent bucket should fail
    auto delNonExistentErr = tx->DeleteBucket("non_existent");
    EXPECT_NE(delNonExistentErr, ErrorCode::OK) << "Delete non-existent bucket should fail";
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test nested buckets
TEST_F(BucketTest, NestedBuckets) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    // Create root bucket
    auto [rootBucket, rootBucketErr] = tx->CreateBucket("root");
    ASSERT_ERROR_CODE_OK(rootBucketErr);
    
    // Create nested bucket
    auto [nestedBucket, nestedBucketErr] = rootBucket->CreateBucket("nested");
    ASSERT_ERROR_CODE_OK(nestedBucketErr);
    
    // Put data in nested bucket
    auto putErr = nestedBucket->Put("key", "value");
    EXPECT_EQ(putErr, ErrorCode::OK);
    
    // Get data from nested bucket
    auto value = nestedBucket->Get("key");
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(value.value(), "value");
    
    // Test bucket hierarchy
    auto foundBucket = rootBucket->FindBucketByName("nested");
    EXPECT_NE(foundBucket, nullptr) << "Should find nested bucket";
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test bucket statistics
TEST_F(BucketTest, BucketStats) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("stats_bucket");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    // Add some data
    for (int i = 0; i < 10; ++i) {
        auto putErr = bucket->Put("key" + std::to_string(i), "value" + std::to_string(i));
        EXPECT_EQ(putErr, ErrorCode::OK);
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
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test bucket iteration
TEST_F(BucketTest, BucketForEach) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("foreach_bucket");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
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
        EXPECT_EQ(putErr, ErrorCode::OK);
    }
    
    // Test ForEach
    std::vector<std::pair<std::string, std::string>> collected_data;
    auto forEachErr = bucket->ForEach([&](const std::string& key, const std::string& value) -> bool {
        collected_data.emplace_back(key, value);
        return true; // Continue iteration
    });
    
    EXPECT_EQ(forEachErr, ErrorCode::OK);
    EXPECT_EQ(collected_data.size(), test_data.size()) << "Should collect all data";
    
    // Sort collected data for comparison
    std::sort(collected_data.begin(), collected_data.end());
    std::sort(test_data.begin(), test_data.end());
    
    for (size_t i = 0; i < test_data.size(); ++i) {
        EXPECT_EQ(collected_data[i].first, test_data[i].first) << "Key mismatch at index " << i;
        EXPECT_EQ(collected_data[i].second, test_data[i].second) << "Value mismatch at index " << i;
    }
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
