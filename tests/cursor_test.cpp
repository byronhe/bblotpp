#include <gtest/gtest.h>
#include "test_common.h"
#include <filesystem>
#include <fstream>
#include <random>
#include <vector>
#include <memory>
#include <algorithm>
#include <sstream>

#include "../../db.h"
#include "../../bucket.h"
#include "../../cursor.h"
#include "../../logger.h"

using namespace bboltpp;

class CursorTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        // Initialize logger
        SetLogger(std::make_unique<DefaultLogger>(&std::cout));
        
        // Create temporary directory for test files
        test_dir_ = std::filesystem::temp_directory_path() / "bbolt_cursor_test";
        std::filesystem::create_directories(test_dir_);
    }
    
    void TearDown() override {
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
        
        std::string filename = "cursor_test_";
        for (int i = 0; i < 8; ++i) {
            filename += "0123456789abcdef"[dis(gen)];
        }
        filename += ".db";
        
        return (test_dir_ / filename).string();
    }
    
    void PrepareData(Bucket* bucket) {
        // Ensure we have at least one branch page
        for (int i = 0; i < 1000; ++i) {
            std::string key = fmt::sprintf("%05d", i);
            auto putErr = bucket->Put(key, key);
            ASSERT_ERROR_CODE_OK(putErr);
        }
    }
    
    std::filesystem::path test_dir_;
};

// Test cursor basic operations
TEST_F(CursorTest, BasicOperations) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
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
    
    // Test cursor operations
    auto cursor = bucket->Cursor();
    ASSERT_NE(cursor, nullptr);
    
    // Test First
    auto [firstKey, firstValue] = cursor->First();
    EXPECT_TRUE(firstKey.has_value());
    EXPECT_TRUE(firstValue.has_value());
    if (firstKey.has_value() && firstValue.has_value()) {
        EXPECT_EQ(firstKey.value(), "a");
        EXPECT_EQ(firstValue.value(), "1");
    }
    
    // Test Next
    auto [nextKey, nextValue] = cursor->Next();
    EXPECT_TRUE(nextKey.has_value());
    EXPECT_TRUE(nextValue.has_value());
    if (nextKey.has_value() && nextValue.has_value()) {
        EXPECT_EQ(nextKey.value(), "b");
        EXPECT_EQ(nextValue.value(), "2");
    }
    
    // Test Last
    auto [lastKey, lastValue] = cursor->Last();
    EXPECT_TRUE(lastKey.has_value());
    EXPECT_TRUE(lastValue.has_value());
    if (lastKey.has_value() && lastValue.has_value()) {
        EXPECT_EQ(lastKey.value(), "e");
        EXPECT_EQ(lastValue.value(), "5");
    }
    
    // Test Prev
    auto [prevKey, prevValue] = cursor->Prev();
    EXPECT_TRUE(prevKey.has_value());
    EXPECT_TRUE(prevValue.has_value());
    if (prevKey.has_value() && prevValue.has_value()) {
        EXPECT_EQ(prevKey.value(), "d");
        EXPECT_EQ(prevValue.value(), "4");
    }
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test cursor seek operations
TEST_F(CursorTest, SeekOperations) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    // Add test data
    std::vector<std::string> keys = {"apple", "banana", "cherry", "date", "elderberry"};
    for (size_t i = 0; i < keys.size(); ++i) {
        auto putErr = bucket->Put(keys[i], "value" + std::to_string(i));
        EXPECT_EQ(putErr, ErrorCode::OK);
    }
    
    auto cursor = bucket->Cursor();
    ASSERT_NE(cursor, nullptr);
    
    // Test Seek to existing key
    auto [seekKey, seekValue] = cursor->Seek("cherry");
    EXPECT_TRUE(seekKey.has_value());
    EXPECT_TRUE(seekValue.has_value());
    if (seekKey.has_value() && seekValue.has_value()) {
        EXPECT_EQ(seekKey.value(), "cherry");
        EXPECT_EQ(seekValue.value(), "value2");
    }
    
    // Test Seek to non-existing key (should find next)
    auto [seekKey2, seekValue2] = cursor->Seek("coconut");
    EXPECT_TRUE(seekKey2.has_value());
    EXPECT_TRUE(seekValue2.has_value());
    if (seekKey2.has_value() && seekValue2.has_value()) {
        EXPECT_EQ(seekKey2.value(), "date");
        EXPECT_EQ(seekValue2.value(), "value3");
    }
    
    // Test Seek to key after all keys
    auto [seekKey3, seekValue3] = cursor->Seek("zebra");
    EXPECT_FALSE(seekKey3.has_value());
    EXPECT_FALSE(seekValue3.has_value());
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test cursor iteration
TEST_F(CursorTest, Iteration) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
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
    
    auto cursor = bucket->Cursor();
    ASSERT_NE(cursor, nullptr);
    
    // Test forward iteration
    std::vector<std::pair<std::string, std::string>> collected_data;
    auto [key, value] = cursor->First();
    while (key.has_value() && value.has_value()) {
        collected_data.emplace_back(key.value(), value.value());
        auto [nextKey, nextValue] = cursor->Next();
        key = nextKey;
        value = nextValue;
    }
    
    EXPECT_EQ(collected_data.size(), test_data.size());
    for (size_t i = 0; i < test_data.size(); ++i) {
        EXPECT_EQ(collected_data[i].first, test_data[i].first);
        EXPECT_EQ(collected_data[i].second, test_data[i].second);
    }
    
    // Test backward iteration
    collected_data.clear();
    auto [lastKey, lastValue] = cursor->Last();
    while (lastKey.has_value() && lastValue.has_value()) {
        collected_data.emplace_back(lastKey.value(), lastValue.value());
        auto [prevKey, prevValue] = cursor->Prev();
        lastKey = prevKey;
        lastValue = prevValue;
    }
    
    EXPECT_EQ(collected_data.size(), test_data.size());
    // Reverse the expected data for comparison
    std::reverse(test_data.begin(), test_data.end());
    for (size_t i = 0; i < test_data.size(); ++i) {
        EXPECT_EQ(collected_data[i].first, test_data[i].first);
        EXPECT_EQ(collected_data[i].second, test_data[i].second);
    }
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test cursor with large dataset
TEST_F(CursorTest, LargeDataset) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    // Add large dataset
    const int num_items = 1000;
    for (int i = 0; i < num_items; ++i) {
        std::string key = fmt::sprintf("%05d", i);
        auto putErr = bucket->Put(key, "value" + std::to_string(i));
        EXPECT_EQ(putErr, ErrorCode::OK);
    }
    
    auto cursor = bucket->Cursor();
    ASSERT_NE(cursor, nullptr);
    
    // Test forward iteration
    int count = 0;
    auto [key, value] = cursor->First();
    while (key.has_value() && value.has_value()) {
        count++;
        auto [nextKey, nextValue] = cursor->Next();
        key = nextKey;
        value = nextValue;
    }
    
    EXPECT_EQ(count, num_items) << "Should iterate over all items";
    
    // Test backward iteration
    count = 0;
    auto [lastKey, lastValue] = cursor->Last();
    while (lastKey.has_value() && lastValue.has_value()) {
        count++;
        auto [prevKey, prevValue] = cursor->Prev();
        lastKey = prevKey;
        lastValue = prevValue;
    }
    
    EXPECT_EQ(count, num_items) << "Should iterate over all items in reverse";
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test cursor with empty bucket
TEST_F(CursorTest, EmptyBucket) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    auto cursor = bucket->Cursor();
    ASSERT_NE(cursor, nullptr);
    
    // Test operations on empty bucket
    auto [firstKey, firstValue] = cursor->First();
    EXPECT_FALSE(firstKey.has_value());
    EXPECT_FALSE(firstValue.has_value());
    
    auto [lastKey, lastValue] = cursor->Last();
    EXPECT_FALSE(lastKey.has_value());
    EXPECT_FALSE(lastValue.has_value());
    
    auto [nextKey, nextValue] = cursor->Next();
    EXPECT_FALSE(nextKey.has_value());
    EXPECT_FALSE(nextValue.has_value());
    
    auto [prevKey, prevValue] = cursor->Prev();
    EXPECT_FALSE(prevKey.has_value());
    EXPECT_FALSE(prevValue.has_value());
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test cursor seek with exact match
TEST_F(CursorTest, SeekExactMatch) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    // Add test data
    std::vector<std::string> keys = {"apple", "banana", "cherry", "date", "elderberry"};
    for (size_t i = 0; i < keys.size(); ++i) {
        auto putErr = bucket->Put(keys[i], "value" + std::to_string(i));
        EXPECT_EQ(putErr, ErrorCode::OK);
    }
    
    auto cursor = bucket->Cursor();
    ASSERT_NE(cursor, nullptr);
    
    // Test seek to each key
    for (size_t i = 0; i < keys.size(); ++i) {
        auto [seekKey, seekValue] = cursor->Seek(keys[i]);
        EXPECT_TRUE(seekKey.has_value());
        EXPECT_TRUE(seekValue.has_value());
        if (seekKey.has_value() && seekValue.has_value()) {
            EXPECT_EQ(seekKey.value(), keys[i]);
            EXPECT_EQ(seekValue.value(), "value" + std::to_string(i));
        }
    }
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

// Test cursor with single item
TEST_F(CursorTest, SingleItem) {
    std::string path = TempFile();
    
    auto [db, err] = DB::Open(path, 0600, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    ASSERT_ERROR_CODE_OK(bucketErr);
    
    // Add single item
    auto putErr = bucket->Put("key", "value");
    EXPECT_EQ(putErr, ErrorCode::OK);
    
    auto cursor = bucket->Cursor();
    ASSERT_NE(cursor, nullptr);
    
    // Test operations with single item
    auto [firstKey, firstValue] = cursor->First();
    EXPECT_TRUE(firstKey.has_value());
    EXPECT_TRUE(firstValue.has_value());
    if (firstKey.has_value() && firstValue.has_value()) {
        EXPECT_EQ(firstKey.value(), "key");
        EXPECT_EQ(firstValue.value(), "value");
    }
    
    auto [lastKey, lastValue] = cursor->Last();
    EXPECT_TRUE(lastKey.has_value());
    EXPECT_TRUE(lastValue.has_value());
    if (lastKey.has_value() && lastValue.has_value()) {
        EXPECT_EQ(lastKey.value(), "key");
        EXPECT_EQ(lastValue.value(), "value");
    }
    
    // Next and Prev should return empty
    auto [nextKey, nextValue] = cursor->Next();
    EXPECT_FALSE(nextKey.has_value());
    EXPECT_FALSE(nextValue.has_value());
    
    auto [prevKey, prevValue] = cursor->Prev();
    EXPECT_FALSE(prevKey.has_value());
    EXPECT_FALSE(prevValue.has_value());
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(commitErr, ErrorCode::OK);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
