#include <gtest/gtest.h>
#include "../db.h"
#include "../bucket.h"
#include "../logger.h"
#include "test_common.h"
#include <iostream>
#include <memory>
#include <filesystem>

using namespace bboltpp;

class BucketTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        SetLogger(std::make_unique<DefaultLogger>(&std::cout));
    }
    
    void TearDown() override {
        cleanupTestFiles();
    }
};

TEST_F(BucketTest, CreateBucket) {
    auto [db, err] = DB::Open("test_bucket.db", 0644, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    EXPECT_NE(nullptr, bucket);
    
    if (bucketErr == bboltpp::ErrorCode::OK) {
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    }
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

TEST_F(BucketTest, GetBucket) {
    // First create a bucket
    {
        auto [db, err] = DB::Open("test_get_bucket.db", 0644, nullptr);
        ASSERT_TRUE(err.OK());
        
        auto [tx, txErr] = db->Begin(true);
        ASSERT_TRUE(txErr.OK());
        
        auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
        ASSERT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        
        auto commitErr = tx->Commit();
        ASSERT_EQ(bboltpp::ErrorCode::OK, commitErr);
        
        auto closeErr = db->Close();
        ASSERT_TRUE(closeErr.OK());
    }
    
    // Now try to get the bucket
    auto [db, err] = DB::Open("test_get_bucket.db", 0644, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(false);
    ASSERT_TRUE(txErr.OK());
    
    auto bucket = tx->FindBucketByName("test_bucket");
    EXPECT_NE(nullptr, bucket);
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

TEST_F(BucketTest, PutAndGet) {
    auto [db, err] = DB::Open("test_put_get.db", 0644, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    ASSERT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    
    if (bucketErr == bboltpp::ErrorCode::OK) {
        // Put a key-value pair
        auto putErr = bucket->Put("key1", "value1");
        EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
        
        if (putErr == bboltpp::ErrorCode::OK) {
            // Get the value
            auto value = bucket->Get("key1");
            EXPECT_TRUE(value.has_value());
            if (value.has_value()) {
                EXPECT_EQ("value1", *value);
            }
        }
        
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    }
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

TEST_F(BucketTest, Delete) {
    auto [db, err] = DB::Open("test_delete.db", 0644, nullptr);
    ASSERT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    ASSERT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    
    if (bucketErr == bboltpp::ErrorCode::OK) {
        // Put a key-value pair
        auto putErr = bucket->Put("key1", "value1");
        EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
        
        if (putErr == bboltpp::ErrorCode::OK) {
            // Verify it exists
            auto value = bucket->Get("key1");
            EXPECT_TRUE(value.has_value());
            
            // Delete the key
            auto delErr = bucket->Delete("key1");
            EXPECT_EQ(bboltpp::ErrorCode::OK, delErr);
            
            // Verify it's gone
            auto valueAfter = bucket->Get("key1");
            EXPECT_FALSE(valueAfter.has_value());
        }
        
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    }
    
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
