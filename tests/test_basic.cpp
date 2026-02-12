#include <gtest/gtest.h>
#include "../db.h"
#include "../logger.h"
#include "../version.h"
#include "test_common.h"
#include <iostream>
#include <memory>

class BasicTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        SetLogger(std::make_unique<bboltpp::DefaultLogger>(&std::cout));
    }
    
    void TearDown() override {
        cleanupTestFiles();
    }
};

TEST_F(BasicTest, VersionTest) {
    std::string version = bboltpp::GetVersion();
    EXPECT_FALSE(version.empty());
    std::cout << "bbolt++ version: " << version << std::endl;
}

TEST_F(BasicTest, DatabaseOperations) {
    std::cout << "Attempting to open database..." << std::endl;
    
    // Test basic database operations
    auto [db, err] = bboltpp::DB::Open("test.db", 0644, nullptr);
    ASSERT_TRUE(err.OK()) << "Failed to open database: " << err.err_msg_;
    
    std::cout << "Database opened successfully" << std::endl;
    
    // Test transaction
    auto [tx, txErr] = db->Begin(true);
    ASSERT_TRUE(txErr.OK()) << "Failed to begin transaction: " << txErr.err_msg_;
    
    std::cout << "Transaction started successfully" << std::endl;
    
    // Test bucket creation
    auto [bucket, bucketErr] = tx->CreateBucket("test_bucket");
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr) << "Failed to create bucket";
    
    if (bucketErr == bboltpp::ErrorCode::OK) {
        std::cout << "Bucket created successfully" << std::endl;
        
        // Test key-value operations
        auto putErr = bucket->Put("key1", "value1");
        EXPECT_EQ(bboltpp::ErrorCode::OK, putErr) << "Failed to put key-value";
        
        if (putErr == bboltpp::ErrorCode::OK) {
            std::cout << "Key-value pair stored successfully" << std::endl;
            
            // Test retrieval
            auto value = bucket->Get("key1");
            EXPECT_TRUE(value.has_value()) << "Failed to retrieve value";
            
            if (value.has_value()) {
                EXPECT_EQ("value1", *value);
                std::cout << "Retrieved value: " << *value << std::endl;
            }
        }
        
        // Commit transaction
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr) << "Failed to commit transaction";
        
        if (commitErr == bboltpp::ErrorCode::OK) {
            std::cout << "Transaction committed successfully" << std::endl;
        }
    }
    
    // Close database
    auto closeErr = db->Close();
    EXPECT_TRUE(closeErr.OK()) << "Failed to close database: " << closeErr.err_msg_;
    
    if (closeErr.OK()) {
        std::cout << "Database closed successfully" << std::endl;
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
