#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../bucket.h"
#include "../tx.h"
#include "../logger.h"
#include <vector>
#include <string>
#include <memory>
#include <sstream>
#include <fstream>
#include <random>
#include <filesystem>

using namespace bboltpp;

class CmdBboltCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
    }

    void TearDown() override {
        cleanupTestFiles();
        // Clean up test files
    }
    
    // Helper function to create a test database
    std::shared_ptr<DB> createTestDB(const std::string& filename) {
        auto options = Options{};
        auto [db, err] = DB::Open(filename, 0600, &options);
        if (!err.OK()) {
            throw std::runtime_error("Failed to create test database: " + std::to_string(static_cast<int>(err.err_code_)));
        }
        return std::move(db);
    }
    
    // Helper function to fill database with test data
    void fillDatabase(DB* db, const std::string& bucketName, int count) {
        auto [tx, txErr] = db->Begin(true);
        if (!txErr.OK()) {
            throw std::runtime_error("Failed to begin transaction: " + std::to_string(static_cast<int>(txErr.err_code_)));
        }
        
        auto [bucket, bucketErr] = tx->CreateBucket(bucketName);
        if (bucketErr != ErrorCode::OK) {
            throw std::runtime_error("Failed to create bucket: " + bucketName);
        }
        
        for (int i = 0; i < count; ++i) {
            std::string key = "k" + std::to_string(i);
            std::string value(10 * (1 + (i % 4)), 'x'); // Variable length value
            auto putErr = bucket->Put(key, value);
            if (putErr != ErrorCode::OK) {
                throw std::runtime_error("Failed to put key: " + key);
            }
        }
        
        auto commitErr = tx->Commit();
        if (commitErr != ErrorCode::OK) {
            throw std::runtime_error("Failed to commit transaction: " + std::to_string(static_cast<int>(commitErr)));
        }
    }
    
    // Helper function to check database integrity
    std::string checkDatabase(const std::string& path) {
        auto db = createTestDB(path);
        std::stringstream result;
        
        auto [tx, txErr] = db->Begin(false);
        if (!txErr.OK()) {
            result << "Failed to begin read transaction: " << std::to_string(static_cast<int>(txErr.err_code_));
            return result.str();
        }
        
        // Walk through all buckets
        auto walkErr = tx->ForEach([&result](std::string_view name, const std::shared_ptr<Bucket>& bucket) -> ErrorCode {
            result << "Bucket: " << name << std::endl;
            return ErrorCode::OK;
        });
        
        if (walkErr != ErrorCode::OK) {
            result << "Failed to walk buckets: " << static_cast<int>(walkErr);
        }
        
        auto commitErr = tx->Commit();
        if (commitErr != ErrorCode::OK) {
            result << "Failed to commit read transaction: " << static_cast<int>(commitErr);
        }
        
        return result.str();
    }
    
    // Helper function to read file data
    std::vector<uint8_t> readFileData(const std::string& filePath) {
        std::ifstream file(filePath, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + filePath);
        }
        
        std::vector<uint8_t> data((std::istreambuf_iterator<char>(file)),
                                 std::istreambuf_iterator<char>());
        return data;
    }
    
    // Helper function to check if file data changed
    void requireDBNoChange(const std::vector<uint8_t>& oldData, const std::string& filePath) {
        auto newData = readFileData(filePath);
        if (oldData != newData) {
            throw std::runtime_error("File data changed unexpectedly");
        }
    }
};

TEST_F(CmdBboltCommandTest, TestBenchCommand_Run) {
    // Test database creation and basic operations
    auto db = createTestDB("test_bench.db");
    
    // Fill with test data
    fillDatabase(db.get(), "data", 1000);
    
    // Test read operations
    {
        auto [tx, txErr] = db->Begin(false);
        EXPECT_TRUE(txErr.OK());
        
        auto bucket = tx->FindBucketByName("data");
        EXPECT_NE(nullptr, bucket);
        
        // Read some keys
        for (int i = 0; i < 10; ++i) {
            std::string key = "k" + std::to_string(i);
            auto value = bucket->Get(key);
            EXPECT_TRUE(value.has_value());
            EXPECT_FALSE(value->empty());
        }
        
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    }
    
    db->Close();
    
    // Test database integrity
    std::string checkResult = checkDatabase("test_bench.db");
    EXPECT_FALSE(checkResult.empty());
    
    // Clean up
    std::remove("test_bench.db");
}

TEST_F(CmdBboltCommandTest, TestUtils_FillBucket) {
    auto db = createTestDB("test_utils.db");
    
    // Fill with test data using the fillDatabase helper
    fillDatabase(db.get(), "test_bucket", 100);
    
    // Verify the data
    {
        auto [tx, txErr] = db->Begin(false);
        EXPECT_TRUE(txErr.OK());
        
        auto bucket = tx->FindBucketByName("test_bucket");
        EXPECT_NE(nullptr, bucket);
        
        // Check that we have the expected number of keys
        int count = 0;
        auto forEachErr = bucket->ForEach([&count](std::string_view key, std::string_view value) -> ErrorCode {
            count++;
            return ErrorCode::OK;
        });
        
        EXPECT_EQ(bboltpp::ErrorCode::OK, forEachErr);
        EXPECT_EQ(100, count);
        
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    }
    
    db->Close();
    
    // Clean up
    std::remove("test_utils.db");
}

TEST_F(CmdBboltCommandTest, TestUtils_CheckDatabase) {
    auto db = createTestDB("test_check.db");
    
    // Fill with test data
    fillDatabase(db.get(), "data1", 50);
    fillDatabase(db.get(), "data2", 75);
    
    db->Close();
    
    // Check database integrity
    std::string checkResult = checkDatabase("test_check.db");
    EXPECT_FALSE(checkResult.empty());
    
    // The result should contain information about our buckets
    EXPECT_TRUE(checkResult.find("data1") != std::string::npos);
    EXPECT_TRUE(checkResult.find("data2") != std::string::npos);
    
    // Clean up
    std::remove("test_check.db");
}

TEST_F(CmdBboltCommandTest, TestUtils_FileOperations) {
    auto db = createTestDB("test_file_ops.db");
    
    // Fill with test data
    fillDatabase(db.get(), "data", 100);
    
    db->Close();
    
    // Read file data
    auto oldData = readFileData("test_file_ops.db");
    EXPECT_FALSE(oldData.empty());
    
    // Reopen and make a small change
    db = createTestDB("test_file_ops.db");
    {
        auto [tx, txErr] = db->Begin(true);
        EXPECT_TRUE(txErr.OK());
        
        auto bucket = tx->FindBucketByName("data");
        EXPECT_NE(nullptr, bucket);
        
        auto putErr = bucket->Put("new_key", "new_value");
        EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
        
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    }
    db->Close();
    
    // Read new file data
    auto newData = readFileData("test_file_ops.db");
    EXPECT_NE(oldData, newData); // Should be different now
    
    // Clean up
    std::remove("test_file_ops.db");
}


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
