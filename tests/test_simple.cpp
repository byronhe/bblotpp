#include <gtest/gtest.h>
#include "../db.h"
#include "../logger.h"
#include "../version.h"
#include "test_common.h"
#include <iostream>
#include <memory>

class SimpleTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        SetLogger(std::make_unique<bboltpp::DefaultLogger>(&std::cout));
    }
    
    void TearDown() override {
        cleanupTestFiles();
    }
};

TEST_F(SimpleTest, VersionTest) {
    std::string version = bboltpp::GetVersion();
    EXPECT_FALSE(version.empty());
    std::cout << "bbolt++ version: " << version << std::endl;
}

TEST_F(SimpleTest, DatabaseOpenClose) {
    // Create options with timeout
    bboltpp::Options options;
    options.Timeout = std::chrono::milliseconds(5000); // 5 second timeout
    options.ReadOnly = false; // Make sure it's not read-only
    
    std::cout << "Opening database with options..." << std::endl;
    auto [db, err] = bboltpp::DB::Open("test.db", 0644, &options);
    
    std::cout << "DB::Open returned" << std::endl;
    std::cout << "Error code: " << static_cast<int>(err.err_code_) << std::endl;
    std::cout << "Int code: " << err.int_code_ << std::endl;
    std::cout << "Message: '" << err.err_msg_ << "'" << std::endl;
    
    EXPECT_TRUE(err.OK()) << "Failed to open database. Error code: " << static_cast<int>(err.err_code_) 
                          << ", int code: " << err.int_code_ 
                          << ", message: '" << err.err_msg_ << "'";
    
    if (err.OK()) {
        std::cout << "Database opened successfully" << std::endl;
        
        // Close database
        auto closeErr = db->Close();
        EXPECT_TRUE(closeErr.OK()) << "Failed to close database: " << closeErr.err_msg_;
        
        if (closeErr.OK()) {
            std::cout << "Database closed successfully" << std::endl;
        }
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
