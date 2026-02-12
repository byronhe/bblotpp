#include <gtest/gtest.h>
#include "../db.h"
#include "../logger.h"
#include "../version.h"
#include "test_common.h"
#include <iostream>
#include <memory>
#include <filesystem>

using namespace bboltpp;

class DBTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        SetLogger(std::make_unique<DefaultLogger>(&std::cout));
    }
    
    void TearDown() override {
        cleanupTestFiles();
    }
};

TEST_F(DBTest, Open) {
    // Test opening a new database
    auto [db, err] = DB::Open("test_open.db", 0644, nullptr);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db.get());
    
    if (err.OK() && db) {
        // Close the database
        auto closeErr = db->Close();
        EXPECT_TRUE(closeErr.OK());
    }
}

TEST_F(DBTest, OpenWithOptions) {
    Options options;
    options.Timeout = std::chrono::milliseconds(1000);
    options.ReadOnly = false;
    
    auto [db, err] = DB::Open("test_options.db", 0644, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db.get());
    
    if (err.OK() && db) {
        auto closeErr = db->Close();
        EXPECT_TRUE(closeErr.OK());
    }
}

TEST_F(DBTest, OpenReadOnly) {
    // First create a database
    {
        auto [db, err] = DB::Open("test_readonly.db", 0644, nullptr);
        ASSERT_TRUE(err.OK());
        if (err.OK() && db) {
            auto closeErr = db->Close();
            ASSERT_TRUE(closeErr.OK());
        }
    }
    
    // Now open it read-only
    Options options;
    options.ReadOnly = true;
    options.Timeout = std::chrono::milliseconds(1000);
    
    auto [db, err] = DB::Open("test_readonly.db", 0644, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db.get());
    
    if (err.OK() && db) {
        auto closeErr = db->Close();
        EXPECT_TRUE(closeErr.OK());
    }
}

TEST_F(DBTest, OpenNonExistentFile) {
    Options options;
    options.ReadOnly = true;
    options.Timeout = std::chrono::milliseconds(1000);
    
    auto [db, err] = DB::Open("non_existent.db", 0644, &options);
    EXPECT_FALSE(err.OK());
    EXPECT_EQ(nullptr, db.get());
}

TEST_F(DBTest, Version) {
    std::string version = GetVersion();
    EXPECT_FALSE(version.empty());
    std::cout << "Version: " << version << std::endl;
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
