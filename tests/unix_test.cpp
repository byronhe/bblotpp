#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../bucket.h"
#include "../logger.h"
#include "../utils.h"
#include <iostream>
#include <memory>
#include <string>
#include <sys/resource.h>
#include <unistd.h>
#include <filesystem>

using namespace bboltpp;

class UnixTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
    }
    
    void TearDown() override {
        cleanupTestFiles();
    }
};

// Skip test if memlock limit is below the requested amount
void skipOnMemlockLimitBelow(const std::string& testName, uint64_t memlockLimitRequest) {
    struct rlimit info;
    if (getrlimit(RLIMIT_MEMLOCK, &info) != 0) {
        std::cout << "Warning: Could not get RLIMIT_MEMLOCK, skipping " << testName << std::endl;
        return;
    }
    
    if (info.rlim_cur < memlockLimitRequest) {
        std::cout << "Skipping " << testName << " as RLIMIT_MEMLOCK is insufficient: " 
                  << info.rlim_cur << " < " << memlockLimitRequest << std::endl;
        return;
    }
}

// Insert a chunk of data into the database
void insertChunk(std::shared_ptr<bboltpp::DB>& db, int chunkId) {
    int chunkSize = 1024;
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("bucket");
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    
    for (int i = 0; i < chunkSize; i++) {
        char key[32], value[16];
        snprintf(key, sizeof(key), "key-%d-%d", chunkId, i);
        snprintf(value, sizeof(value), "value");
        
        auto putErr = bucket->Put(key, value);
        EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
    }
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
}

// Get file size
size_t getFileSize(const std::string& path) {
    auto [size, sizeErr] = bboltpp::GetFileSize(path);
    EXPECT_TRUE(sizeErr.OK());
    return size;
}

TEST_F(UnixTest, TestMlock_DbOpen) {
    std::cout << "Running TestMlock_DbOpen..." << std::endl;
    
    // 32KB
    skipOnMemlockLimitBelow("TestMlock_DbOpen", 32 * 1024);
    
    bboltpp::Options options;
    options.Mlock = true;
    
    auto [db, err] = bboltpp::DB::Open("test_mlock_open.db", 0644, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db);
    
    db->Close();
    bboltpp::RemoveFile("test_mlock_open.db");
    
    std::cout << "TestMlock_DbOpen passed" << std::endl;
}

TEST_F(UnixTest, TestMlock_DbCanGrow_Small) {
    std::cout << "Running TestMlock_DbCanGrow_Small..." << std::endl;
    
    // 32KB
    skipOnMemlockLimitBelow("TestMlock_DbCanGrow_Small", 32 * 1024);
    
    bboltpp::Options options;
    options.Mlock = true;
    
    auto [db, err] = bboltpp::DB::Open("test_mlock_grow_small.db", 0644, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db);
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("bucket");
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    
    auto putErr = bucket->Put("key", "value");
    EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
    
    auto commitErr = tx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
    
    db->Close();
    bboltpp::RemoveFile("test_mlock_grow_small.db");
    
    std::cout << "TestMlock_DbCanGrow_Small passed" << std::endl;
}

TEST_F(UnixTest, TestMlock_DbCanGrow_Big) {
    std::cout << "Running TestMlock_DbCanGrow_Big..." << std::endl;
    
    // 32MB
    skipOnMemlockLimitBelow("TestMlock_DbCanGrow_Big", 32 * 1024 * 1024);
    
    int chunksBefore = 64;
    int chunksAfter = 64;
    
    bboltpp::Options options;
    options.Mlock = true;
    
    auto [db, err] = bboltpp::DB::Open("test_mlock_grow_big.db", 0644, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db);
    
    // Insert chunks before
    for (int chunk = 0; chunk < chunksBefore; chunk++) {
        insertChunk(db, chunk);
    }
    
    size_t dbSize = getFileSize("test_mlock_grow_big.db");
    
    // Insert chunks after
    for (int chunk = 0; chunk < chunksAfter; chunk++) {
        insertChunk(db, chunksBefore + chunk);
    }
    
    size_t newDbSize = getFileSize("test_mlock_grow_big.db");
    
    EXPECT_GT(newDbSize, dbSize);
    
    db->Close();
    bboltpp::RemoveFile("test_mlock_grow_big.db");
    
    std::cout << "TestMlock_DbCanGrow_Big passed" << std::endl;
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
