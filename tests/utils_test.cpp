#include <gtest/gtest.h>
#include "../db.h"
#include "../bucket.h"
#include "../utils.h"
#include "../logger.h"
#include "test_common.h"
#include <iostream>
#include <memory>
#include <filesystem>

using namespace bboltpp;

class UtilsTest : public ::testing::Test {
protected:
    void SetUp() override {
        bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
        cleanupTestFiles();
    }
    
    void TearDown() override {
        cleanupTestFiles();
    }
};

// Test utility functions
TEST_F(UtilsTest, TestUtilsCopyFile) {
    // Create a source file
    const std::string srcFile = "test_utils_src.db";
    const std::string dstFile = "test_utils_dst.db";
    
    auto [db, err] = DB::Open(srcFile, 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    if (err.OK()) {
        // Add some data
        auto [tx, txErr] = db->Begin(true);
        EXPECT_TRUE(txErr.OK());
        
        if (txErr.OK()) {
            auto [bucket, bucketErr] = tx->CreateBucket("test");
            EXPECT_EQ(ErrorCode::OK, bucketErr);
            
            if (bucketErr == ErrorCode::OK) {
                bucket->Put("key1", "value1");
                bucket->Put("key2", "value2");
                
                tx->Commit();
            }
        }
        
        db->Close();
    }
    
    // Test file copy
    auto copyErr = CopyFile(srcFile, dstFile);
    EXPECT_TRUE(copyErr.OK());
    
    if (copyErr.OK()) {
        // Verify the copied file
        EXPECT_TRUE(FileExists(dstFile));
        
        // Open the copied file and verify data
        auto [copiedDb, copiedErr] = DB::Open(dstFile, 0644, nullptr);
        EXPECT_TRUE(copiedErr.OK());
        
        if (copiedErr.OK()) {
            auto [readTx, readErr] = copiedDb->Begin(false);
            EXPECT_TRUE(readErr.OK());
            
            if (readErr.OK()) {
                auto readBucket = readTx->FindBucketByName("test");
                EXPECT_NE(nullptr, readBucket);
                
                if (readBucket) {
                    auto value1 = readBucket->Get("key1");
                    EXPECT_TRUE(value1.has_value());
                    if (value1.has_value()) {
                        EXPECT_EQ("value1", *value1);
                    }
                }
                
                readTx->Rollback();
            }
            copiedDb->Close();
        }
    }
}

TEST_F(UtilsTest, TestUtilsFileExists) {
    const std::string existingFile = "test_utils_exists.db";
    const std::string nonExistingFile = "test_utils_not_exists.db";
    
    // Create a file
    auto [db, err] = DB::Open(existingFile, 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    if (err.OK()) {
        db->Close();
        
        // Test file existence
        EXPECT_TRUE(FileExists(existingFile));
        EXPECT_FALSE(FileExists(nonExistingFile));
        
        // Cleanup
        RemoveFile(existingFile);
    }
}

TEST_F(UtilsTest, TestUtilsGetFileSize) {
    const std::string testFile = "test_utils_size.db";
    
    // Create a file with some data
    auto [db, err] = DB::Open(testFile, 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test");
    EXPECT_EQ(ErrorCode::OK, bucketErr);
    
    // Add some data to make the file larger
    for (int i = 0; i < 100; i++) {
        char key[16], value[64];
        snprintf(key, sizeof(key), "key%03d", i);
        snprintf(value, sizeof(value), "value%03d", i);
        bucket->Put(key, value);
    }
    
    tx->Commit();
    db->Close();
    
    // Test file size
    auto [size, sizeErr] = GetFileSize(testFile);
    EXPECT_GT(size, 0);
    
    // Cleanup
    RemoveFile(testFile);
}

TEST_F(UtilsTest, TestUtilsCreateTempFile) {
    auto [tempFile, err] = CreateTempFile("test_utils_temp");
    EXPECT_TRUE(err.OK());
    EXPECT_FALSE(tempFile.empty());
    
    // Verify the file exists
    EXPECT_TRUE(FileExists(tempFile));
    
    // Cleanup
    RemoveFile(tempFile);
}

TEST_F(UtilsTest, TestUtilsRemoveFile) {
    const std::string testFile = "test_utils_remove.db";
    
    // Create a file
    auto [db, err] = DB::Open(testFile, 0644, nullptr);
    EXPECT_TRUE(err.OK());
    db->Close();
    
    // Verify it exists
    EXPECT_TRUE(FileExists(testFile));
    
    // Remove it
    auto removeErr = RemoveFile(testFile);
    EXPECT_TRUE(removeErr.OK());
    
    // Verify it's gone
    EXPECT_FALSE(FileExists(testFile));
}

TEST_F(UtilsTest, TestUtilsRenameFile) {
    const std::string oldFile = "test_utils_old.db";
    const std::string newFile = "test_utils_new.db";
    
    // Create a file
    auto [db, err] = DB::Open(oldFile, 0644, nullptr);
    EXPECT_TRUE(err.OK());
    db->Close();
    
    // Rename it
    auto renameErr = RenameFile(oldFile, newFile);
    EXPECT_TRUE(renameErr.OK());
    
    // Verify old file doesn't exist and new file does
    EXPECT_FALSE(FileExists(oldFile));
    EXPECT_TRUE(FileExists(newFile));
    
    // Cleanup
    RemoveFile(newFile);
}

TEST_F(UtilsTest, TestUtilsEnsureDir) {
    const std::string testDir = "test_utils_dir";
    
    // Ensure directory exists
    auto ensureErr = EnsureDir(testDir);
    EXPECT_TRUE(ensureErr.OK());
    
    // Verify it exists
    EXPECT_TRUE(IsDir(testDir));
    
    // Cleanup
    std::filesystem::remove_all(testDir);
}

TEST_F(UtilsTest, TestUtilsIsDir) {
    const std::string testDir = "test_utils_is_dir";
    const std::string testFile = "test_utils_is_file.db";
    
    // Create a directory
    EnsureDir(testDir);
    EXPECT_TRUE(IsDir(testDir));
    
    // Create a file
    auto [db, err] = DB::Open(testFile, 0644, nullptr);
    EXPECT_TRUE(err.OK());
    db->Close();
    
    EXPECT_FALSE(IsDir(testFile));
    
    // Cleanup
    std::filesystem::remove_all(testDir);
    RemoveFile(testFile);
}

TEST_F(UtilsTest, TestUtilsJoinPath) {
    std::string path1 = JoinPath(JoinPath("dir1", "dir2"), "file.db");
    EXPECT_EQ("dir1/dir2/file.db", path1);
    
    std::string path2 = JoinPath("", "file.db");
    EXPECT_EQ("file.db", path2);
}

TEST_F(UtilsTest, TestUtilsBaseName) {
    EXPECT_EQ("file.db", BaseName("path/to/file.db"));
    EXPECT_EQ("file.db", BaseName("file.db"));
    EXPECT_EQ("", BaseName(""));
}

TEST_F(UtilsTest, TestUtilsDirName) {
    EXPECT_EQ("path/to", DirName("path/to/file.db"));
    EXPECT_EQ("", DirName("file.db"));
    EXPECT_EQ("", DirName(""));
}

// Test bucket dumping utility
TEST_F(UtilsTest, TestUtilsDumpBucket) {
    const std::string srcFile = "test_utils_dump_src.db";
    const std::string dstFile = "test_utils_dump_dst.db";
    
    // Create source database
    auto [srcDb, srcErr] = DB::Open(srcFile, 0644, nullptr);
    EXPECT_TRUE(srcErr.OK());
    
    auto [srcTx, srcTxErr] = srcDb->Begin(true);
    EXPECT_TRUE(srcTxErr.OK());
    
    auto [srcBucket, srcBucketErr] = srcTx->CreateBucket("test");
    EXPECT_EQ(ErrorCode::OK, srcBucketErr);
    
    // Add some data
    srcBucket->Put("key1", "value1");
    srcBucket->Put("key2", "value2");
    
    // Create a nested bucket
    auto [nestedBucket, nestedErr] = srcBucket->CreateBucket("nested");
    EXPECT_EQ(ErrorCode::OK, nestedErr);
    nestedBucket->Put("nested_key", "nested_value");
    
    srcTx->Commit();
    srcDb->Close();
    
    // Note: Full bucket dumping would require implementing a dumpBucket function
    // This test verifies the basic infrastructure is available
    
    // Cleanup
    RemoveFile(srcFile);
    RemoveFile(dstFile);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
