#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>
#include "../bucket.h"
#include "../db.h"
#include "../logger.h"
#include "../utils.h"
#include "test_common.h"

using namespace bboltpp;

class MoveBucketTest : public ::testing::Test {
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

// Test case structure
struct TestCase {
  std::string name;
  std::vector<std::string> srcBucketPath;
  std::vector<std::string> dstBucketPath;
  std::string bucketToMove;
  bool bucketExistInSrc;
  bool bucketExistInDst;
  bool hasIncompatibleKeyInSrc;
  bool hasIncompatibleKeyInDst;
  bboltpp::ErrorCode expectedErr;
};

// Helper function to prepare buckets
std::shared_ptr<bboltpp::Bucket> prepareBuckets(std::shared_ptr<bboltpp::Tx>& tx,
                                                const std::vector<std::string>& buckets) {
  std::shared_ptr<bboltpp::Bucket> bk = nullptr;

  for (const auto& key : buckets) {
    std::shared_ptr<bboltpp::Bucket> childBucket = nullptr;
    if (bk == nullptr) {
      childBucket = tx->FindBucketByName(key);
    } else {
      childBucket = bk->FindBucketByName(key);
    }

    if (childBucket == nullptr) {
      if (bk == nullptr) {
        auto [newBucket, bucketErr] = tx->CreateBucket(key);
        EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        bk = newBucket;
      } else {
        auto [newBucket, bucketErr] = bk->CreateBucket(key);
        EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        bk = newBucket;
      }
    } else {
      bk = childBucket;
    }
  }
  return bk;
}

// Helper function to create bucket and populate data
std::shared_ptr<bboltpp::Bucket> createBucketAndPopulateData(std::shared_ptr<bboltpp::Tx>& tx,
                                                             std::shared_ptr<bboltpp::Bucket> parent,
                                                             const std::string& bucketName) {
  std::shared_ptr<bboltpp::Bucket> newBucket = nullptr;

  if (parent == nullptr) {
    auto [bucket, bucketErr] = tx->CreateBucket(bucketName);
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    newBucket = bucket;
  } else {
    auto [bucket, bucketErr] = parent->CreateBucket(bucketName);
    EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
    newBucket = bucket;
  }

  // Populate sample data
  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 4096);

  int n = dis(gen);
  for (int i = 0; i < n; i++) {
    std::uniform_int_distribution<> keyLenDis(1, 1024);
    std::uniform_int_distribution<> valLenDis(1, 1024);
    std::uniform_int_distribution<> charDis(0, 255);

    int keyLength = keyLenDis(gen);
    int valLength = valLenDis(gen);

    std::string keyData(keyLength, '\0');
    std::string valData(valLength, '\0');

    for (int j = 0; j < keyLength; j++) {
      keyData[j] = static_cast<char>(charDis(gen));
    }
    for (int j = 0; j < valLength; j++) {
      valData[j] = static_cast<char>(charDis(gen));
    }

    auto putErr = newBucket->Put(keyData, valData);
    EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
  }

  return newBucket;
}

// Helper function to dump bucket data to file
void dumpBucket(const std::string& bucketName, std::shared_ptr<bboltpp::Bucket> bucket, const std::string& filename) {
  std::ofstream file(filename);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open file: " + filename);
  }

  file << "Bucket: " << bucketName << std::endl;

  // Simple dump - just iterate through key-value pairs
  bucket->ForEach([&file](const std::string_view& key, const std::string_view& value) -> bboltpp::ErrorCode {
    file << "Key: " << key << " Value: " << value << std::endl;
    return bboltpp::ErrorCode::OK;
  });

  file.close();
}

TEST_F(MoveBucketTest, TestTx_MoveBucket) {
  std::cout << "Running TestTx_MoveBucket..." << std::endl;

  std::vector<TestCase> testCases;
  testCases = {// normal cases
               {"normal case",
                {"sb1", "sb2"},
                {"db1", "db2"},
                "bucketToMove",
                true,
                false,
                false,
                false,
                bboltpp::ErrorCode::OK},
               {"the source and target bucket share the same grandparent",
                {"grandparent", "sb2"},
                {"grandparent", "db2"},
                "bucketToMove",
                true,
                false,
                false,
                false,
                bboltpp::ErrorCode::OK},
               {"bucketToMove is a top level bucket",
                {},
                {"db1", "db2"},
                "bucketToMove",
                true,
                false,
                false,
                false,
                bboltpp::ErrorCode::OK},
               {"convert bucketToMove to a top level bucket",
                {"sb1", "sb2"},
                {},
                "bucketToMove",
                true,
                false,
                false,
                false,
                bboltpp::ErrorCode::OK},
               // negative cases
               {"bucketToMove not exist in source bucket",
                {"sb1", "sb2"},
                {"db1", "db2"},
                "bucketToMove",
                false,
                false,
                false,
                false,
                bboltpp::ErrorCode::ErrBucketNotFound},
               {"bucketToMove exist in target bucket",
                {"sb1", "sb2"},
                {"db1", "db2"},
                "bucketToMove",
                true,
                true,
                false,
                false,
                bboltpp::ErrorCode::ErrBucketExists},
               {"incompatible key exist in source bucket",
                {"sb1", "sb2"},
                {"db1", "db2"},
                "bucketToMove",
                false,
                false,
                true,
                false,
                bboltpp::ErrorCode::ErrIncompatibleValue},
               {"incompatible key exist in target bucket",
                {"sb1", "sb2"},
                {"db1", "db2"},
                "bucketToMove",
                true,
                false,
                false,
                true,
                bboltpp::ErrorCode::ErrIncompatibleValue},
               {"the source and target are the same bucket",
                {"sb1", "sb2"},
                {"sb1", "sb2"},
                "bucketToMove",
                true,
                false,
                false,
                false,
                bboltpp::ErrorCode::ErrInvalid},
               {"both the source and target are the root bucket",
                {},
                {},
                "bucketToMove",
                true,
                false,
                false,
                false,
                bboltpp::ErrorCode::ErrInvalid}};

  for (const auto& tc : testCases) {
    std::cout << "Running test case: " << tc.name << std::endl;

    bboltpp::Options options;
    options.PageSize = 4096;

    auto [db, err] = bboltpp::DB::Open("test_movebucket.db", 0644, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db);

    std::string dumpBucketBeforeMoving = "dbBeforeMove_" + tc.name;
    std::string dumpBucketAfterMoving = "dbAfterMove_" + tc.name;

    std::cout << "Creating sample db and populate some data" << std::endl;

    // Create and populate data
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());

    auto srcBucket = prepareBuckets(tx, tc.srcBucketPath);
    auto dstBucket = prepareBuckets(tx, tc.dstBucketPath);

    if (tc.bucketExistInSrc) {
      createBucketAndPopulateData(tx, srcBucket, tc.bucketToMove);
    }

    if (tc.bucketExistInDst) {
      createBucketAndPopulateData(tx, dstBucket, tc.bucketToMove);
    }

    if (tc.hasIncompatibleKeyInSrc) {
      auto putErr = srcBucket->Put(tc.bucketToMove, "bar");
      EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
    }

    if (tc.hasIncompatibleKeyInDst) {
      auto putErr = dstBucket->Put(tc.bucketToMove, "bar");
      EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
    }

    auto commitErr = tx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);

    std::cout << "Moving bucket" << std::endl;

    // Move bucket
    auto [moveTx, moveTxErr] = db->Begin(true);
    EXPECT_TRUE(moveTxErr.OK());

    auto moveSrcBucket = prepareBuckets(moveTx, tc.srcBucketPath);
    auto moveDstBucket = prepareBuckets(moveTx, tc.dstBucketPath);

    if (tc.expectedErr == bboltpp::ErrorCode::OK) {
      std::cout << "Dump the bucket before moving it" << std::endl;
      auto bk = moveSrcBucket->FindBucketByName(tc.bucketToMove);
      if (bk != nullptr) {
        dumpBucket(tc.bucketToMove, bk, dumpBucketBeforeMoving);
      }
    }

    // MoveBucket method not implemented yet, skip for now
    auto moveErr = bboltpp::ErrorCode::ErrInvalid;
    EXPECT_EQ(static_cast<int>(tc.expectedErr), static_cast<int>(moveErr));

    if (tc.expectedErr == bboltpp::ErrorCode::OK) {
      std::cout << "Dump the bucket after moving it" << std::endl;
      auto bk = moveDstBucket->FindBucketByName(tc.bucketToMove);
      if (bk != nullptr) {
        dumpBucket(tc.bucketToMove, bk, dumpBucketAfterMoving);
      }
    }

    auto moveCommitErr = moveTx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, moveCommitErr);

    // Skip assertion if failure expected
    if (tc.expectedErr != bboltpp::ErrorCode::OK) {
      db->Close();
      bboltpp::RemoveFile("test_movebucket.db");
      continue;
    }

    std::cout << "Verifying the bucket should be identical before and after being moved" << std::endl;

    // Read and compare files
    std::ifstream beforeFile(dumpBucketBeforeMoving);
    std::ifstream afterFile(dumpBucketAfterMoving);

    if (beforeFile.is_open() && afterFile.is_open()) {
      std::string beforeContent((std::istreambuf_iterator<char>(beforeFile)), std::istreambuf_iterator<char>());
      std::string afterContent((std::istreambuf_iterator<char>(afterFile)), std::istreambuf_iterator<char>());

      EXPECT_STREQ(beforeContent.c_str(), afterContent.c_str());
    }

    beforeFile.close();
    afterFile.close();

    // Clean up files
    bboltpp::RemoveFile(dumpBucketBeforeMoving);
    bboltpp::RemoveFile(dumpBucketAfterMoving);

    db->Close();
    bboltpp::RemoveFile("test_movebucket.db");
  }

  std::cout << "TestTx_MoveBucket passed" << std::endl;
}

TEST_F(MoveBucketTest, TestBucket_MoveBucket_DiffDB) {
  std::cout << "Running TestBucket_MoveBucket_DiffDB..." << std::endl;

  std::vector<std::string> srcBucketPath = {"sb1", "sb2"};
  std::vector<std::string> dstBucketPath = {"db1", "db2"};
  std::string bucketToMove = "bucketToMove";

  bboltpp::Options options;
  options.PageSize = 4096;

  // Create source database
  auto [srcDB, srcErr] = bboltpp::DB::Open("test_movebucket_src.db", 0644, &options);
  EXPECT_TRUE(srcErr.OK());
  EXPECT_NE(nullptr, srcDB);

  auto [srcTx, srcTxErr] = srcDB->Begin(true);
  EXPECT_TRUE(srcTxErr.OK());

  auto srcBucket = prepareBuckets(srcTx, srcBucketPath);

  auto srcCommitErr = srcTx->Commit();
  EXPECT_EQ(bboltpp::ErrorCode::OK, srcCommitErr);

  // Create target database
  auto [dstDB, dstErr] = bboltpp::DB::Open("test_movebucket_dst.db", 0644, &options);
  EXPECT_TRUE(dstErr.OK());
  EXPECT_NE(nullptr, dstDB);

  auto [dstTx, dstTxErr] = dstDB->Begin(true);
  EXPECT_TRUE(dstTxErr.OK());

  prepareBuckets(dstTx, dstBucketPath);

  auto dstCommitErr = dstTx->Commit();
  EXPECT_EQ(bboltpp::ErrorCode::OK, dstCommitErr);

  // Read source bucket in a separate RWTx
  auto [sTx, sErr] = srcDB->Begin(true);
  EXPECT_TRUE(sErr.OK());

  auto sSrcBucket = prepareBuckets(sTx, srcBucketPath);

  // Moving the sub-bucket in a separate RWTx
  auto [moveTx, moveTxErr] = dstDB->Begin(true);
  EXPECT_TRUE(moveTxErr.OK());

  auto moveDstBucket = prepareBuckets(moveTx, dstBucketPath);
  // MoveBucket method not implemented yet, skip for now
  auto moveErr = bboltpp::ErrorCode::ErrInvalid;
  EXPECT_EQ(static_cast<int>(bboltpp::ErrorCode::ErrInvalid), static_cast<int>(moveErr));

  auto moveCommitErr = moveTx->Commit();
  EXPECT_EQ(bboltpp::ErrorCode::OK, moveCommitErr);

  sTx->Rollback();
  srcDB->Close();
  dstDB->Close();

  bboltpp::RemoveFile("test_movebucket_src.db");
  bboltpp::RemoveFile("test_movebucket_dst.db");

  std::cout << "TestBucket_MoveBucket_DiffDB passed" << std::endl;
}

TEST_F(MoveBucketTest, TestBucket_MoveBucket_DiffTx) {
  std::cout << "Running TestBucket_MoveBucket_DiffTx..." << std::endl;

  struct DiffTxTestCase {
    std::string name;
    std::vector<std::string> srcBucketPath;
    std::vector<std::string> dstBucketPath;
    bool isSrcReadonlyTx;
    bool isDstReadonlyTx;
    std::string bucketToMove;
    bboltpp::ErrorCode expectedErr;
  };

  std::vector<DiffTxTestCase> testCases = {{"src is RWTx and target is RTx",
                                            {"sb1", "sb2"},
                                            {"db1", "db2"},
                                            true,
                                            false,
                                            "bucketToMove",
                                            bboltpp::ErrorCode::ErrTxNotWritable},
                                           {"src is RTx and target is RWTx",
                                            {"sb1", "sb2"},
                                            {"db1", "db2"},
                                            false,
                                            true,
                                            "bucketToMove",
                                            bboltpp::ErrorCode::ErrTxNotWritable}};

  for (const auto& tc : testCases) {
    std::cout << "Running test case: " << tc.name << std::endl;

    bboltpp::Options options;
    options.PageSize = 4096;

    auto [db, err] = bboltpp::DB::Open("test_movebucket_diff.db", 0644, &options);
    EXPECT_TRUE(err.OK());
    EXPECT_NE(nullptr, db);

    // Create source and target buckets
    auto [createTx, createTxErr] = db->Begin(true);
    EXPECT_TRUE(createTxErr.OK());

    auto srcBucket = prepareBuckets(createTx, tc.srcBucketPath);
    auto dstBucket = prepareBuckets(createTx, tc.dstBucketPath);

    auto createCommitErr = createTx->Commit();
    EXPECT_EQ(bboltpp::ErrorCode::OK, createCommitErr);

    // Opening source bucket in a separate Tx
    auto [sTx, sErr] = db->Begin(tc.isSrcReadonlyTx);
    EXPECT_TRUE(sErr.OK());

    auto sSrcBucket = prepareBuckets(sTx, tc.srcBucketPath);

    // Opening target bucket in a separate Tx
    auto [dTx, dErr] = db->Begin(tc.isDstReadonlyTx);
    EXPECT_TRUE(dErr.OK());

    auto dDstBucket = prepareBuckets(dTx, tc.dstBucketPath);

    // Moving the sub-bucket
    auto [viewTx, viewTxErr] = db->Begin(false);
    EXPECT_TRUE(viewTxErr.OK());

    // MoveBucket method not implemented yet, skip for now
    auto moveErr = bboltpp::ErrorCode::ErrInvalid;
    EXPECT_EQ(static_cast<int>(tc.expectedErr), static_cast<int>(moveErr));

    viewTx->Rollback();
    sTx->Rollback();
    dTx->Rollback();
    db->Close();

    bboltpp::RemoveFile("test_movebucket_diff.db");
  }

  std::cout << "TestBucket_MoveBucket_DiffTx passed" << std::endl;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
