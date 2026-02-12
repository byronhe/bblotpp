#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../bucket.h"
#include "../logger.h"
#include "../utils.h"
#include <iostream>
#include <memory>
#include <random>
#include <vector>
#include <string>
#include <map>
#include <algorithm>
#include <chrono>
#include <filesystem>

// Quick test configuration
struct QuickConfig {
    int count = 5;
    int maxItems = 1000;
    int maxKeySize = 1024;
    int maxValueSize = 1024;
    int seed = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count()) % 100000;
};

QuickConfig g_config;

class QuickTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
    }
    
    void TearDown() override {
        cleanupTestFiles();
    }
};

// Test data item
struct TestDataItem {
    std::string key;
    std::string value;
    
    TestDataItem() = default;
    TestDataItem(const std::string& k, const std::string& v) : key(k), value(v) {}
};

// Test data container
class TestData {
private:
    std::vector<TestDataItem> items_;
    
public:
    TestData() = default;
    
    void Generate(std::mt19937& gen) {
        std::uniform_int_distribution<> itemCountDis(1, g_config.maxItems);
        std::uniform_int_distribution<> keySizeDis(1, g_config.maxKeySize);
        std::uniform_int_distribution<> valueSizeDis(0, g_config.maxValueSize);
        std::uniform_int_distribution<> charDis(0, 255);
        
        int n = itemCountDis(gen);
        items_.clear();
        items_.reserve(n);
        
        std::map<std::string, bool> used;
        
        for (int i = 0; i < n; i++) {
            std::string key;
            // Ensure that keys are unique by looping until we find one that we have not already used.
            do {
                int keySize = keySizeDis(gen);
                key.resize(keySize);
                for (int j = 0; j < keySize; j++) {
                    key[j] = static_cast<char>(charDis(gen));
                }
            } while (used.find(key) != used.end());
            
            used[key] = true;
            
            int valueSize = valueSizeDis(gen);
            std::string value(valueSize, '\0');
            for (int j = 0; j < valueSize; j++) {
                value[j] = static_cast<char>(charDis(gen));
            }
            
            items_.emplace_back(key, value);
        }
    }
    
    size_t size() const { return items_.size(); }
    bool empty() const { return items_.empty(); }
    
    const TestDataItem& operator[](size_t index) const { return items_[index]; }
    TestDataItem& operator[](size_t index) { return items_[index]; }
    
    void sort() {
        std::sort(items_.begin(), items_.end(), 
                 [](const TestDataItem& a, const TestDataItem& b) {
                     return a.key < b.key;
                 });
    }
    
    void reverseSort() {
        std::sort(items_.begin(), items_.end(), 
                 [](const TestDataItem& a, const TestDataItem& b) {
                     return a.key > b.key;
                 });
    }
    
    auto begin() { return items_.begin(); }
    auto end() { return items_.end(); }
    auto begin() const { return items_.begin(); }
    auto end() const { return items_.end(); }
};

// Generate random byte slice
std::string randByteSlice(std::mt19937& gen, int minSize, int maxSize) {
    std::uniform_int_distribution<> sizeDis(minSize, maxSize);
    std::uniform_int_distribution<> charDis(0, 255);
    
    int n = sizeDis(gen);
    std::string result(n, '\0');
    for (int i = 0; i < n; i++) {
        result[i] = static_cast<char>(charDis(gen));
    }
    return result;
}

TEST_F(QuickTest, TestQuickBasic) {
    std::cout << "Running TestQuickBasic..." << std::endl;
    
    for (int i = 0; i < g_config.count; i++) {
        std::cout << "Quick test iteration " << (i + 1) << "/" << g_config.count << std::endl;
        
        // Generate test data
        std::mt19937 gen(g_config.seed + i);
        TestData testData;
        testData.Generate(gen);
        
        std::cout << "Generated " << testData.size() << " test items" << std::endl;
        
        // Create database
        auto [db, err] = bboltpp::DB::Open("test_quick.db", 0644, nullptr);
        EXPECT_TRUE(err.OK());
        EXPECT_NE(nullptr, db);
        
        // Insert data
        auto [tx, txErr] = db->Begin(true);
        EXPECT_TRUE(txErr.OK());
        
        auto [bucket, bucketErr] = tx->CreateBucket("test");
        EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        
        for (const auto& item : testData) {
            auto putErr = bucket->Put(item.key, item.value);
            EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
        }
        
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
        
        // Verify data
        auto [verifyTx, verifyTxErr] = db->Begin(false);
        EXPECT_TRUE(verifyTxErr.OK());
        
        auto verifyBucket = verifyTx->FindBucketByName("test");
        EXPECT_NE(nullptr, verifyBucket);
        
        for (const auto& item : testData) {
            auto value = verifyBucket->Get(item.key);
            EXPECT_TRUE(value.has_value());
            EXPECT_EQ(item.value, std::string(value.value()));
        }
        
        verifyTx->Rollback();
        db->Close();
        
        // Clean up
        bboltpp::RemoveFile("test_quick.db");
    }
    
    std::cout << "TestQuickBasic passed" << std::endl;
}

TEST_F(QuickTest, TestQuickSorted) {
    std::cout << "Running TestQuickSorted..." << std::endl;
    
    for (int i = 0; i < g_config.count; i++) {
        std::cout << "Quick sorted test iteration " << (i + 1) << "/" << g_config.count << std::endl;
        
        // Generate test data
        std::mt19937 gen(g_config.seed + i);
        TestData testData;
        testData.Generate(gen);
        testData.sort();
        
        std::cout << "Generated " << testData.size() << " sorted test items" << std::endl;
        
        // Create database
        auto [db, err] = bboltpp::DB::Open("test_quick_sorted.db", 0644, nullptr);
        EXPECT_TRUE(err.OK());
        EXPECT_NE(nullptr, db);
        
        // Insert data
        auto [tx, txErr] = db->Begin(true);
        EXPECT_TRUE(txErr.OK());
        
        auto [bucket, bucketErr] = tx->CreateBucket("test");
        EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        
        for (const auto& item : testData) {
            auto putErr = bucket->Put(item.key, item.value);
            EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
        }
        
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
        
        // Verify data in sorted order
        auto [verifyTx, verifyTxErr] = db->Begin(false);
        EXPECT_TRUE(verifyTxErr.OK());
        
        auto verifyBucket = verifyTx->FindBucketByName("test");
        EXPECT_NE(nullptr, verifyBucket);
        
        std::vector<std::string> keys;
        verifyBucket->ForEach([&keys](const std::string_view& key, const std::string_view& value) -> bboltpp::ErrorCode {
            keys.push_back(std::string(key));
            return bboltpp::ErrorCode::OK;
        });
        
        // Verify keys are sorted
        for (size_t j = 1; j < keys.size(); j++) {
            EXPECT_LE(keys[j-1], keys[j]);
        }
        
        verifyTx->Rollback();
        db->Close();
        
        // Clean up
        bboltpp::RemoveFile("test_quick_sorted.db");
    }
    
    std::cout << "TestQuickSorted passed" << std::endl;
}

TEST_F(QuickTest, TestQuickReverseSorted) {
    std::cout << "Running TestQuickReverseSorted..." << std::endl;
    
    for (int i = 0; i < g_config.count; i++) {
        std::cout << "Quick reverse sorted test iteration " << (i + 1) << "/" << g_config.count << std::endl;
        
        // Generate test data
        std::mt19937 gen(g_config.seed + i);
        TestData testData;
        testData.Generate(gen);
        testData.reverseSort();
        
        std::cout << "Generated " << testData.size() << " reverse sorted test items" << std::endl;
        
        // Create database
        auto [db, err] = bboltpp::DB::Open("test_quick_reverse.db", 0644, nullptr);
        EXPECT_TRUE(err.OK());
        EXPECT_NE(nullptr, db);
        
        // Insert data
        auto [tx, txErr] = db->Begin(true);
        EXPECT_TRUE(txErr.OK());
        
        auto [bucket, bucketErr] = tx->CreateBucket("test");
        EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        
        for (const auto& item : testData) {
            auto putErr = bucket->Put(item.key, item.value);
            EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
        }
        
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
        
        // Verify data
        auto [verifyTx, verifyTxErr] = db->Begin(false);
        EXPECT_TRUE(verifyTxErr.OK());
        
        auto verifyBucket = verifyTx->FindBucketByName("test");
        EXPECT_NE(nullptr, verifyBucket);
        
        for (const auto& item : testData) {
            auto value = verifyBucket->Get(item.key);
            EXPECT_TRUE(value.has_value());
            EXPECT_EQ(item.value, std::string(value.value()));
        }
        
        verifyTx->Rollback();
        db->Close();
        
        // Clean up
        bboltpp::RemoveFile("test_quick_reverse.db");
    }
    
    std::cout << "TestQuickReverseSorted passed" << std::endl;
}

TEST_F(QuickTest, TestQuickRandom) {
    std::cout << "Running TestQuickRandom..." << std::endl;
    
    for (int i = 0; i < g_config.count; i++) {
        std::cout << "Quick random test iteration " << (i + 1) << "/" << g_config.count << std::endl;
        
        // Generate test data
        std::mt19937 gen(g_config.seed + i);
        TestData testData;
        testData.Generate(gen);
        
        // Shuffle the data
        std::shuffle(testData.begin(), testData.end(), gen);
        
        std::cout << "Generated " << testData.size() << " random test items" << std::endl;
        
        // Create database
        auto [db, err] = bboltpp::DB::Open("test_quick_random.db", 0644, nullptr);
        EXPECT_TRUE(err.OK());
        EXPECT_NE(nullptr, db);
        
        // Insert data
        auto [tx, txErr] = db->Begin(true);
        EXPECT_TRUE(txErr.OK());
        
        auto [bucket, bucketErr] = tx->CreateBucket("test");
        EXPECT_EQ(bboltpp::ErrorCode::OK, bucketErr);
        
        for (const auto& item : testData) {
            auto putErr = bucket->Put(item.key, item.value);
            EXPECT_EQ(bboltpp::ErrorCode::OK, putErr);
        }
        
        auto commitErr = tx->Commit();
        EXPECT_EQ(bboltpp::ErrorCode::OK, commitErr);
        
        // Verify data
        auto [verifyTx, verifyTxErr] = db->Begin(false);
        EXPECT_TRUE(verifyTxErr.OK());
        
        auto verifyBucket = verifyTx->FindBucketByName("test");
        EXPECT_NE(nullptr, verifyBucket);
        
        for (const auto& item : testData) {
            auto value = verifyBucket->Get(item.key);
            EXPECT_TRUE(value.has_value());
            EXPECT_EQ(item.value, std::string(value.value()));
        }
        
        verifyTx->Rollback();
        db->Close();
        
        // Clean up
        bboltpp::RemoveFile("test_quick_random.db");
    }
    
    std::cout << "TestQuickRandom passed" << std::endl;
}

int main(int argc, char* argv[]) {
    // Parse command line arguments (simplified)
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--count" && i + 1 < argc) {
            g_config.count = std::stoi(argv[++i]);
        } else if (arg == "--seed" && i + 1 < argc) {
            g_config.seed = std::stoi(argv[++i]);
        } else if (arg == "--maxitems" && i + 1 < argc) {
            g_config.maxItems = std::stoi(argv[++i]);
        } else if (arg == "--maxksize" && i + 1 < argc) {
            g_config.maxKeySize = std::stoi(argv[++i]);
        } else if (arg == "--maxvsize" && i + 1 < argc) {
            g_config.maxValueSize = std::stoi(argv[++i]);
        }
    }
    
    std::cout << "Quick test settings: count=" << g_config.count 
              << ", items=" << g_config.maxItems 
              << ", ksize=" << g_config.maxKeySize 
              << ", vsize=" << g_config.maxValueSize 
              << ", seed=" << g_config.seed << std::endl;
    
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
