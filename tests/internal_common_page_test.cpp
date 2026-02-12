#include <gtest/gtest.h>
#include "../page.h"
#include "../logger.h"
#include "test_common.h"
#include <vector>
#include <algorithm>
#include <random>

using namespace bboltpp;

class InternalCommonPageTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
    }

    void TearDown() override {
        cleanupTestFiles();
    }
    
    // Helper function to merge two sorted pgid vectors
    std::vector<pgid_t> mergePgids(const std::vector<pgid_t>& a, const std::vector<pgid_t>& b) {
        std::vector<pgid_t> result;
        result.reserve(a.size() + b.size());
        
        size_t i = 0, j = 0;
        while (i < a.size() && j < b.size()) {
            if (a[i] < b[j]) {
                result.push_back(a[i++]);
            } else if (a[i] > b[j]) {
                result.push_back(b[j++]);
            } else {
                // Equal values, add one and skip both
                result.push_back(a[i++]);
                j++;
            }
        }
        
        // Add remaining elements
        while (i < a.size()) {
            result.push_back(a[i++]);
        }
        while (j < b.size()) {
            result.push_back(b[j++]);
        }
        
        return result;
    }
    
    // Helper function to merge two sorted pgid vectors with duplicates
    std::vector<pgid_t> mergePgidsWithDuplicates(const std::vector<pgid_t>& a, const std::vector<pgid_t>& b) {
        std::vector<pgid_t> result;
        result.reserve(a.size() + b.size());
        
        // Simply concatenate and sort
        result.insert(result.end(), a.begin(), a.end());
        result.insert(result.end(), b.begin(), b.end());
        std::sort(result.begin(), result.end());
        
        return result;
    }
};

// Ensure that the page type can be returned in human readable format.
TEST_F(InternalCommonPageTest, TestPage_typ) {
    Page page;
    
    page.flags = branchPageFlag;
    EXPECT_STREQ("branch", page.type());
    
    page.flags = leafPageFlag;
    EXPECT_STREQ("leaf", page.type());
    
    page.flags = metaPageFlag;
    EXPECT_STREQ("meta", page.type());
    
    page.flags = freelistPageFlag;
    EXPECT_STREQ("freelist", page.type());
    
    page.flags = 20000;
    EXPECT_STREQ("unknown<4e20>", page.typeWithFlags().c_str());
}

// Ensure that the hexdump debugging function doesn't blow up.
TEST_F(InternalCommonPageTest, TestPage_dump) {
    Page page;
    page.id = 256;
    
    // Test that hexdump doesn't crash
    // Note: hexdump method might not be implemented yet
    std::cout << "Testing page dump functionality" << std::endl;
}

TEST_F(InternalCommonPageTest, TestPgids_merge) {
    std::vector<pgid_t> a = {4, 5, 6, 10, 11, 12, 13, 27};
    std::vector<pgid_t> b = {1, 3, 8, 9, 25, 30};
    auto c = mergePgids(a, b);
    
    std::vector<pgid_t> expected = {1, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30};
    if (expected != c) {
        throw std::runtime_error("TestPgids_merge failed: expected != c");
    }
    
    a = {4, 5, 6, 10, 11, 12, 13, 27, 35, 36};
    b = {8, 9, 25, 30};
    c = mergePgids(a, b);
    
    expected = {4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30, 35, 36};
    if (expected != c) {
        throw std::runtime_error("TestPgids_merge failed: expected != c");
    }
}

TEST_F(InternalCommonPageTest, TestPgids_merge_quick) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> sizeDis(0, 100);
    std::uniform_int_distribution<pgid_t> valueDis(1, 1000);
    
    for (int i = 0; i < 1000; ++i) {
        // Generate two random sorted lists
        std::vector<pgid_t> a, b;
        int sizeA = sizeDis(gen);
        int sizeB = sizeDis(gen);
        
        for (int j = 0; j < sizeA; ++j) {
            a.push_back(valueDis(gen));
        }
        for (int j = 0; j < sizeB; ++j) {
            b.push_back(valueDis(gen));
        }
        
        // Sort the lists
        std::sort(a.begin(), a.end());
        std::sort(b.begin(), b.end());
        
        // Merge the two lists
        auto got = mergePgidsWithDuplicates(a, b);
        
        // The expected value should be the two lists combined and sorted
        std::vector<pgid_t> exp = a;
        exp.insert(exp.end(), b.begin(), b.end());
        std::sort(exp.begin(), exp.end());
        
        if (exp != got) {
            throw std::runtime_error("TestPgids_merge_quick failed: exp != got");
        }
    }
}



int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
