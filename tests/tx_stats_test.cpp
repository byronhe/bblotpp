#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../bucket.h"
#include "../logger.h"
#include "../tx.h"
#include <iostream>
#include <memory>
#include <chrono>

using namespace bboltpp;

class TxStatsTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanupTestFiles();
        bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
    }
    
    void TearDown() override {
        cleanupTestFiles();
        // No cleanup needed for this test
    }
};

TEST_F(TxStatsTest, TestTxStats_add) {
    std::cout << "Running TestTxStats_add..." << std::endl;
    
    // Create statsA
    bboltpp::TxStats statsA;
    statsA.PageCount = 1;
    statsA.PageAlloc = 2;
    statsA.CursorCount = 3;
    statsA.NodeCount = 100;
    statsA.NodeDeref = 101;
    statsA.Rebalance = 1000;
    statsA.RebalanceTime = std::chrono::seconds(1001);
    statsA.Split = 10000;
    statsA.Spill = 10001;
    statsA.SpillTime = std::chrono::seconds(10001);
    statsA.Write = 100000;
    statsA.WriteTime = std::chrono::seconds(100001);
    
    // Create statsB
    bboltpp::TxStats statsB;
    statsB.PageCount = 2;
    statsB.PageAlloc = 3;
    statsB.CursorCount = 4;
    statsB.NodeCount = 101;
    statsB.NodeDeref = 102;
    statsB.Rebalance = 1001;
    statsB.RebalanceTime = std::chrono::seconds(1002);
    statsB.Split = 11001;
    statsB.Spill = 11002;
    statsB.SpillTime = std::chrono::seconds(11002);
    statsB.Write = 110001;
    statsB.WriteTime = std::chrono::seconds(110010);
    
    // Add statsA to statsB
    statsB.add(statsA);
    
    // Verify results
    EXPECT_EQ(3, statsB.PageCount);
    EXPECT_EQ(5, statsB.PageAlloc);
    EXPECT_EQ(7, statsB.CursorCount);
    EXPECT_EQ(201, statsB.NodeCount);
    EXPECT_EQ(203, statsB.NodeDeref);
    EXPECT_EQ(2001, statsB.Rebalance);
    EXPECT_EQ(std::chrono::seconds(2003).count(), statsB.RebalanceTime.count());
    EXPECT_EQ(21001, statsB.Split);
    EXPECT_EQ(21003, statsB.Spill);
    EXPECT_EQ(std::chrono::seconds(21003).count(), statsB.SpillTime.count());
    EXPECT_EQ(210001, statsB.Write);
    EXPECT_EQ(std::chrono::seconds(210011).count(), statsB.WriteTime.count());
    
    std::cout << "TestTxStats_add passed" << std::endl;
}

TEST_F(TxStatsTest, TestTxStats_initialization) {
    std::cout << "Running TestTxStats_initialization..." << std::endl;
    
    bboltpp::TxStats stats;
    
    // Verify initial values are zero
    EXPECT_EQ(0, stats.PageCount);
    EXPECT_EQ(0, stats.PageAlloc);
    EXPECT_EQ(0, stats.CursorCount);
    EXPECT_EQ(0, stats.NodeCount);
    EXPECT_EQ(0, stats.NodeDeref);
    EXPECT_EQ(0, stats.Rebalance);
    EXPECT_EQ(std::chrono::seconds(0).count(), stats.RebalanceTime.count());
    EXPECT_EQ(0, stats.Split);
    EXPECT_EQ(0, stats.Spill);
    EXPECT_EQ(std::chrono::seconds(0).count(), stats.SpillTime.count());
    EXPECT_EQ(0, stats.Write);
    EXPECT_EQ(std::chrono::seconds(0).count(), stats.WriteTime.count());
    
    std::cout << "TestTxStats_initialization passed" << std::endl;
}

TEST_F(TxStatsTest, TestTxStats_accumulation) {
    std::cout << "Running TestTxStats_accumulation..." << std::endl;
    
    bboltpp::TxStats stats;
    
    // Add multiple stats
    for (int i = 0; i < 10; i++) {
        bboltpp::TxStats temp;
        temp.PageCount = 1;
        temp.PageAlloc = 2;
        temp.CursorCount = 3;
        temp.NodeCount = 4;
        temp.NodeDeref = 5;
        temp.Rebalance = 6;
        temp.RebalanceTime = std::chrono::milliseconds(100);
        temp.Split = 7;
        temp.Spill = 8;
        temp.SpillTime = std::chrono::milliseconds(200);
        temp.Write = 9;
        temp.WriteTime = std::chrono::milliseconds(300);
        
        stats.add(temp);
    }
    
    // Verify accumulated values
    EXPECT_EQ(10, stats.PageCount);
    EXPECT_EQ(20, stats.PageAlloc);
    EXPECT_EQ(30, stats.CursorCount);
    EXPECT_EQ(40, stats.NodeCount);
    EXPECT_EQ(50, stats.NodeDeref);
    EXPECT_EQ(60, stats.Rebalance);
    EXPECT_EQ(std::chrono::milliseconds(1000).count(), stats.RebalanceTime.count());
    EXPECT_EQ(70, stats.Split);
    EXPECT_EQ(80, stats.Spill);
    EXPECT_EQ(std::chrono::milliseconds(2000).count(), stats.SpillTime.count());
    EXPECT_EQ(90, stats.Write);
    EXPECT_EQ(std::chrono::milliseconds(3000).count(), stats.WriteTime.count());
    
    std::cout << "TestTxStats_accumulation passed" << std::endl;
}

TEST_F(TxStatsTest, TestTxStats_copy) {
    std::cout << "Running TestTxStats_copy..." << std::endl;
    
    bboltpp::TxStats original;
    original.PageCount = 1;
    original.PageAlloc = 2;
    original.CursorCount = 3;
    original.NodeCount = 4;
    original.NodeDeref = 5;
    original.Rebalance = 6;
    original.RebalanceTime = std::chrono::seconds(7);
    original.Split = 8;
    original.Spill = 9;
    original.SpillTime = std::chrono::seconds(10);
    original.Write = 11;
    original.WriteTime = std::chrono::seconds(12);
    
    // Copy stats
    bboltpp::TxStats copy = original;
    
    // Verify copy has same values
    EXPECT_EQ(original.PageCount, copy.PageCount);
    EXPECT_EQ(original.PageAlloc, copy.PageAlloc);
    EXPECT_EQ(original.CursorCount, copy.CursorCount);
    EXPECT_EQ(original.NodeCount, copy.NodeCount);
    EXPECT_EQ(original.NodeDeref, copy.NodeDeref);
    EXPECT_EQ(original.Rebalance, copy.Rebalance);
    EXPECT_EQ(original.RebalanceTime.count(), copy.RebalanceTime.count());
    EXPECT_EQ(original.Split, copy.Split);
    EXPECT_EQ(original.Spill, copy.Spill);
    EXPECT_EQ(original.SpillTime.count(), copy.SpillTime.count());
    EXPECT_EQ(original.Write, copy.Write);
    EXPECT_EQ(original.WriteTime.count(), copy.WriteTime.count());
    
    std::cout << "TestTxStats_copy passed" << std::endl;
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
