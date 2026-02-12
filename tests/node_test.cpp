#include <gtest/gtest.h>
#include "../db.h"
#include "../bucket.h"
#include "../node.h"
#include "../page.h"
#include "../logger.h"
#include "test_common.h"
#include <iostream>
#include <memory>
#include <cstring>
#include <filesystem>

using namespace bboltpp;

class NodeTest : public ::testing::Test {
protected:
    void SetUp() override {
        bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
        cleanupTestFiles();
    }
    
    void TearDown() override {
        cleanupTestFiles();
    }
};

// Test that a node can insert a key/value.
TEST_F(NodeTest, TestNodePut) {
    // Create a mock transaction and bucket
    auto [db, err] = DB::Open("test_node_put.db", 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test");
    EXPECT_EQ(ErrorCode::OK, bucketErr);
    
    // Create a node
    auto n = std::make_shared<Node>();
    n->bucket = bucket;
    n->inodes.clear();
    
    // Insert keys in specific order
    n->put("baz", "baz", "2", 0, 0);
    n->put("foo", "foo", "0", 0, 0);
    n->put("bar", "bar", "1", 0, 0);
    n->put("foo", "foo", "3", 0, leafPageFlag);
    
    // Check that we have 3 inodes (foo should be updated, not added)
    EXPECT_EQ(3, n->inodes.size());
    
    // Check the order (should be sorted)
    EXPECT_EQ("bar", n->inodes[0].key);
    EXPECT_EQ("1", n->inodes[0].value);
    
    EXPECT_EQ("baz", n->inodes[1].key);
    EXPECT_EQ("2", n->inodes[1].value);
    
    EXPECT_EQ("foo", n->inodes[2].key);
    EXPECT_EQ("3", n->inodes[2].value);
    EXPECT_EQ(leafPageFlag, n->inodes[2].flags);
    
    tx->Rollback();
    db->Close();
}

// Test that a node can deserialize from a leaf page.
TEST_F(NodeTest, TestNodeReadLeafPage) {
    // Create a page buffer
    constexpr size_t pageSize = 4096;
    std::vector<uint8_t> buf(pageSize, 0);
    Page* page = reinterpret_cast<Page*>(buf.data());
    
    // Set up page header
    page->flags = leafPageFlag;
    page->count = 2;
    
    // Create leaf page elements
    auto [elements, count] = page->leafPageElements();
    elements[0] = LeafPageElement{0, 32, 3, 4};  // pos = sizeof(leafPageElement) * 2
    elements[1] = LeafPageElement{0, 23, 10, 3}; // pos = sizeof(leafPageElement) + 3 + 4
    
    // Write data for the nodes at the end
    const char* s = "barfoozhelloworldbye";
    char* data = reinterpret_cast<char*>(page) + sizeof(Page) + leafPageElementSize * 2;
    std::memcpy(data, s, std::strlen(s));
    
    // Deserialize page into a leaf
    auto n = std::make_shared<Node>();
    n->read(page);
    
    // Check that there are two inodes with correct data
    EXPECT_TRUE(n->isLeaf);
    EXPECT_EQ(2, n->inodes.size());
    
    EXPECT_EQ("bar", n->inodes[0].key);
    EXPECT_EQ("fooz", n->inodes[0].value);
    
    EXPECT_EQ("helloworld", n->inodes[1].key);
    EXPECT_EQ("bye", n->inodes[1].value);
    
}

// Test that a node can serialize into a leaf page.
TEST_F(NodeTest, TestNodeWriteLeafPage) {
    // Create a mock transaction and bucket
    auto [db, err] = DB::Open("test_node_write.db", 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test");
    EXPECT_EQ(ErrorCode::OK, bucketErr);
    
    // Create a node
    auto n = std::make_shared<Node>();
    n->isLeaf = true;
    n->bucket = bucket;
    n->inodes.clear();
    
    n->put("susy", "susy", "que", 0, 0);
    n->put("ricki", "ricki", "lake", 0, 0);
    n->put("john", "john", "johnson", 0, 0);
    
    // Write it to a page
    constexpr size_t pageSize = 4096;
    std::vector<uint8_t> buf(pageSize, 0);
    Page* p = reinterpret_cast<Page*>(buf.data());
    n->write(p);
    
    // Read the page back in
    auto n2 = std::make_shared<Node>();
    n2->read(p);
    
    // Check that the two pages are the same
    EXPECT_EQ(3, n2->inodes.size());
    
    EXPECT_EQ("john", n2->inodes[0].key);
    EXPECT_EQ("johnson", n2->inodes[0].value);
    
    EXPECT_EQ("ricki", n2->inodes[1].key);
    EXPECT_EQ("lake", n2->inodes[1].value);
    
    EXPECT_EQ("susy", n2->inodes[2].key);
    EXPECT_EQ("que", n2->inodes[2].value);
    
    tx->Rollback();
    db->Close();
}

// Test that a node can split into appropriate subgroups.
TEST_F(NodeTest, TestNodeSplit) {
    // Create a mock transaction and bucket
    auto [db, err] = DB::Open("test_node_split.db", 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test");
    EXPECT_EQ(ErrorCode::OK, bucketErr);
    
    // Create a node
    auto n = std::make_shared<Node>();
    n->bucket = bucket;
    n->inodes.clear();
    
    n->put("00000001", "00000001", "0123456701234567", 0, 0);
    n->put("00000002", "00000002", "0123456701234567", 0, 0);
    n->put("00000003", "00000003", "0123456701234567", 0, 0);
    n->put("00000004", "00000004", "0123456701234567", 0, 0);
    n->put("00000005", "00000005", "0123456701234567", 0, 0);
    
    // Split between 2 & 3
    n->split(100);
    
    auto parent = n->parent.lock();
    EXPECT_NE(nullptr, parent);
    EXPECT_EQ(2, parent->children.size());
    EXPECT_EQ(2, parent->children[0]->inodes.size());
    EXPECT_EQ(3, parent->children[1]->inodes.size());
    
    tx->Rollback();
    db->Close();
}

// Test that a page with the minimum number of inodes just returns a single node.
TEST_F(NodeTest, TestNodeSplitMinKeys) {
    // Create a mock transaction and bucket
    auto [db, err] = DB::Open("test_node_split_min.db", 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test");
    EXPECT_EQ(ErrorCode::OK, bucketErr);
    
    // Create a node
    auto n = std::make_shared<Node>();
    n->bucket = bucket;
    n->inodes.clear();
    
    n->put("00000001", "00000001", "0123456701234567", 0, 0);
    n->put("00000002", "00000002", "0123456701234567", 0, 0);
    
    // Split
    n->split(20);
    EXPECT_EQ(nullptr, n->parent.lock());
    
    tx->Rollback();
    db->Close();
}

// Test that a node that has keys that all fit on a page just returns one leaf.
TEST_F(NodeTest, TestNodeSplitSinglePage) {
    // Create a mock transaction and bucket
    auto [db, err] = DB::Open("test_node_split_single.db", 0644, nullptr);
    EXPECT_TRUE(err.OK());
    
    auto [tx, txErr] = db->Begin(true);
    EXPECT_TRUE(txErr.OK());
    
    auto [bucket, bucketErr] = tx->CreateBucket("test");
    EXPECT_EQ(ErrorCode::OK, bucketErr);
    
    // Create a node
    auto n = std::make_shared<Node>();
    n->bucket = bucket;
    n->inodes.clear();
    
    n->put("00000001", "00000001", "0123456701234567", 0, 0);
    n->put("00000002", "00000002", "0123456701234567", 0, 0);
    n->put("00000003", "00000003", "0123456701234567", 0, 0);
    n->put("00000004", "00000004", "0123456701234567", 0, 0);
    n->put("00000005", "00000005", "0123456701234567", 0, 0);
    
    // Split
    n->split(4096);
    EXPECT_EQ(nullptr, n->parent.lock());
    
    tx->Rollback();
    db->Close();
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
