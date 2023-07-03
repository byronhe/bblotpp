#pragma once
#include <cstdint>
#include <string_view>
#include <tuple>
#include <vector>
#include "errors.h"
#include "page.h"
namespace bboltpp {

struct Bucket;

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
struct INode {
  uint32_t flags;
  pgid_t pgid;
  std::string_view key;
  std::string_view value;
};

using INodeVec = std::vector<INode>;

struct Node;
using nodes = std::vector<Node *>;

// node represents an in-memory, deserialized page.
struct Node {
  Bucket *bucket = nullptr;
  bool isLeaf = false;
  bool unbalanced = false;
  bool spilled = false;
  std::string key;
  pgid_t pgid = 0;
  Node *parent = nullptr;
  nodes children;
  INodeVec inodes;

  Node *root();
  int minKeys();
  int size();
  bool sizeLessThan(int v);
  int pageElementSize();
  Node *childAt(int index);
  int childIndex(Node *child);
  int numChildren();
  Node *nextSibling();
  Node *prevSibling();
  void put(std::string_view oldKey, std::string_view newKey, std::string_view value, pgid_t pgid, uint32_t flags);
  void del(std::string_view key);
  void read(Page *p);
  void write(Page *p);
  std::vector<Node *> split(int pageSize);
  std::tuple<struct Node *, struct Node *> splitTwo(int pageSize);
  std::tuple<int, int> splitIndex(int threshold);
  ErrorCode spill();
  void rebalance();
  void removeChild(Node *target);
  void dereference();
  void free();
  void dump();
};

}  // namespace bboltpp
