#pragma once
#include <cstdint>
#include <memory>
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
  uint32_t flags = 0;
  pgid_t pgid = 0;
  std::string key;
  std::string value;
};

// node represents an in-memory, deserialized page.
struct Node : public std::enable_shared_from_this<Node> {
  std::weak_ptr<Bucket> bucket;
  bool isLeaf = false;
  bool unbalanced = false;
  bool spilled = false;
  std::string key;
  pgid_t pgid = 0;
  std::weak_ptr<Node> parent;
  std::vector<std::shared_ptr<Node>> children;
  std::vector<INode> inodes;

  std::shared_ptr<Node> root();
  int minKeys();
  int size();
  bool sizeLessThan(int v);
  int pageElementSize();
  std::shared_ptr<Node> childAt(int index);
  int childIndex(std::shared_ptr<Node> child);
  int numChildren();
  std::shared_ptr<Node> nextSibling();
  std::shared_ptr<Node> prevSibling();
  void put(std::string_view oldKey, std::string_view newKey, std::string_view value, pgid_t pgid, uint32_t flags);
  void del(std::string_view key);
  void read(Page* p);
  void write(Page* p);
  std::vector<std::shared_ptr<Node>> split(int pageSize);
  std::tuple<std::shared_ptr<Node>, std::shared_ptr<Node>> splitTwo(int pageSize);
  std::tuple<int, int> splitIndex(int threshold);
  ErrorCode spill();
  void rebalance();
  void removeChild(std::shared_ptr<Node> target);
  void dereference();
  void free();
  void dump();
};

}  // namespace bboltpp
