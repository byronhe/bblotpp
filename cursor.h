#pragma once
#include <cstdint>
#include <string_view>

#include "errors.h"
#include "node.h"
#include "page.h"

namespace bboltpp {

struct Bucket;
struct KeyValue {
  std::string_view key;
  std::string_view value;
  bool nil = false;
  explicit KeyValue(std::string_view k, std::string_view v) : key(k), value(v) {}
};

struct KeyValueFlags {
  std::string_view key;
  std::string_view value;
  uint32_t flags = 0;

  // explicit KeyValueFlags() = default;
  explicit KeyValueFlags(std::string_view k, std::string_view v, uint32_t f) : key(k), value(v), flags(f) {}
};

// elemRef represents a reference to an element on a given page/node.
struct ElemRef {
  Page* page = nullptr;
  std::shared_ptr<Node> node = nullptr;
  int index = 0;

  explicit ElemRef() = default;
  explicit ElemRef(Page* p, std::shared_ptr<Node> n, int i = 0) : page(p), node(n), index(i) {}

  // isLeaf returns whether the ref is pointing at a leaf page/node.
  bool isLeaf() const;

  // count returns the number of inodes or page elements.
  int count() const;
};

// Cursor represents an iterator that can traverse over all key/value pairs in a
// bucket in sorted order. Cursors see nested buckets with value == nil. Cursors
// can be obtained from a transaction and are valid as long as the transaction
// is open.
//
// Keys and values returned from the cursor are only valid for the life of the
// transaction.
//
// Changing data while traversing with a cursor may cause it to be invalidated
// and return unexpected keys and/or values. You must reposition your cursor
// after mutating data.
struct Cursor {
  Bucket* bucket = nullptr;
  std::vector<ElemRef> stack;

  // Bucket returns the bucket that this cursor was created from.
  Bucket* GetBucket() const { return bucket; }
  KeyValue First();
  KeyValue Last();
  KeyValue Next();
  KeyValue Prev();
  KeyValue Seek(std::string_view key);
  ErrorCode Delete();

  KeyValueFlags seek(std::string_view key);
  std::shared_ptr<Node> node();

 private:
  void first();
  void last();
  KeyValueFlags next();
  void search(std::string_view key, pgid_t pgid);
  void searchNode(std::string_view key, std::shared_ptr<Node> n);
  void searchPage(std::string_view key, Page* p);
  void nsearch(std::string_view key);
  KeyValueFlags keyValue();
};

}  // namespace bboltpp
