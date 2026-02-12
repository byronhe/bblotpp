#include "cursor.h"
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <string_view>
#include "absl/strings/str_format.h"
#include "bucket.h"
#include "errors.h"
#include "page.h"

namespace bboltpp {

// First moves the cursor to the first item in the bucket and returns its key and value.
// If the bucket is empty then a nullptr key and value are returned.
// The returned key and value are only valid for the life of the transaction.
KeyValue Cursor::First() {
  _assert(bucket->tx.lock()->GetDB() != nullptr, "tx closed");
  stack.clear();
  auto [p, n] = bucket->pageNode(bucket->bucket.root);
  stack.emplace_back(p, n, 0);
  first();

  // If we land on an empty page then move to the next value.
  // https://github.com/boltdb/bolt/issues/450
  if (stack.back().count() == 0) {
    next();
  }

  auto kvf = keyValue();
  if ((kvf.flags & bucketLeafFlag) != 0) {
    return KeyValue{kvf.key, ""};
  }
  return KeyValue{kvf.key, kvf.value};
}

// Last moves the cursor to the last item in the bucket and returns its key and value.
// If the bucket is empty then a nullptr key and value are returned.
// The returned key and value are only valid for the life of the transaction.
KeyValue Cursor::Last() {
  _assert(bucket->tx.lock()->GetDB() != nullptr, "tx closed");
  stack.clear();
  auto [p, n] = bucket->pageNode(bucket->bucket.root);
  ElemRef ref(p, n);
  ref.index = ref.count() - 1;
  stack.emplace_back(ref);
  last();
  auto kvf = keyValue();
  if ((kvf.flags & bucketLeafFlag) != 0) {
    return KeyValue{kvf.key, ""};
  }
  return KeyValue{kvf.key, kvf.value};
}

// Next moves the cursor to the next item in the bucket and returns its key and value.
// If the cursor is at the end of the bucket then a nullptr key and value are returned.
// The returned key and value are only valid for the life of the transaction.
KeyValue Cursor::Next() {
  _assert(bucket->tx.lock()->GetDB() != nullptr, "tx closed");
  auto kvf = next();
  if ((kvf.flags & bucketLeafFlag) != 0) {
    return KeyValue{kvf.key, ""};
  }
  return KeyValue{kvf.key, kvf.value};
}

// Prev moves the cursor to the previous item in the bucket and returns its key and value.
// If the cursor is at the beginning of the bucket then a nullptr key and value are returned.
// The returned key and value are only valid for the life of the transaction.
KeyValue Cursor::Prev() {
  _assert(bucket->tx.lock()->GetDB() != nullptr, "tx closed");

  // Attempt to move back one element until we're successful.
  // Move up the stack as we hit the beginning of each page in our stack.
  for (size_t i = stack.size() - 1; i >= 0; i--) {
    auto &elem = stack[i];
    if (elem.index > 0) {
      elem.index--;
      break;
    }
    stack.resize(i);
  }

  // If we've hit the end then return nullptr.
  if (stack.empty()) {
    return KeyValue{"", ""};
  }

  // Move down the stack to find the last element of the last leaf under this branch.
  last();
  auto kvf = keyValue();
  if ((kvf.flags & bucketLeafFlag) != 0) {
    return KeyValue{kvf.key, ""};
  }
  return KeyValue{kvf.key, kvf.value};
}

// Seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used. If no keys
// follow, a nullptr key is returned.
// The returned key and value are only valid for the life of the transaction.
KeyValue Cursor::Seek(std::string_view key) {
  auto kvf = seek(key);

  // If we ended up after the last element of a page then move to the next one.
  if (stack.back().index >= stack.back().count()) {
    kvf = next();
  }

  if (kvf.key.empty()) {
    return KeyValue{"", ""};
  } else if ((kvf.flags & bucketLeafFlag) != 0) {
    return KeyValue{kvf.key, ""};
  }
  return KeyValue{kvf.key, kvf.value};
}

// Delete removes the current key/value under the cursor from the bucket.
// Delete fails if current key/value is a bucket or if the transaction is not writable.
ErrorCode Cursor::Delete() {
  if (bucket->tx.lock()->GetDB() == nullptr) {
    return ErrorCode::ErrTxClosed;
  } else if (!bucket->Writable()) {
    return ErrorCode::ErrTxNotWritable;
  }

  auto kvf = keyValue();
  // Return an error if current value is a bucket.
  if ((kvf.flags & bucketLeafFlag) != 0) {
    return ErrorCode::ErrIncompatibleValue;
  }
  node()->del(kvf.key);
  return ErrorCode::OK;
}

// seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used.
KeyValueFlags Cursor::seek(std::string_view seek) {
  _assert(bucket->tx.lock()->GetDB() != nullptr, "tx closed");

  // Start from root page/node and traverse to correct page.
  stack.clear();
  search(seek, bucket->bucket.root);

  // If this is a bucket then return a nullptr value.
  return keyValue();
}

// first moves the cursor to the first leaf element under the last page in the stack.
void Cursor::first() {
  for (;;) {
    // Exit when we hit a leaf page.
    auto &ref = stack.back();
    if (ref.isLeaf()) {
      break;
    }

    // Keep adding pages pointing to the first element to the stack.
    pgid_t pgid = 0;
    if (ref.node != nullptr) {
      pgid = ref.node->inodes[ref.index].pgid;
    } else {
      pgid = ref.page->branchPageElement(uint16_t(ref.index))->pgid;
    }
    auto [p, n] = bucket->pageNode(pgid);
    stack.emplace_back(p, n, 0);
  }
}

// last moves the cursor to the last leaf element under the last page in the stack.
void Cursor::last() {
  for (;;) {
    // Exit when we hit a leaf page.
    auto &ref = stack.back();
    if (ref.isLeaf()) {
      break;
    }

    // Keep adding pages pointing to the last element in the stack.
    pgid_t pgid = 0;
    if (ref.node != nullptr) {
      pgid = ref.node->inodes[ref.index].pgid;
    } else {
      pgid = ref.page->branchPageElement(uint16_t(ref.index))->pgid;
    }
    auto [p, n] = bucket->pageNode(pgid);

    ElemRef nextRef{p, n};
    nextRef.index = nextRef.count() - 1;
    stack.emplace_back(nextRef);
  }
}

// next moves to the next leaf element and returns the key and value.
// If the cursor is at the last leaf element then it stays there and returns nullptr.
KeyValueFlags Cursor::next() {
  for (;;) {
    // Attempt to move over one element until we're successful.
    // Move up the stack as we hit the end of each page in our stack.
    int i = stack.size() - 1;
    for (; i >= 0; i--) {
      auto &elem = stack[i];
      if (elem.index < elem.count() - 1) {
        elem.index++;
        break;
      }
    }

    // If we've hit the root page then stop and return. This will leave the
    // cursor on the last element of the last page.
    if (i == -1) {
      return KeyValueFlags{"", "", 0};
    }

    // Otherwise start from where we left off in the stack and find the
    // first element of the first leaf page.
    stack.resize(i + 1);
    first();

    // If this is an empty page then restart and move back up the stack.
    // https://github.com/boltdb/bolt/issues/450
    if (stack.back().count() == 0) {
      continue;
    }

    return keyValue();
  }
}

// search recursively performs a binary search against a given page/node until it finds a given key.
void Cursor::search(std::string_view key, pgid_t pgid) {
  auto [p, n] = bucket->pageNode(pgid);
  if (p != nullptr && (p->flags & (branchPageFlag | leafPageFlag)) == 0) {
    panic(absl::StrFormat("invalid page type: %lu: %x", p->id, p->flags));
  }
  ElemRef e{p, n};
  stack.emplace_back(e);

  // If we're on a leaf page/node then find the specific node.
  if (e.isLeaf()) {
    nsearch(key);
    return;
  }

  if (n != nullptr) {
    searchNode(key, n);
    return;
  }
  if (p != nullptr) {
    searchPage(key, p);
  }
}

void Cursor::searchNode(std::string_view key, std::shared_ptr<Node> n) {
  bool exact = false;
  // index := sort.Search(len(n.inodes), func(i int) bool {
  // 	// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
  // 	// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
  // 	ret := bytes.Compare(n.inodes[i].key, key)
  // 	if (ret == 0 ){
  // 		exact = true
  // 	}
  // 	return ret != -1
  // })
  auto it = std::lower_bound(n->inodes.begin(), n->inodes.end(), key,
                             [&](const auto &n, const std::string_view &key) { return n.key < key; });

  if (it != n->inodes.end()) {
    exact = (it->key == key);
  }

  size_t index = it - n->inodes.begin();
  if (!exact && index > 0) {
    index--;
  }
  stack.back().index = index;

  // Recursively search to the next page.
  search(key, n->inodes[index].pgid);
}

void Cursor::searchPage(std::string_view key, Page *p) {
  // Binary search for the correct range.
  auto [inodes, out_count] = p->branchPageElements();

  bool exact = false;
  auto it = std::lower_bound(inodes, inodes + out_count, key,
                             [&](const auto &p, const std::string_view &key) { return p.key() < key; });

  size_t index = it - inodes;
  if (!exact && index > 0) {
    index--;
  }
  stack.back().index = index;

  // Recursively search to the next page.
  search(key, inodes[index].pgid);
}

// nsearch searches the leaf node on the top of the stack for a key.
void Cursor::nsearch(std::string_view key) {
  auto &e = stack.back();
  auto p = e.page;
  auto n = e.node;

  // If we have a node then search its inodes.
  if (n != nullptr) {
    auto it = std::lower_bound(n->inodes.begin(), n->inodes.end(), key,
                               [&](const auto &in, const auto &key) { return in.key < key; });

    e.index = it - n->inodes.begin();
    return;
  }

  // If we have a page then search its leaf elements.
  auto [inodes, out_count] = p->leafPageElements();
  auto it = std::lower_bound(inodes, inodes + out_count, key,
                             [&](const auto &in, const auto &key) { return in.key() < key; });
  e.index = it - inodes;
}

// keyValue returns the key and value of the current leaf element.
KeyValueFlags Cursor::keyValue() {
  auto &ref = stack.back();

  // If the cursor is pointing to the end of page/node then return nullptr.
  if (ref.count() == 0 || ref.index >= ref.count()) {
    return KeyValueFlags{"", "", 0};
  }

  // Retrieve value from node.
  if (ref.node != nullptr) {
    auto &inode = ref.node->inodes[ref.index];
    return KeyValueFlags{std::string_view(inode.key), std::string_view(inode.value), inode.flags};
  }

  // Or retrieve value from page.
  auto *elem = ref.page->leafPageElement(ref.index);
  return KeyValueFlags{elem->key(), elem->value(), elem->flags};
}

// node returns the node that the cursor is currently positioned on.
std::shared_ptr<Node> Cursor::node() {
  _assert(stack.size() > 0, "accessing a node with a zero-length cursor stack");

  // If the top of the stack is a leaf node then just return it.
  if (auto &ref = stack.back(); ref.node != nullptr && ref.isLeaf()) {
    return ref.node;
  }

  // Start from root and traverse down the hierarchy.
  auto n = stack.front().node;
  if (n == nullptr) {
    if (stack.front().page != nullptr) {
      n = bucket->node(stack.front().page->id, nullptr);
    } else {
      // Inline bucket with no page — use pgid 0
      n = bucket->node(0, nullptr);
    }
  }

  // Traverse stack excluding the last element (which is a leaf).
  for (size_t i = 0; i + 1 < stack.size(); i++) {
    _assert(!n->isLeaf, "expected branch node");
    n = n->childAt(stack[i].index);
  }
  _assert(n->isLeaf, "expected leaf node");
  return n;
}

// isLeaf returns whether the ref is pointing at a leaf page/node.
bool ElemRef::isLeaf() const {
  if (node != nullptr) {
    return node->isLeaf;
  }
  if (page != nullptr) {
    return (page->flags & leafPageFlag) != 0;
  }
  return false;  // Default to non-leaf if both are null
}

// count returns the number of inodes or page elements.
int ElemRef::count() const {
  if (node != nullptr) {
    return node->inodes.size();
  }
  if (page != nullptr) {
    return page->count;
  }
  return 0;  // Default to 0 if both are null
}
}  // namespace bboltpp
