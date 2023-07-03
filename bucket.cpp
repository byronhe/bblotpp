#include "bucket.h"
#include <cstdint>
#include <cstring>
#include <functional>
#include <optional>
#include <string_view>
#include "absl/strings/str_format.h"
#include "cursor.h"
#include "db.h"
#include "errors.h"
#include "page.h"

using std::string;
using std::string_view;

namespace bboltpp {

// newBucket returns a new bucket associated with a transaction.
Bucket *newBucket(Tx *tx) {
  auto b = new Bucket();
  b->tx = tx;
  b->FillPercent = DefaultFillPercent;

  if (tx->writable) {
    // b->buckets = make(map[string]*Bucket)
    // b->nodes = make(map[pgid]*node)
  }
  return b;
}

// Root returns the root of the bucket.
pgid_t Bucket::Root() const { return bucket.root; }

// Writable returns whether the bucket is writable.
bool Bucket::Writable() const { return tx->writable; }

// Cursor creates a cursor associated with the bucket.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
Cursor *Bucket::NewCursor() {
  // Update transaction statistics.
  tx->stats.CursorCount++;

  // Allocate and return a cursor.
  auto cursor = new struct Cursor();
  cursor->bucket = this;
  return cursor;
}

// Bucket retrieves a nested bucket by name.
// Returns nullptr if the bucket does not exist.
// The bucket instance is only valid for the lifetime of the transaction.
Bucket *Bucket::FindBucketByName(string_view name) {
  if (buckets.empty()) {
    return buckets[std::string(name)] = newBucket(nullptr);
  }

  // Move cursor to key.
  auto c = NewCursor();
  auto kvf = c->seek(name);
  // k, v, flags :=

  // Return nullptr if the key doesn't exist or it is not a bucket.
  if ((name != kvf.key) || ((kvf.flags & bucketLeafFlag) == 0)) {
    return nullptr;
  }

  // Otherwise create a bucket and cache it.
  auto child = openBucket(kvf.value);
  if (!buckets.empty()) {
    buckets[std::string(name)] = child;
  }

  return child;
}

// Helper method that re-interprets a sub-bucket value
// from a parent into a Bucket
Bucket *Bucket::openBucket(string_view value) {
  auto child = newBucket(tx);

  // If this is a writable transaction then we need to copy the bucket entry.
  // Read-only transactions can point directly at the mmap entry.
  memcpy(&child->bucket, value.data(), sizeof(child->bucket));

  // Save a reference to the inline page if the bucket is inline.
  if (child->bucket.root == 0) {
    child->page = reinterpret_cast<struct Page *>(const_cast<char *>(&value[bucketHeaderSize]));
  }

  return child;
}

// CreateBucket creates a new bucket at the given key and returns the new bucket.
// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
std::tuple<Bucket *, ErrorCode> Bucket::CreateBucket(string_view key) {
  if (tx->db == nullptr) {
    return {nullptr, ErrorCode::ErrTxClosed};
  } else if (!tx->writable) {
    return {nullptr, ErrorCode::ErrTxNotWritable};
  } else if (key.empty()) {
    return {nullptr, ErrorCode::ErrBucketNameRequired};
  }

  // Move cursor to correct position.
  auto c = NewCursor();
  auto kvf = c->seek(key);

  // Return an error if there is an existing key.
  if (kvf.key == key) {
    if ((kvf.flags & bucketLeafFlag) != 0) {
      return {nullptr, ErrorCode::ErrBucketExists};
    }
    return {nullptr, ErrorCode::ErrIncompatibleValue};
  }

  // Create empty, inline bucket.
  auto rootNode = new struct Node;
  rootNode->isLeaf = true;
  Bucket bucket = {
      .rootNode = rootNode,
      .FillPercent = DefaultFillPercent,
  };
  auto value = bucket.write();

  // Insert into node.
  // TODO(memory lifetime issue)
  // key = cloneBytes(key)
  c->node()->put(key, key, value, 0, bucketLeafFlag);

  // Since subbuckets are not allowed on inline buckets, we need to
  // dereference the inline page, if it exists. This will cause the bucket
  // to be treated as a regular, non-inline bucket for the rest of the tx.
  page = nullptr;
  return {FindBucketByName(key), ErrorCode::OK};
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist and returns a reference to it.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
std::tuple<Bucket *, ErrorCode> Bucket::CreateBucketIfNotExists(string_view key) {
  auto [child, err] = CreateBucket(key);
  if (err == ErrorCode::ErrBucketExists) {
    return {FindBucketByName(key), ErrorCode::OK};
  } else if (err != ErrorCode::OK) {
    return {nullptr, err};
  }
  return {child, ErrorCode::OK};
}

// DeleteBucket deletes a bucket at the given key.
// Returns an error if the bucket does not exist, or if the key represents a non-bucket value.
ErrorCode Bucket::DeleteBucket(string_view key) {
  if (tx->db == nullptr) {
    return ErrorCode::ErrTxClosed;
  } else if (!Writable()) {
    return ErrorCode::ErrTxNotWritable;
  }

  // Move cursor to correct position.
  auto c = NewCursor();
  auto [k, _, flags] = c->seek(key);

  // Return an error if bucket doesn't exist or is not a bucket.
  if (key != k) {
    return ErrorCode::ErrBucketNotFound;
  } else if ((flags & bucketLeafFlag) == 0) {
    return ErrorCode::ErrIncompatibleValue;
  }

  // Recursively delete all child buckets.
  auto child = FindBucketByName(key);
  auto err = child->ForEach([&](string_view k, string_view v) -> ErrorCode {
    if (auto [_, __, childFlags] = child->NewCursor()->seek(k); (childFlags & bucketLeafFlag) != 0) {
      if (auto err = child->DeleteBucket(k); err != ErrorCode::OK) {
        return err;
        // return fmt.Errorf("delete bucket: %s", err)
      }
    }
    return ErrorCode::OK;
  });
  if (err != ErrorCode::OK) {
    return err;
  }

  // Remove cached copy.
  buckets.erase(std::string(key));

  // Release all bucket pages to freelist.
  child->nodes.clear();
  child->rootNode = nullptr;
  child->free();

  // Delete the node if we have a matching key.
  c->node()->del(key);

  return ErrorCode::OK;
}

// Get retrieves the value for a key in the bucket.
// Returns a nullptr value if the key does not exist or if the key is a nested bucket.
// The returned value is only valid for the life of the transaction.
std::optional<string_view> Bucket::Get(string_view key) {
  auto [k, v, flags] = NewCursor()->seek(key);

  // Return nullptr if this is a bucket.
  if ((flags & bucketLeafFlag) != 0) {
    return std::nullopt;
  }

  // If our target node isn't the same key as what's passed in then return nullptr.
  if (key != k) {
    return std::nullopt;
  }
  return v;
}

// Put sets the value for a key in the bucket.
// If the key exist then its previous value will be overwritten.
// Supplied value must remain valid for the life of the transaction.
// Returns an error if the bucket was created from a read-only transaction, if the key is blank, if the key is too
// large, or if the value is too large.
ErrorCode Bucket::Put(string_view key, string_view value) {
  if (tx->db == nullptr) {
    return ErrorCode::ErrTxClosed;
  } else if (!Writable()) {
    return ErrorCode::ErrTxNotWritable;
  } else if (key.empty()) {
    return ErrorCode::ErrKeyRequired;
  } else if (key.size() > MaxKeySize) {
    return ErrorCode::ErrKeyTooLarge;
  } else if (value.size() > MaxValueSize) {
    return ErrorCode::ErrValueTooLarge;
  }

  // Move cursor to correct position.
  auto c = NewCursor();
  auto [k, _, flags] = c->seek(key);

  // Return an error if there is an existing key with a bucket value.
  if ((key == k) && (flags & bucketLeafFlag) != 0) {
    return ErrorCode::ErrIncompatibleValue;
  }

  // Insert into node.
  // TODO(memory lifetime issue)
  // key = cloneBytes(key)
  c->node()->put(key, key, value, 0, 0);

  return ErrorCode::OK;
}

// Delete removes a key from the bucket.
// If the key does not exist then nothing is done and a nullptr error is returned.
// Returns an error if the bucket was created from a read-only transaction.
ErrorCode Bucket::Delete(string_view key) {
  if (tx->db == nullptr) {
    return ErrorCode::ErrTxClosed;
  } else if (!Writable()) {
    return ErrorCode::ErrTxNotWritable;
  }

  // Move cursor to correct position.
  auto c = NewCursor();
  auto [k, _, flags] = c->seek(key);

  // Return nullptr if the key doesn't exist.
  if (key != k) {
    return ErrorCode::OK;
  }

  // Return an error if there is already existing bucket value.
  if ((flags & bucketLeafFlag) != 0) {
    return ErrorCode::ErrIncompatibleValue;
  }

  // Delete the node if we have a matching key.
  c->node()->del(key);

  return ErrorCode::OK;
}

// Sequence returns the current integer for the bucket without incrementing it.
uint64_t Bucket::Sequence() { return bucket.sequence; }

// SetSequence updates the sequence number for the bucket.
ErrorCode Bucket::SetSequence(uint64_t v) {
  if (tx->db == nullptr) {
    return ErrorCode::ErrTxClosed;
  } else if (!Writable()) {
    return ErrorCode::ErrTxNotWritable;
  }

  // Materialize the root node if it hasn't been already so that the
  // bucket will be saved during commit.
  if (rootNode == nullptr) {
    auto _ = node(bucket.root, nullptr);
    (void)_;
  }

  // Increment and return the sequence.
  bucket.sequence = v;
  return ErrorCode::OK;
}

// NextSequence returns an autoincrementing integer for the bucket.
std::tuple<uint64_t, ErrorCode> Bucket::NextSequence() {
  if (tx->db == nullptr) {
    return {0, ErrorCode::ErrTxClosed};
  } else if (!Writable()) {
    return {0, ErrorCode::ErrTxNotWritable};
  }

  // Materialize the root node if it hasn't been already so that the
  // bucket will be saved during commit.
  if (rootNode == nullptr) {
    auto _ = node(bucket.root, nullptr);
    (void)_;
  }

  // Increment and return the sequence.
  bucket.sequence++;
  return {bucket.sequence, ErrorCode::OK};
}

// ForEach executes a function for each key/value pair in a bucket.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller. The provided function must not modify
// the bucket; this will result in undefined behavior.
ErrorCode Bucket::ForEach(std::function<ErrorCode(string_view, string_view)> fn) {
  if (tx->db == nullptr) {
    return ErrorCode::ErrTxClosed;
  }
  auto c = NewCursor();
  for (auto kv = c->First(); !kv.key.empty(); kv = c->Next()) {
    if (auto err = fn(kv.key, kv.value); err != ErrorCode::OK) {
      return err;
    }
  }
  return ErrorCode::OK;
}

// Stat returns stats on a bucket.
BucketStats Bucket::Stats() {
  BucketStats s, subStats;
  auto pageSize = tx->db->pageSize;
  s.BucketN += 1;
  if (bucket.root == 0) {
    s.InlineBucketN += 1;
  }
  forEachPage([&s, &subStats, this](struct Page *p, int depth) {
    if ((p->flags & leafPageFlag) != 0) {
      s.KeyN += int(p->count);

      // used totals the used bytes for the page
      auto used = pageHeaderSize;

      if (p->count != 0) {
        // If page has any elements, add all element headers.
        used += leafPageElementSize * (p->count - 1);

        // Add all element key, value sizes.
        // The computation takes advantage of the fact that the position
        // of the last element's key/value equals to the total of the sizes
        // of all previous elements' keys and values.
        // It also includes the last element's header.
        auto lastElement = p->leafPageElement(p->count - 1);
        used += (lastElement->pos + lastElement->ksize + lastElement->vsize);
      }

      if (bucket.root == 0) {
        // For inlined bucket just update the inline stats
        s.InlineBucketInuse += int(used);
      } else {
        // For non-inlined bucket update all the leaf stats
        s.LeafPageN++;
        s.LeafInuse += int(used);
        s.LeafOverflowN += int(p->overflow);

        // Collect stats from sub-buckets.
        // Do that by iterating over all element headers
        // looking for the ones with the bucketLeafFlag.
        for (auto i = 0; i < p->count; i++) {
          auto e = p->leafPageElement(i);
          if ((e->flags & bucketLeafFlag) != 0) {
            // For any bucket element, open the element value
            // and recursively call Stats on the contained bucket.
            subStats.Add(openBucket(e->value())->Stats());
          }
        }
      }
    } else if ((p->flags & branchPageFlag) != 0) {
      s.BranchPageN++;
      auto lastElement = p->branchPageElement(p->count - 1);

      // used totals the used bytes for the page
      // Add header and all element headers.
      auto used = pageHeaderSize + (branchPageElementSize * (p->count - 1));

      // Add size of all keys and values.
      // Again, use the fact that last element's position equals to
      // the total of key, value sizes of all previous elements.
      used += (lastElement->pos + lastElement->ksize);
      s.BranchInuse += int(used);
      s.BranchOverflowN += int(p->overflow);
    }

    // Keep track of maximum page depth.
    if (depth + 1 > s.Depth) {
      s.Depth = (depth + 1);
    }
  });

  // Alloc stats can be computed from page counts and pageSize.
  s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize;
  s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize;

  // Add the max depth of sub-buckets to get total nested depth.
  s.Depth += subStats.Depth;
  // Add the stats for all sub-buckets
  s.Add(subStats);
  return s;
}

// forEachPage iterates over every page in a bucket, including inline pages.
void Bucket::forEachPage(std::function<void(struct Page *, int)> fn) {
  // If we have an inline page then just use that.
  if (page != nullptr) {
    fn(page, 0);
    return;
  }

  // Otherwise traverse the page hierarchy.
  tx->forEachPage(bucket.root, 0, fn);
}

// forEachPageNode iterates over every page (or node) in a bucket.
// This also includes inline pages.
void Bucket::forEachPageNode(std::function<void(struct Page *, struct Node *, int)> fn) {
  // If we have an inline page or root node then just use that.
  if (page != nullptr) {
    fn(page, nullptr, 0);
    return;
  }
  _forEachPageNode(bucket.root, 0, fn);
}

void Bucket::_forEachPageNode(pgid_t pgid, int depth, std::function<void(struct Page *, struct Node *, int)> fn) {
  auto [p, n] = pageNode(pgid);

  // Execute function.
  fn(p, n, depth);

  // Recursively loop over children.
  if (p != nullptr) {
    if ((p->flags & branchPageFlag) != 0) {
      for (auto i = 0; i < int(p->count); i++) {
        auto elem = p->branchPageElement(i);
        _forEachPageNode(elem->pgid, depth + 1, fn);
      }
    }
  } else {
    if (!n->isLeaf) {
      for (const auto &inode : n->inodes) {
        _forEachPageNode(inode.pgid, depth + 1, fn);
      }
    }
  }
}

// spill writes all the nodes for this bucket to dirty pages.
ErrorCode Bucket::spill() {
  // Spill all child buckets first.
  for (const auto &[name, child] : buckets) {
    // If the child bucket is small enough and it has no child buckets then
    // write it inline into the parent bucket's page. Otherwise spill it
    // like a normal bucket and make the parent value a pointer to the page.
    string value;
    if (child->inlineable()) {
      child->free();
      value = child->write();
    } else {
      if (auto err = child->spill(); err != ErrorCode::OK) {
        return err;
      }

      // Update the child bucket header in this bucket.
      value.assign(reinterpret_cast<const char *>(&child->bucket), sizeof(child->bucket));
    }

    // Skip writing the bucket if there are no materialized nodes.
    if (child->rootNode == nullptr) {
      continue;
    }

    // Update parent node.
    auto c = NewCursor();
    auto [k, _, flags] = c->seek(name);
    if (name != k) {
      panic(absl::StrFormat("misplaced bucket header: %s -> %s", name.data(), k.data()));
    }
    if ((flags & bucketLeafFlag) == 0) {
      panic(absl::StrFormat("unexpected bucket header flag: %x", flags));
    }
    c->node()->put(name, name, value, 0, bucketLeafFlag);
  }

  // Ignore if there's not a materialized root node.
  if (rootNode == nullptr) {
    return ErrorCode::OK;
  }

  // Spill nodes.
  if (auto err = rootNode->spill(); err != ErrorCode::OK) {
    return err;
  }
  rootNode = rootNode->root();

  // Update the root node for this bucket.
  if (rootNode->pgid >= tx->meta->pgid) {
    panic(absl::StrFormat("pgid (%lu) above high water mark (%lu)", rootNode->pgid, tx->meta->pgid));
  }
  bucket.root = rootNode->pgid;

  return ErrorCode::OK;
}

// inlineable returns true if a bucket is small enough to be written inline
// and if it contains no subbuckets. Otherwise returns false.
bool Bucket::inlineable() {
  auto n = rootNode;

  // Bucket must only contain a single leaf node.
  if (n == nullptr || !n->isLeaf) {
    return false;
  }

  // Bucket is not inlineable if it contains subbuckets or if it goes beyond
  // our threshold for inline bucket size.
  auto size = pageHeaderSize;
  for (auto &inode : n->inodes) {
    size += leafPageElementSize + inode.key.size() + inode.value.size();

    if ((inode.flags & bucketLeafFlag) != 0) {
      return false;
    } else if (size > maxInlineBucketSize()) {
      return false;
    }
  }

  return true;
}

// Returns the maximum total size of a bucket to make it a candidate for inlining.
int Bucket::maxInlineBucketSize() { return tx->db->pageSize / 4; }

// write allocates and writes a bucket to a byte slice.
std::string Bucket::write() {
  // Allocate the appropriate size.
  auto n = rootNode;
  string value;
  value.assign(reinterpret_cast<const char *>(&bucket), sizeof(bucket));

  // Convert byte slice to a fake page and write the root node.
  auto p = reinterpret_cast<struct Page *>(&value[bucketHeaderSize]);
  n->write(p);

  return value;
}

// rebalance attempts to balance all nodes.
void Bucket::rebalance() {
  for (auto [_, n] : nodes) {
    n->rebalance();
  }
  for (auto [_, child] : buckets) {
    child->rebalance();
  }
}

// node creates a node from a page and associates it with a given parent.
Node *Bucket::node(pgid_t pgid, struct Node *parent) {
  _assert(!nodes.empty(), "nodes map expected");

  // Retrieve node if it's already been created.
  if (auto it = nodes.find(pgid); it != nodes.end()) {
    return it->second;
  }

  // Otherwise create a node and cache it.
  auto n = new struct Node;
  n->bucket = this;
  n->parent = parent;
  if (parent == nullptr) {
    rootNode = n;
  } else {
    parent->children.emplace_back(n);
  }

  // Use the inline page if this is an inline bucket.
  auto p = page;
  if (p == nullptr) {
    p = tx->page(pgid);
  }

  // Read the page into the node and cache it.
  n->read(p);
  nodes[pgid] = n;

  // Update statistics.
  tx->stats.NodeCount++;

  return n;
}

// free recursively frees all pages in the bucket.
void Bucket::free() {
  if (bucket.root == 0) {
    return;
  }

  forEachPageNode([this](struct Page *p, struct Node *n, int _) {
    if (p != nullptr) {
      tx->db->freelist->free(tx->meta->txid, p);
    } else {
      n->free();
    }
  });
  bucket.root = 0;
}

// dereference removes all references to the old mmap.
void Bucket::dereference() {
  if (rootNode != nullptr) {
    rootNode->root()->dereference();
  }

  for (auto [_, child] : buckets) {
    child->dereference();
  }
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
std::tuple<struct Page *, struct Node *> Bucket::pageNode(pgid_t id) {
  // Inline buckets have a fake page embedded in their value so treat them
  // differently. We'll return the rootNode (if available) or the fake page.
  if (bucket.root == 0) {
    if (id != 0) {
      panic(absl::StrFormat("inline bucket non-zero page access(2): %lu != 0", id));
    }
    if (rootNode != nullptr) {
      return {nullptr, rootNode};
    }
    return {page, nullptr};
  }

  // Check the node cache for non-inline buckets.
  if (!nodes.empty()) {
    if (auto it = nodes.find(id); it != nodes.end()) {
      return {nullptr, it->second};
    }
  }

  // Finally lookup the page from the transaction if no node is materialized.
  return {tx->page(id), nullptr};
}

void BucketStats::Add(const BucketStats &other) {
  BranchPageN += other.BranchPageN;
  BranchOverflowN += other.BranchOverflowN;
  LeafPageN += other.LeafPageN;
  LeafOverflowN += other.LeafOverflowN;
  KeyN += other.KeyN;
  if (Depth < other.Depth) {
    Depth = other.Depth;
  }
  BranchAlloc += other.BranchAlloc;
  BranchInuse += other.BranchInuse;
  LeafAlloc += other.LeafAlloc;
  LeafInuse += other.LeafInuse;

  BucketN += other.BucketN;
  InlineBucketN += other.InlineBucketN;
  InlineBucketInuse += other.InlineBucketInuse;
}

// // cloneBytes returns a copy of a given slice.
// func cloneBytes(v []byte) []byte {
// 	var clone = make([]byte, len(v));
// 	copy(clone, v);
// 	return clone;

}  // namespace bboltpp
