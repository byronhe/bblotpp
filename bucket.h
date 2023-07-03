#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <tuple>
#include "cursor.h"
#include "errors.h"
#include "node.h"
#include "page.h"
#include "tx.h"

namespace bboltpp {

// MaxKeySize is the maximum length of a key, in bytes.
constexpr std::size_t MaxKeySize = 32768;

// MaxValueSize is the maximum length of a value, in bytes.
constexpr std::size_t MaxValueSize = (1UL << 31) - 2;

constexpr double minFillPercent = 0.1;
constexpr double maxFillPercent = 1.0;

// DefaultFillPercent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
constexpr double DefaultFillPercent = 0.5;

struct BucketStats;

// bucket represents the on-file representation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header. In the case of inline buckets, the "root" will be 0.
struct BucketStored {
  pgid_t root;        // page id of the bucket's root-level page
  uint64_t sequence;  // monotonically incrementing, used by NextSequence()

  template <typename H>
  friend H AbslHashValue(H h, const BucketStored &b) {
    return H::combine(std::move(h), b.root, b.sequence);
  }
};
constexpr std::size_t bucketHeaderSize = sizeof(BucketStored);

// Bucket represents a collection of key/value pairs inside the database.
struct Bucket {
  struct BucketStored bucket;               // *bucket
  Tx *tx = nullptr;                         // the associated transaction
  std::map<std::string, Bucket *> buckets;  // subbucket cache
  Page *page = nullptr;                     // inline page reference
  Node *rootNode = nullptr;                 // materialized node for the root page.
  std::map<pgid_t, Node *> nodes;           // node cache

  // Sets the threshold for filling nodes when they split. By default,
  // the bucket will fill to 50% but it can be useful to increase this
  // amount if you know that your write workloads are mostly append-only.
  //
  // This is non-persisted across transactions so it must be set in every Tx.
  double FillPercent = DefaultFillPercent;

  Tx *GetTx() { return tx; }
  pgid_t Root() const;
  bool Writable() const;
  Cursor *NewCursor();
  Bucket *FindBucketByName(std::string_view name);
  Bucket *openBucket(std::string_view value);
  std::tuple<Bucket *, ErrorCode> CreateBucket(std::string_view key);
  std::tuple<Bucket *, ErrorCode> CreateBucketIfNotExists(std::string_view key);

  ErrorCode DeleteBucket(std::string_view key);
  std::optional<std::string_view> Get(std::string_view key);
  ErrorCode Put(std::string_view key, std::string_view value);
  ErrorCode Delete(std::string_view key);
  uint64_t Sequence();
  ErrorCode SetSequence(uint64_t v);
  std::tuple<uint64_t, ErrorCode> NextSequence();
  ErrorCode ForEach(std::function<ErrorCode(std::string_view, std::string_view)> fn);
  BucketStats Stats();
  void forEachPage(std::function<void(struct Page *, int)> fn);
  void forEachPageNode(std::function<void(struct Page *, struct Node *, int)> fn);

  void _forEachPageNode(pgid_t pgid, int depth, std::function<void(struct Page *, struct Node *, int)> fn);
  ErrorCode spill();
  bool inlineable();
  int maxInlineBucketSize();
  std::string write();
  void rebalance();
  Node *node(pgid_t pgid, Node *parent);
  void free();
  void dereference();
  std::tuple<struct Page *, struct Node *> pageNode(pgid_t id);
};
Bucket *newBucket(Tx *tx);

// BucketStats records statistics about resources used by a bucket.
struct BucketStats {
  // Page count statistics.
  int BranchPageN = 0;      // number of logical branch pages
  int BranchOverflowN = 0;  // number of physical branch overflow pages
  int LeafPageN = 0;        // number of logical leaf pages
  int LeafOverflowN = 0;    // number of physical leaf overflow pages

  // Tree statistics.
  int KeyN = 0;   // number of keys/value pairs
  int Depth = 0;  // number of levels in B+tree

  // Page size utilization.
  int BranchAlloc = 0;  // bytes allocated for physical branch pages
  int BranchInuse = 0;  // bytes actually used for branch data
  int LeafAlloc = 0;    // bytes allocated for physical leaf pages
  int LeafInuse = 0;    // bytes actually used for leaf data

  // Bucket statistics
  int BucketN = 0;            // total number of buckets including the top bucket
  int InlineBucketN = 0;      // total number on inlined buckets
  int InlineBucketInuse = 0;  // bytes used for inlined buckets (also accounted for
                              // in LeafInuse)

  void Add(const BucketStats &other);
};

}  // namespace bboltpp
