#pragma once
#include <cstdint>
#include <functional>
#include <vector>

#include "errors.h"
#include "page.h"
#include "tx.h"
namespace bboltpp {

// FreelistType is the type of the freelist backend

enum class FreelistType {
  // FreelistArrayType indicates backend freelist type is array
  FreelistArrayType = 0,  // FreelistType("array")
  // FreelistMapType indicates backend freelist type is hashmap
  FreelistMapType = 1,  // FreelistType("hashmap")
};

// txPending holds a list of pgid_vec and corresponding allocation txns
// that are pending to be freed.
struct TxPending {
  pgid_vec ids;
  std::vector<txid_t> alloctx;  // txids allocating the ids
  txid_t lastReleaseBegin = 0;  // beginning txid of last matching releaseRange
};

// pidSet holds the set of starting pgid_vec which have the same span size
using pidSet = std::map<pgid_t, void *>;  // pgid]struct{}

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open
// transactions.
struct FreeList {
  FreelistType freelistType;               // freelist type
  pgid_vec ids;                            // all free and available free page ids.
  std::map<pgid_t, txid_t> allocs;         // mapping of txid that allocated a pgid.
  std::map<txid_t, TxPending> pending;     // mapping of soon-to-be free page ids by tx.
  std::map<pgid_t, bool> cache;            // fast lookup of all free and pending page ids.
  std::map<uint64_t, pidSet> freemaps;     // key is the size of continuous pages(span), value is a set
                                           // which contains the starting pgid_vec of same size
  std::map<pgid_t, uint64_t> forwardMap;   // key is start pgid, value is its span size
  std::map<pgid_t, uint64_t> backwardMap;  // key is end pgid, value is its span size

  virtual ~FreeList() = default;
  virtual pgid_t Allocate(txid_t, int) = 0;      // the freelist allocate func
  virtual int FreeCount() = 0;                   // the function which gives you free page number
  virtual void MergeSpans(pgid_vec &pgids) = 0;  // the mergeSpan func
  virtual pgid_vec GetFreePageIDs() = 0;         // get free pgid_vec func
  virtual void ReadIDs(pgid_vec &) = 0;          // readIDs func reads list of pages and init the freelist

  void reindex();
  int size();
  int count();
  int pending_count();
  void copyall(pgid_vec &dst);
  void free(txid_t txid, Page *p);
  void release(txid_t txid);
  void releaseRange(txid_t begin, txid_t end);
  void rollback(txid_t txid);
  bool freed(pgid_t pgid);
  void read(Page *p);
  ErrorCode write(Page *p);
  void reload(Page *p);
  void noSyncReload(pgid_vec &pgids);
};

class HashMapFreeList : public FreeList {
 public:
  pgid_t Allocate(txid_t txid, int n) override;
  int FreeCount() override;
  void MergeSpans(pgid_vec &pgids) override;
  pgid_vec GetFreePageIDs() override;
  void ReadIDs(pgid_vec &pgids) override;

 private:
  void mergeWithExistingSpan(pgid_t pid);
  void addSpan(pgid_t start, uint64_t size);
  void delSpan(pgid_t start, uint64_t size);
  void init(const pgid_vec &pgids);
};

class ArrayFreeList : public FreeList {
 public:
  pgid_t Allocate(txid_t txid, int n) override;
  int FreeCount() override;
  void MergeSpans(pgid_vec &pgids) override;
  pgid_vec GetFreePageIDs() override;
  void ReadIDs(pgid_vec &pgids) override;
};

std::unique_ptr<FreeList> newFreelist(FreelistType type);

}  // namespace bboltpp
