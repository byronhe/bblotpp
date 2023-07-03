#include "freelist.h"
#include <cstdint>
#include <memory>
#include <vector>
#include "absl/strings/str_format.h"
#include "errors.h"
#include "page.h"
#include "tx.h"
#include "unsafe.h"

namespace bboltpp {

// newFreelist returns an empty, initialized freelist.
std::unique_ptr<FreeList> newFreelist(FreelistType type) {
  if (FreelistType::FreelistMapType == type) {
    return std::make_unique<HashMapFreeList>();
  }
  return std::make_unique<ArrayFreeList>();
}

// size returns the size of the page after serialization.
int FreeList::size() {
  auto n = count();
  if (n >= 0xFFFF) {
    // The first element will be used to store the count. See freelist.write.
    n++;
  }
  return int(pageHeaderSize) + (int(sizeof(pgid_t(0))) * n);
}

// count returns count of pages on the freelist
int FreeList::count() { return FreeCount() + pending_count(); }

// arrayFreeCount returns count of free pages(array version)
int ArrayFreeList::FreeCount() { return ids.size(); }

// pending_count returns count of pending pages
int FreeList::pending_count() {
  int count = 0;
  for (auto &[_, txp] : pending) {
    count += txp.ids.size();
  }
  return count;
}

// copyall copies a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
void FreeList::copyall(pgid_vec &dst) {
  pgid_vec m;
  m.reserve(pending_count());
  for (auto &[_, txp] : pending) {
    m.insert(m.end(), txp.ids.begin(), txp.ids.end());
  }

  std::sort(m.begin(), m.end());
  mergepgids(dst, GetFreePageIDs(), m);
}

// arrayAllocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
pgid_t ArrayFreeList::Allocate(txid_t txid, int n) {
  if (ids.empty()) {
    return 0;
  }

  pgid_t initial = 0;
  pgid_t previd = 0;
  for (size_t i = 0; i < ids.size(); ++i) {
    auto id = ids[i];
    if (id <= 1) {
      panic(absl::StrFormat("invalid page allocation: %lu", id));
    }

    // Reset initial page if this is not contiguous.
    if (previd == 0 || id - previd != 1) {
      initial = id;
    }

    // If we found a contiguous block then remove it and return it.
    if ((id - initial) + 1 == pgid_t(n)) {
      // If we're allocating off the beginning then take the fast path
      // and just adjust the existing slice. This will use extra memory
      // temporarily but the append() in free() will realloc the slice
      // as is necessary.
      if ((i + 1) == n) {
        ids.erase(ids.begin(), ids.begin() + i + 2);
        // ids = ids[i+1:]
      } else {
        ids.erase(ids.begin() + i - n + 1, ids.begin() + i + 1);
        // copy(f.ids[i-n+1:], f.ids[i+1:])
        // f.ids = f.ids[:len(f.ids)-n]
      }

      for (pgid_t i = 0; i < n; ++i) {
        cache.erase(initial + i);
      }
      // Remove from the free cache.
      allocs[initial] = txid;
      return initial;
    }

    previd = id;
  }
  return 0;
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
void FreeList::free(txid_t txid, Page *p) {
  if (p->id <= 1) {
    panic(absl::StrFormat("cannot free page 0 or 1: %lu", p->id))
  }

  // Free page and all its overflow pages.
  auto &txp = pending[txid];
  auto it = allocs.find(p->id);
  txid_t allocTxid = 0;
  if (it != allocs.end()) {
    allocs.erase(it);
  } else if ((p->flags & freelistPageFlag) != 0) {
    // Freelist is always allocated by prior tx.
    allocTxid = txid - 1;
  }

  for (auto id = p->id; id <= p->id + pgid_t(p->overflow); id++) {
    // Verify that page is not already free.
    if (cache.find(id) != cache.end()) {
      panic(absl::StrFormat("page %lu already freed", id))
    }
    // Add to the freelist and cache.
    txp.ids.emplace_back(id);
    txp.alloctx.emplace_back(allocTxid);
    cache[id] = true;
  }
}

// release moves all page ids for a transaction id (or older) to the freelist.
void FreeList::release(txid_t txid) {
  pgid_vec m;
  for (auto &[tid, txp] : pending) {
    if (tid <= txid) {
      // Move transaction's pending pages to the available freelist.
      // Don't remove from the cache since the page is still free.
      m.insert(m.end(), txp.ids.begin(), txp.ids.end());
      pending.erase(tid);
    }
  }
  MergeSpans(m);
}

// releaseRange moves pending pages allocated within an extent [begin,end] to the free list.
void FreeList::releaseRange(txid_t begin, txid_t end) {
  if (begin > end) {
    return;
  }
  pgid_vec m;
  for (auto &[tid, txp] : pending) {
    if (tid < begin || tid > end) {
      continue;
    }
    // Don't recompute freed pages if ranges haven't updated.
    if (txp.lastReleaseBegin == begin) {
      continue;
    }
    for (auto i = 0; i < txp.ids.size(); i++) {
      if (auto atx = txp.alloctx[i]; atx < begin || atx > end) {
        continue;
      }
      m.emplace_back(txp.ids[i]);
      txp.ids[i] = txp.ids.back();
      txp.ids.pop_back();

      txp.alloctx[i] = txp.alloctx.back();
      txp.alloctx.pop_back();
      i--;
    }
    txp.lastReleaseBegin = begin;
    if (txp.ids.size() == 0) {
      pending.erase(tid);
    }
  }
  MergeSpans(m);
}

// rollback removes the pages from a given pending tx.
void FreeList::rollback(txid_t txid) {
  // Remove page ids from cache.
  auto it = pending.find(txid);
  if (it == pending.end()) {
    return;
  }
  auto &txp = it->second;

  pgid_vec m;
  for (size_t i = 0; i < ids.size(); ++i) {
    auto pgid = ids[i];
    cache.erase(pgid);
    const auto tx = txp.alloctx[i];
    if (tx == 0) {
      continue;
    }
    if (tx != txid) {
      // Pending free aborted; restore page back to alloc list.
      allocs[pgid] = tx;
    } else {
      // Freed page was allocated by this txn; OK to throw away.
      m.emplace_back(pgid);
    }
  }
  // Remove pages from pending list and mark as free if allocated by txid.
  pending.erase(txid);
  MergeSpans(m);
}

// freed returns whether a given page is in the free list.
bool FreeList::freed(pgid_t pgid) { return cache[pgid]; }

// read initializes the freelist from a freelist page.
void FreeList::read(Page *p) {
  if ((p->flags & freelistPageFlag) == 0) {
    panic(absl::StrFormat("invalid freelist page: %lu, page type is %s", p->id, p->type()));
  }
  // If the page.count is at the max uint16 value (64k) then it's considered
  // an overflow and the size of the freelist is stored as the first element.
  int idx = 0;
  int count = int(p->count);
  if (count == 0xFFFF) {
    idx = 1;
    const pgid_t c = (p + 1)->id;
    // c := *(*pgid)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
    count = int(c);
    if (count < 0) {
      panic(absl::StrFormat("leading element count %lu overflows int", c))
    }
  }

  // Copy the list of page ids from the freelist.
  if (count == 0) {
    ids.clear();
  } else {
    char *data = reinterpret_cast<char *>(p + 1) + sizeof(ids[0]) * idx;
    pgid_t *ids_ptr = reinterpret_cast<pgid_t *>(data);
    // data := unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p), unsafe.Sizeof(ids[0]), idx)
    // unsafeSlice(unsafe.Pointer(&ids), data, count)

    // copy the ids, so we don't modify on the freelist page directly
    pgid_vec idsCopy(ids_ptr, ids_ptr + count);

    // Make sure they're sorted.
    std::sort(idsCopy.begin(), idsCopy.end());

    ReadIDs(idsCopy);
  }
}

// arrayReadIDs initializes the freelist from a given list of ids.
void ArrayFreeList::ReadIDs(pgid_vec &ids_arg) {
  ids = ids_arg;
  reindex();
}

pgid_vec ArrayFreeList::GetFreePageIDs() { return ids; }

// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
ErrorCode FreeList::write(Page *p) {
  // Combine the old free pgid_vec and pgid_vec waiting on an open transaction.

  // Update the header flag.
  p->flags |= freelistPageFlag;

  // The page.count can only hold up to 64k elements so if we overflow that
  // number then we handle it by putting the size in the first element.
  auto l = count();
  if (l == 0) {
    p->count = uint16_t(l);
  } else if (l < 0xFFFF) {
    p->count = uint16_t(l);
    Page *data = unsafeAdd(p, sizeof(*p));
    pgid_t *ids_ptr = reinterpret_cast<pgid_t *>(data);
    pgid_vec ids(ids_ptr, ids_ptr + l);
    copyall(ids);
    // TODO() p->append( &ids[0], ids.size() * sizeof(ids[0]) );
  } else {
    p->count = 0xFFFF;
    char *data = reinterpret_cast<char *>(p + 1);
    pgid_t *ids_ptr = reinterpret_cast<pgid_t *>(data);
    pgid_vec ids(ids_ptr, ids_ptr + l + 1);
    ids[0] = pgid_t(l);
    pgid_vec tmp_ids(ids.begin() + 1, ids.end());
    copyall(tmp_ids);
    // TODO() p->append( &ids[0], ids.size() * sizeof(ids[0]) );
  }

  return ErrorCode::OK;
}

// reload reads the freelist from a page and filters out pending items.
void FreeList::reload(Page *p) {
  read(p);

  // Build a cache of only pending pages.
  std::map<pgid_t, bool> pcache;
  for (auto &[_, txp] : pending) {
    for (auto pending_id : txp.ids) {
      pcache[pending_id] = true;
    }
  }

  // Check each page in the freelist and build a new available freelist
  // with any pages not in the pending lists.
  pgid_vec a;
  for (auto id : GetFreePageIDs()) {
    if (!pcache[id]) {
      a.emplace_back(id);
    }
  }

  ReadIDs(a);
}

// noSyncReload reads the freelist from pgid_vec and filters out pending items.
void FreeList::noSyncReload(pgid_vec &pgids) {
  // Build a cache of only pending pages.
  std::map<pgid_t, bool> pcache;
  for (auto &[_, txp] : pending) {
    for (auto pendingID : txp.ids) {
      pcache[pendingID] = true;
    }
  }

  // Check each page in the freelist and build a new available freelist
  // with any pages not in the pending lists.
  pgid_vec a;
  for (auto id : pgids) {
    if (!pcache[id]) {
      a.emplace_back(id);
    }
  }

  ReadIDs(a);
}

// reindex rebuilds the free cache based on available and pending free lists.
void FreeList::reindex() {
  auto ids = GetFreePageIDs();
  cache.clear();
  for (auto id : ids) {
    cache[id] = true;
  }
  for (auto &[_, txp] : pending) {
    for (auto pending_id : txp.ids) {
      cache[pending_id] = true;
    }
  }
}

// arrayMergeSpans try to merge list of pages(represented by pgid_vec) with existing spans but using array
void ArrayFreeList::MergeSpans(pgid_vec &ids_arg) {
  std::sort(ids_arg.begin(), ids_arg.end());
  ids.insert(ids.end(), ids_arg.begin(), ids_arg.end());
}

}  // namespace bboltpp
