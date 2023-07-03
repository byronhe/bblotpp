#include <algorithm>
#include <cstdint>

#include "absl/strings/str_format.h"
#include "freelist.h"
#include "page.h"
#include "tx.h"

namespace bboltpp {

// hashmapFreeCount returns count of free pages(hashmap version)
int HashMapFreeList::FreeCount() {
  // use the forwardMap to get the total count
  int count = 0;
  for (auto &[_, size] : forwardMap) {
    count += int(size);
  }
  return count;
}

// hashmapAllocate serves the same purpose as arrayAllocate, but use hashmap as
// backend
pgid_t HashMapFreeList::Allocate(txid_t txid, int n) {
  if (n == 0) {
    return 0;
  }

  // if we have a exact size match just return short path
  // lookup the map to find larger span
  auto it = freemaps.lower_bound(n);
  if (it != freemaps.end()) {
    auto size = it->first;
    for (auto &[pid, _] : it->second) {
      // remove the span
      delSpan(pid, uint64_t(n));
      allocs[pid] = txid;

      auto remain = size - uint64_t(n);
      if (size != n) {
        // add remain span
        addSpan(pid + pgid_t(n), remain);
      }

      // TODO : erase (range)
      // auto cache_it_beg = cache.find(pid);
      // auto cache_it_end = cache.find(pid + n);
      for (pgid_t i = 0; i < pgid_t(n); ++i) {
        cache.erase(pid + i);
      }
      return pid;
    }
  }
  return 0;
}

// hashmapReadIDs reads pgid_vec as input an initial the freelist(hashmap version)
void HashMapFreeList::ReadIDs(pgid_vec &pgids) {
  init(pgids);

  // Rebuild the page cache.
  reindex();
}

// hashmapGetFreePageIDs returns the sorted free page ids
pgid_vec HashMapFreeList::GetFreePageIDs() {
  auto count = FreeCount();
  if (count == 0) {
    return {};
  }

  pgid_vec m;
  m.reserve(count);
  for (auto &[start, size] : forwardMap) {
    for (int i = 0; i < int(size); ++i) {
      m.emplace_back(start + pgid_t(i));
    }
  }
  std::sort(m.begin(), m.end());
  return m;
}

// hashmapMergeSpans try to merge list of pages(represented by pgid_vec) with
// existing spans
void HashMapFreeList::MergeSpans(pgid_vec &ids) {
  for (auto id : ids) {
    // try to see if we can merge and update
    mergeWithExistingSpan(id);
  }
}

// mergeWithExistingSpan merges pid to the existing free spans, try to merge it
// backward and forward
void HashMapFreeList::mergeWithExistingSpan(pgid_t pid) {
  auto prev = pid - 1;
  auto next = pid + 1;

  auto prev_it = backwardMap.find(prev);
  auto next_it = forwardMap.find(next);
  const bool mergeWithPrev = (prev_it != backwardMap.end());
  const bool mergeWithNext = (next_it != forwardMap.end());
  auto newStart = pid;
  auto newSize = uint64_t(1);

  if (mergeWithPrev) {
    auto preSize = prev_it->second;
    // merge with previous span
    auto start = prev + 1 - pgid_t(preSize);
    delSpan(start, preSize);

    newStart -= pgid_t(preSize);
    newSize += preSize;
  }

  if (mergeWithNext) {
    auto nextSize = next_it->second;
    // merge with next span
    delSpan(next, nextSize);
    newSize += nextSize;
  }
  addSpan(newStart, newSize);
}

void HashMapFreeList::addSpan(pgid_t start, uint64_t size) {
  backwardMap[start - 1 + pgid_t(size)] = size;
  forwardMap[start] = size;

  freemaps[size][start] = nullptr;
}

void HashMapFreeList::delSpan(pgid_t start, uint64_t size) {
  forwardMap.erase(start);
  backwardMap.erase(start + pgid_t(size - 1));
  freemaps[size].erase(start);
  if (freemaps[size].empty()) {
    freemaps.erase(size);
  }
}

// initial from pgid_vec using when use hashmap version
// pgid_vec must be sorted
void HashMapFreeList::init(const pgid_vec &pgids) {
  if (pgids.empty()) {
    return;
  }

  uint64_t size = 1;
  pgid_t start = pgids[0];

  if (not std::is_sorted(pgids.begin(), pgids.end(), [](auto i, auto j) { return i < j; })) {
    absl::PrintF("pgids not sorted");
    abort();
  }

  for (size_t i = 1; i < pgids.size(); ++i) {
    // continuous page
    if (pgids[i] == pgids[i - 1] + 1) {
      size++;
    } else {
      addSpan(start, size);

      size = 1;
      start = pgids[i];
    }
  }

  // init the tail
  if (size != 0 && start != 0) {
    addSpan(start, size);
  }
}
}  // namespace bboltpp
