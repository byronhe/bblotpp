#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace bboltpp {

constexpr std::size_t minKeysPerPage = 2;

constexpr uint32_t branchPageFlag = 0x01;
constexpr uint32_t leafPageFlag = 0x02;
constexpr uint32_t metaPageFlag = 0x04;
constexpr uint32_t freelistPageFlag = 0x10;

constexpr uint32_t bucketLeafFlag = 0x01;

// Page header size - defined after Page struct
extern const std::size_t pageHeaderSize;

// Page element sizes - defined after element structs
extern const std::size_t branchPageElementSize;
extern const std::size_t leafPageElementSize;

// maxMapSize represents the largest mmap size supported by Bolt.
constexpr uint64_t maxMapSize = 0xFFFFFFFFFFFF;  // 256TB

// maxAllocSize is the size used when creating array pointers.
constexpr uint64_t maxAllocSize = 0x7FFFFFFF;

using pgid_t = uint64_t;
struct Meta;
struct BranchPageElement;
struct LeafPageElement;

struct Page {
  pgid_t id = 0;
  uint16_t flags = 0;
  uint16_t count = 0;
  uint32_t overflow = 0;

  const char *type() const;
  std::string typeWithFlags() const;
  struct Meta *meta();
  struct LeafPageElement *leafPageElement(uint16_t index);
  std::tuple<struct LeafPageElement *, uint16_t> leafPageElements();
  struct BranchPageElement *branchPageElement(uint16_t index);
  std::tuple<struct BranchPageElement *, uint16_t> branchPageElements();
};

// meta returns a pointer to the metadata section of the page.
inline struct Meta *Page::meta() {
  return reinterpret_cast<struct Meta *>(reinterpret_cast<char *>(this) + sizeof(*this));
}

// leafPageElement represents a node on a leaf page.
struct LeafPageElement {
  uint32_t flags = 0;
  uint32_t pos = 0;
  uint32_t ksize = 0;
  uint32_t vsize = 0;

  // key returns a byte slice of the node key.
  std::string_view key() const { return {reinterpret_cast<const char *>(this) + pos, ksize}; }

  // value returns a byte slice of the node value.
  std::string_view value() { return {reinterpret_cast<const char *>(this) + pos + ksize, vsize}; }
};

// leafPageElement retrieves the leaf node by index
inline struct LeafPageElement *Page::leafPageElement(uint16_t index) {
  return reinterpret_cast<struct LeafPageElement *>(reinterpret_cast<char *>(this) + sizeof(*this) + leafPageElementSize * index);
}

// leafPageElements retrieves a list of leaf nodes.
inline std::tuple<struct LeafPageElement *, uint16_t> Page::leafPageElements() {
  if (count == 0) {
    return {nullptr, 0};
  }
  auto *elems = reinterpret_cast<struct LeafPageElement *>(reinterpret_cast<char *>(this) + sizeof(*this));
  return {elems, this->count};
}

// branchPageElement represents a node on a branch page.
struct BranchPageElement {
  uint32_t pos = 0;
  uint32_t ksize = 0;
  pgid_t pgid = 0;

  // key returns a byte slice of the node key.
  std::string_view key() const { return std::string_view((const char *)this + pos, ksize); }
};

// branchPageElement retrieves the branch node by index
inline BranchPageElement *Page::branchPageElement(uint16_t index) {
  return reinterpret_cast<struct BranchPageElement *>(reinterpret_cast<char *>(this) + sizeof(*this) + branchPageElementSize * index);
}

// branchPageElements retrieves a list of branch nodes.
inline std::tuple<struct BranchPageElement *, uint16_t> Page::branchPageElements() {
  if (count == 0) {
    return {nullptr, 0};
  }
  auto *elems = reinterpret_cast<struct BranchPageElement *>(reinterpret_cast<char *>(this) + sizeof(*this));
  return {elems, count};
}

// dump writes n bytes of the page to STDERR as hex output.
std::string hexdump(Page *p, size_t n);

// PageInfo represents human readable information about a page.
struct PageInfo {
  int ID = 0;
  std::string Type;
  int Count = 0;
  int OverflowCount = 0;
};

using pgid_vec = std::vector<pgid_t>;

// merge returns the sorted union of a and b.
inline pgid_vec merge(const pgid_vec &a, const pgid_vec &b) {
  // Return the opposite slice if one is nil.

  pgid_vec merged;
  std::set_union(a.begin(), a.end(), b.begin(), b.end(), std::back_inserter(merged));
  return merged;
}

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
// void mergepgids(pgid_vec &a, pgid_vec &b, pgid_vec &dst) {
// }
void mergepgids(pgid_vec &dst, const pgid_vec &a, const pgid_vec &b);

#define panic(code) \
  {                 \
    (void)code;     \
    abort();        \
  }

#define _assert(condition, message) assert((condition) && message)

}  // namespace bboltpp
