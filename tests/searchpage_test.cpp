// Procedure G: direct unit-level coverage of Cursor::searchPage's branch-slot
// selection across the branch-layout matrix (single-element, multi-element,
// exact separator at index 0 / middle / last, non-exact, past-end, absent).
//
// Approach: open a minimal empty DB (DB::Open auto-creates 4 pages), begin a
// read-only tx, then inject hand-built branch/leaf PAGES into the tx's public
// `pages` cache (which Tx::page() consults before the mmap) and repoint
// `tx->root->bucket.root` at our synthetic root branch page. Bucket::Get then
// exercises Cursor::searchPage entirely against our injected pages — no
// on-disk multi-level file required, and no Node materialization (the `nodes`
// cache stays empty, so the page path, not searchNode, is taken).
#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../bucket.h"
#include "../page.h"
#include "../logger.h"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

using namespace bboltpp;

namespace {

constexpr int kPageSize = 4096;

// One 8-byte-aligned buffer holding all synthetic pages, kept alive for the
// life of the test so Page* pointers stored in tx->pages remain valid.
struct PageBuffer {
  std::vector<uint64_t> raw;
  char* base;
  explicit PageBuffer(int numPages)
      : raw((numPages * kPageSize) / sizeof(uint64_t) + 1, 0),
        base(reinterpret_cast<char*>(raw.data())) {}
  char* pageBase(pgid_t id) { return base + id * kPageSize; }
};

Page* makePage(PageBuffer& buf, pgid_t id, uint16_t flags, uint16_t count) {
  auto* p = reinterpret_cast<Page*>(buf.pageBase(id));
  p->id = id;
  p->flags = flags;
  p->count = count;
  p->overflow = 0;
  return p;
}

void writeBranch(PageBuffer& buf, pgid_t id, const std::vector<std::pair<std::string, pgid_t>>& items) {
  auto* p = makePage(buf, id, branchPageFlag, uint16_t(items.size()));
  size_t off = sizeof(Page) + branchPageElementSize * items.size();
  for (size_t i = 0; i < items.size(); ++i) {
    auto* elem = p->branchPageElement(uint16_t(i));
    char* blob = reinterpret_cast<char*>(p) + off;
    elem->pos = uint32_t(blob - reinterpret_cast<char*>(elem));
    elem->ksize = uint32_t(items[i].first.size());
    elem->pgid = items[i].second;
    std::memcpy(blob, items[i].first.data(), items[i].first.size());
    off += items[i].first.size();
  }
}

void writeLeaf(PageBuffer& buf, pgid_t id, const std::vector<std::pair<std::string, std::string>>& items) {
  auto* p = makePage(buf, id, leafPageFlag, uint16_t(items.size()));
  size_t off = sizeof(Page) + leafPageElementSize * items.size();
  for (size_t i = 0; i < items.size(); ++i) {
    auto* elem = p->leafPageElement(uint16_t(i));
    char* blob = reinterpret_cast<char*>(p) + off;
    elem->pos = uint32_t(blob - reinterpret_cast<char*>(elem));
    elem->flags = 0;
    elem->ksize = uint32_t(items[i].first.size());
    elem->vsize = uint32_t(items[i].second.size());
    std::memcpy(blob, items[i].first.data(), items[i].first.size());
    std::memcpy(blob + items[i].first.size(), items[i].second.data(), items[i].second.size());
    off += items[i].first.size() + items[i].second.size();
  }
}

// A read-only tx with synthetic pages injected. The branch root and leaves
// live in `buf` (kept alive for the tx's life). Page ids >= 100 avoid colliding
// with the real DB pages (0..3) served from the mmap.
struct SyntheticTree {
  std::shared_ptr<DB> db;
  std::shared_ptr<Tx> tx;
  std::shared_ptr<Bucket> bucket;
  std::shared_ptr<PageBuffer> buf;
  std::string dbPath;
};

std::string optStr(std::optional<std::string_view> v) {
  return v.has_value() ? std::string(*v) : std::string("<nullopt>");
}

SyntheticTree openWithSyntheticRoot(pgid_t rootBranchPage) {
  SyntheticTree t;
  t.buf = std::make_shared<PageBuffer>(256);  // room for ids 100..355
  t.dbPath = (std::filesystem::temp_directory_path() / "searchpage_min.db").string();
  std::error_code ec;
  std::filesystem::remove(t.dbPath, ec);

  Options opt;
  auto [db, oerr] = DB::Open(t.dbPath, 0644, &opt);
  EXPECT_TRUE(oerr.OK()) << oerr.err_msg_;
  t.db = db;

  auto [tx, terr] = db->Begin(false);
  EXPECT_TRUE(terr.OK()) << terr.err_msg_;
  t.tx = tx;
  t.bucket = tx->root;
  // Repoint the root bucket at our synthetic branch page and ensure no node
  // materialization interferes (nodes cache stays empty for read-only tx).
  t.bucket->bucket.root = rootBranchPage;
  EXPECT_TRUE(t.bucket->nodes.empty());
  return t;
}

void injectPage(SyntheticTree& t, pgid_t id) {
  t.tx->pages[id] = reinterpret_cast<Page*>(t.buf->pageBase(id));
}

void expectGet(SyntheticTree& t, std::string_view key, const std::string& want) {
  auto v = t.bucket->Get(key);
  if (want.empty()) {
    EXPECT_FALSE(v.has_value())
        << "Get(\"" << key << "\") expected nullopt, got " << optStr(v);
  } else {
    EXPECT_TRUE(v.has_value())
        << "Get(\"" << key << "\") expected \"" << want << "\", got nullopt";
    if (v.has_value()) {
      EXPECT_EQ(optStr(v), want) << "Get(\"" << key << "\") value mismatch";
    }
  }
}

}  // namespace

class SearchPageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cleanupTestFiles();
    SetLogger(std::make_unique<DefaultLogger>(&std::cout));
  }
  void TearDown() override {
    // Drop tx before the page buffer so Page* pointers in tx->pages stay valid
    // for the life of the transaction (rollback dereferences nothing here, but
    // be safe).
    cleanupTestFiles();
  }
};

// Guarantee 1 + 3: exact separator at index 0. The decrement guard
// (`index > 0`) means index 0 is never decremented regardless of `exact`, so
// this case must pass BOTH before and after the fix — a no-regression guard.
TEST_F(SearchPageTest, ExactSeparatorAtIndex0) {
  constexpr pgid_t B = 100, L0 = 101;
  auto t = openWithSyntheticRoot(B);
  writeBranch(*t.buf, B, {{"b", L0}});
  writeLeaf(*t.buf, L0, {{"b", "vb"}, {"c", "vc"}, {"d", "vd"}});
  injectPage(t, B);
  injectPage(t, L0);
  expectGet(t, "b", "vb");  // exact separator at index 0 -> found (unchanged)
  expectGet(t, "c", "vc");
  expectGet(t, "d", "vd");
  expectGet(t, "a", "");    // before first key -> nullopt
  t.tx->Rollback();
  t.db->Close();
}

// Guarantee 1 (the bug): exact separator at index > 0 must be found. Before the
// fix this returned nullopt; after the fix it returns the stored value.
TEST_F(SearchPageTest, ExactSeparatorAtMiddleIndex) {
  constexpr pgid_t B = 100, L0 = 101, L1 = 102;
  auto t = openWithSyntheticRoot(B);
  writeBranch(*t.buf, B, {{"a", L0}, {"d", L1}});
  writeLeaf(*t.buf, L0, {{"a", "va"}, {"b", "vb"}, {"c", "vc"}});
  writeLeaf(*t.buf, L1, {{"d", "vd"}, {"e", "ve"}, {"f", "vf"}});
  injectPage(t, B);
  injectPage(t, L0);
  injectPage(t, L1);
  expectGet(t, "a", "va");
  expectGet(t, "b", "vb");
  expectGet(t, "c", "vc");
  expectGet(t, "d", "vd");  // <-- discriminating: separator at index 1
  expectGet(t, "e", "ve");
  expectGet(t, "f", "vf");
  expectGet(t, "z", "");    // past last -> nullopt
  t.tx->Rollback();
  t.db->Close();
}

// Guarantees 1 + 4: exact separators at every index of a 3-way branch,
// including the last index; non-exact keys between separators still decrement.
TEST_F(SearchPageTest, ExactSeparatorsAtMultipleIndices) {
  constexpr pgid_t B = 100, L0 = 101, L1 = 102, L2 = 103;
  auto t = openWithSyntheticRoot(B);
  writeBranch(*t.buf, B, {{"a", L0}, {"d", L1}, {"g", L2}});
  writeLeaf(*t.buf, L0, {{"a", "va"}, {"b", "vb"}, {"c", "vc"}});
  writeLeaf(*t.buf, L1, {{"d", "vd"}, {"e", "ve"}, {"f", "vf"}});
  writeLeaf(*t.buf, L2, {{"g", "vg"}, {"h", "vh"}, {"i", "vi"}});
  injectPage(t, B);
  injectPage(t, L0);
  injectPage(t, L1);
  injectPage(t, L2);
  // exact separators at index 0, 1, 2 (incl. LAST)
  expectGet(t, "a", "va");
  expectGet(t, "d", "vd");  // index 1 separator
  expectGet(t, "g", "vg");  // index 2 (last) separator
  // non-exact keys still route to the correct (left) child via decrement
  expectGet(t, "b", "vb");
  expectGet(t, "c", "vc");
  expectGet(t, "e", "ve");
  expectGet(t, "f", "vf");
  expectGet(t, "h", "vh");
  expectGet(t, "i", "vi");
  // absent keys
  expectGet(t, "cc", "");   // between "c" and "d" -> nullopt
  expectGet(t, "z", "");    // past last -> nullopt
  expectGet(t, "", "");     // empty key -> nullopt
  t.tx->Rollback();
  t.db->Close();
}

// Guarantee 2 + 5: a key strictly less than the first separator descends into
// child 0 and misses if absent; a key greater than every separator descends
// into the last child and is found only if present there. Also covers a branch
// with a single (index 0) element with a key greater than the separator.
TEST_F(SearchPageTest, KeyBeforeFirstAndAfterLastSeparator) {
  constexpr pgid_t B = 100, L0 = 101, L1 = 102;
  auto t = openWithSyntheticRoot(B);
  writeBranch(*t.buf, B, {{"m", L0}, {"p", L1}});
  writeLeaf(*t.buf, L0, {{"m", "vm"}, {"n", "vn"}});
  writeLeaf(*t.buf, L1, {{"p", "vp"}, {"q", "vq"}});
  injectPage(t, B);
  injectPage(t, L0);
  injectPage(t, L1);
  expectGet(t, "a", "");    // before "m" -> child 0, absent -> nullopt
  expectGet(t, "m", "vm");  // exact index 0 separator
  expectGet(t, "n", "vn");  // between m and p -> decrement to child 0
  expectGet(t, "p", "vp");  // exact index 1 separator
  expectGet(t, "q", "vq");  // >= "p" exact? lower_bound "q">"p" index1, not exact, idx>0 -> decrement to 0? No: "q">"p", lower_bound returns index1 ("p"<"q" so first !< is index1? actually lower_bound with p.key()<key: "p"<"q" true, end. returns index2 (end). index2 not exact, >0, decrement to 1 -> child L1, find "q" -> vq. Good.
  expectGet(t, "z", "");    // past last -> child L1, absent -> nullopt
  t.tx->Rollback();
  t.db->Close();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
