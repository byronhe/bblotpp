#include <gtest/gtest.h>
#include "test_common.h"
#include "../db.h"
#include "../bucket.h"
#include "../page.h"
#include "../logger.h"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

using namespace bboltpp;

namespace {

// Page size used by the synthetic database. Matches the on-disk layout that
// Go bbolt would produce with `db.PageSize = 4096`.
constexpr int kPageSize = 4096;
constexpr int kNumPages = 6;
constexpr int kFileBytes = kPageSize * kNumPages;

// Page ids used in the synthetic layout:
//   0,1 -> meta pages
//   2   -> freelist page (empty)
//   3   -> root branch page for the root bucket
//   4   -> left  leaf page  (keys "a","b","c")
//   5   -> right leaf page  (keys "d","e","f")
constexpr pgid_t kMetaPages[2] = {0, 1};
constexpr pgid_t kFreelistPage = 2;
constexpr pgid_t kRootBranchPage = 3;
constexpr pgid_t kLeftLeafPage = 4;
constexpr pgid_t kRightLeafPage = 5;
constexpr pgid_t kNextPgid = 6;
constexpr txid_t kTxid = 1;

// An 8-byte aligned backing buffer so that reinterpret_cast<Page*> / Meta* etc.
// is correctly aligned (every bboltpp on-disk struct needs at most 8-byte
// alignment). This mirrors how the production code views its mmap'd region.
struct AlignedFile {
  std::vector<uint64_t> raw;
  char* base;
  AlignedFile() : raw(kFileBytes / sizeof(uint64_t), 0), base(reinterpret_cast<char*>(raw.data())) {}
};

Page* pageAt(char* base, pgid_t id) { return reinterpret_cast<Page*>(base + id * kPageSize); }

// Write a valid meta page with FNV-1a checksum, mirroring DB::init().
void writeMetaPage(char* base, pgid_t pageId, txid_t txid) {
  auto* p = pageAt(base, pageId);
  p->id = pageId;
  p->flags = metaPageFlag;
  p->count = 0;
  p->overflow = 0;

  auto* m = p->meta();
  m->magic = kMetaMagic;
  m->version = kVersion;
  m->pageSize = uint32_t(kPageSize);
  m->flags = 0;
  m->root = BucketStored{.root = kRootBranchPage};
  m->freelist = kFreelistPage;
  m->pgid = kNextPgid;
  m->txid = txid;
  m->checksum = 0;            // must be cleared before hashing the first 56 bytes
  m->checksum = m->sum64();   // same FNV-1a as the production reader requires
}

void writeFreelistPage(char* base, pgid_t pageId) {
  auto* p = pageAt(base, pageId);
  p->id = pageId;
  p->flags = freelistPageFlag;
  p->count = 0;
  p->overflow = 0;
}

struct KeyPgid { std::string key; pgid_t pgid; };
struct KeyValue { std::string key; std::string value; };

// Write a branch page whose elements separate the children. The element keys
// are laid out after the element headers exactly like Node::write does.
void writeBranchPage(char* base, pgid_t pageId, const std::vector<KeyPgid>& items) {
  auto* p = pageAt(base, pageId);
  p->id = pageId;
  p->flags = branchPageFlag;
  p->count = uint16_t(items.size());
  p->overflow = 0;

  size_t off = sizeof(Page) + branchPageElementSize * items.size();
  for (size_t i = 0; i < items.size(); ++i) {
    auto* elem = p->branchPageElement(uint16_t(i));
    char* blob = reinterpret_cast<char*>(p) + off;
    elem->pos = uint32_t(blob - reinterpret_cast<char*>(elem));
    elem->ksize = uint32_t(items[i].key.size());
    elem->pgid = items[i].pgid;
    std::memcpy(blob, items[i].key.data(), items[i].key.size());
    off += items[i].key.size();
  }
}

// Write a leaf page whose key/value blobs follow the element headers, mirroring
// the production Node::write layout (pos is relative to the element itself).
void writeLeafPage(char* base, pgid_t pageId, const std::vector<KeyValue>& items) {
  auto* p = pageAt(base, pageId);
  p->id = pageId;
  p->flags = leafPageFlag;
  p->count = uint16_t(items.size());
  p->overflow = 0;

  size_t off = sizeof(Page) + leafPageElementSize * items.size();
  for (size_t i = 0; i < items.size(); ++i) {
    auto* elem = p->leafPageElement(uint16_t(i));
    char* blob = reinterpret_cast<char*>(p) + off;
    elem->pos = uint32_t(blob - reinterpret_cast<char*>(elem));
    elem->flags = 0;  // plain key/value, not a nested bucket
    elem->ksize = uint32_t(items[i].key.size());
    elem->vsize = uint32_t(items[i].value.size());
    std::memcpy(blob, items[i].key.data(), items[i].key.size());
    std::memcpy(blob + items[i].key.size(), items[i].value.data(), items[i].value.size());
    off += items[i].key.size() + items[i].value.size();
  }
}

// Build the multi-level bbolt database entirely by hand on disk so that the
// read-only branch-descent path (Cursor::searchPage) is exercised, which the
// C++ write path cannot reach today (Tx::Commit crashes in Node::spill before
// a multi-level tree is committed).
std::string buildSyntheticDb() {
  auto dir = std::filesystem::temp_directory_path() / "bbolt_synthetic_test";
  std::filesystem::create_directories(dir);
  std::string path = (dir / "synthetic_multilevel.db").string();

  AlignedFile f;
  char* base = f.base;

  // Two meta pages; meta1 has the higher txid so DB::GetMeta picks it.
  writeMetaPage(base, kMetaPages[0], 0);
  writeMetaPage(base, kMetaPages[1], kTxid);

  writeFreelistPage(base, kFreelistPage);

  // Root branch: ["a" -> leaf4, "d" -> leaf5]. "d" is a separator at index > 0
  // and is also a present leaf key in leaf5 -- the exact case the bug misses.
  writeBranchPage(base, kRootBranchPage, {
      {"a", kLeftLeafPage},
      {"d", kRightLeafPage},
  });

  writeLeafPage(base, kLeftLeafPage, {
      {"a", "va"},
      {"b", "vb"},
      {"c", "vc"},
  });
  writeLeafPage(base, kRightLeafPage, {
      {"d", "vd"},
      {"e", "ve"},
      {"f", "vf"},
  });

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.good()) {
    std::cerr << "cannot open " << path << " for writing" << std::endl;
    return "";
  }
  out.write(base, kFileBytes);
  if (!out.good()) {
    std::cerr << "failed to write synthetic db: " << path << std::endl;
    return "";
  }
  out.close();
  return path;
}

std::string valueOr(const std::optional<std::string_view>& v, const char* fallback = "nullopt") {
  return v.has_value() ? std::string(*v) : std::string(fallback);
}

}  // namespace

class SyntheticDbTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cleanupTestFiles();
    SetLogger(std::make_unique<DefaultLogger>(&std::cout));
  }
  void TearDown() override {
    cleanupTestFiles();
    auto dir = std::filesystem::temp_directory_path() / "bbolt_synthetic_test";
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
  }
};

// Regression: a present key that exactly equals a branch separator at index > 0
// must be found by Bucket::Get in a read-only transaction. Before the fix,
// Cursor::searchPage never set `exact` to true, so it decremented the branch
// slot and descended into the wrong leaf, returning std::nullopt for "d".
TEST_F(SyntheticDbTest, GetFindsSeparatorKeyInReadOnlyTx) {
  std::string path = buildSyntheticDb();
  ASSERT_FALSE(path.empty());

  Options opt;
  opt.ReadOnly = true;
  auto [db, openErr] = DB::Open(path, 0644, &opt);
  ASSERT_TRUE(openErr.OK()) << "Open(readOnly) failed: " << openErr.err_msg_;
  ASSERT_NE(db, nullptr);

  auto [tx, txErr] = db->Begin(false);
  ASSERT_TRUE(txErr.OK()) << "Begin(read-only) failed: " << txErr.err_msg_;
  ASSERT_NE(tx, nullptr);

  struct Case { std::string key; std::string want; };
  const std::vector<Case> cases = {
      {"a", "va"}, {"b", "vb"}, {"c", "vc"},
      {"d", "vd"},  // <-- separator key; the bug returned nullopt here
      {"e", "ve"}, {"f", "vf"},
  };
  for (const auto& c : cases) {
    auto v = tx->root->Get(c.key);
    EXPECT_TRUE(v.has_value())
        << "Get(\"" << c.key << "\") returned nullopt, expected \"" << c.want << "\" (got "
        << valueOr(v) << ")";
    EXPECT_EQ(c.want, valueOr(v))
        << "Get(\"" << c.key << "\") mismatch";
  }

  // Non-existent keys must still miss (no false positives): "cz" sorts between
  // the two leaf ranges and "z" is past the last key.
  EXPECT_FALSE(tx->root->Get("cz").has_value());
  EXPECT_FALSE(tx->root->Get("z").has_value());

  tx->Rollback();
  auto closeErr = db->Close();
  EXPECT_TRUE(closeErr.OK());
}

// Verify the on-disk meta checksum actually validates, so this synthetic DB is
// indistinguishable (layout-wise) from one produced by Go bbolt and the bug is
// genuinely on the read path rather than an artifact of an invalid file.
TEST_F(SyntheticDbTest, SyntheticMetaValidates) {
  std::string path = buildSyntheticDb();
  std::ifstream in(path, std::ios::binary);
  ASSERT_TRUE(in.good());
  std::vector<char> buf(kFileBytes, 0);
  in.read(buf.data(), kFileBytes);
  ASSERT_EQ(in.gcount(), kFileBytes);

  const Meta* m0 = pageAt(buf.data(), 0)->meta();
  const Meta* m1 = pageAt(buf.data(), 1)->meta();
  EXPECT_EQ(m0->magic, kMetaMagic);
  EXPECT_EQ(m1->version, kVersion);
  EXPECT_EQ(m1->root.root, kRootBranchPage);
  // validate()/sum64() are non-const; copy into a writable Meta to run them.
  Meta mc = *m0;
  EXPECT_EQ(mc.validate(), ErrorCode::OK);
  EXPECT_EQ(mc.checksum, mc.sum64());
  Meta mc1 = *m1;
  EXPECT_EQ(mc1.validate(), ErrorCode::OK);
  EXPECT_EQ(mc1.checksum, mc1.sum64());

  const Page* root = pageAt(buf.data(), kRootBranchPage);
  EXPECT_EQ(uint32_t(root->flags), uint32_t(branchPageFlag));
  EXPECT_EQ(int(root->count), 2);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
