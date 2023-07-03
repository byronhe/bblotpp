#include "tx.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>
#include <typeinfo>
#include <vector>
#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "bucket.h"
#include "cursor.h"
#include "db.h"
#include "errors.h"
#include "page.h"
#include "unsafe.h"

using std::string_view;
namespace bboltpp {

// init initializes the transaction.
void Tx::init(class DB *db) {
  this->db = db;

  // Copy the meta page since it can be changed by the writer.
  this->meta = new struct Meta;  //&meta{}
  db->GetMeta()->copy(this->meta);

  // Copy over the root bucket.
  this->root = newBucket(this);
  // this->root->bucket = new bucket{};
  this->root->bucket = this->meta->root;

  // Increment the transaction id and add a page cache for writable transactions.
  if (this->writable) {
    // this->pages = make(map[pgid]*page);
    this->meta->txid += static_cast<txid_t>(1);
  }
}

// ID returns the transaction id.
int Tx::ID() { return static_cast<int>(this->meta->txid); }

// DB returns a reference to the database that created the transaction.
DB *Tx::GetDB() { return this->db; }

// Size returns current database size in bytes as seen by this transaction.
int64_t Tx::Size() { return static_cast<int64_t>(this->meta->pgid) * static_cast<int64_t>(this->db->pageSize); }

// Writable returns whether the transaction can perform write operations.
bool Tx::Writable() { return this->writable; }

// Cursor creates a cursor associated with the root bucket.
// All items in the cursor will return a nullptr value because all root bucket keys point to buckets.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
Cursor *Tx::GetCursor() { return this->root->NewCursor(); }

// Stats retrieves a copy of the current transaction statistics.
const TxStats &Tx::Stats() { return this->stats; }

// Bucket retrieves a bucket by name.
// Returns nullptr if the bucket does not exist.
// The bucket instance is only valid for the lifetime of the transaction.
Bucket *Tx::FindBucketByName(string_view name) { return this->root->FindBucketByName(name); }

// CreateBucket creates a new bucket.
// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
std::tuple<Bucket *, ErrorCode> Tx::CreateBucket(string_view name) { return this->root->CreateBucket(name); }

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
std::tuple<Bucket *, ErrorCode> Tx::CreateBucketIfNotExists(string_view name) {
  return this->root->CreateBucketIfNotExists(name);
}

// DeleteBucket deletes a bucket.
// Returns an error if the bucket cannot be found or if the key represents a non-bucket value.
ErrorCode Tx::DeleteBucket(string_view name) { return this->root->DeleteBucket(name); }

// ForEach executes a function for each bucket in the root.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller.
ErrorCode Tx::ForEach(std::function<ErrorCode(string_view, struct Bucket *)> fn) {
  return this->root->ForEach(
      [this, &fn](string_view k, string_view v) -> ErrorCode { return fn(k, this->root->FindBucketByName(k)); });
}

// OnCommit adds a handler function to be executed after the transaction successfully commits.
void Tx::OnCommit(std::function<void()> fn) { commitHandlers.emplace_back(fn); }

// Commit writes all changes to disk and updates the meta page.
// Returns an error if a disk write error occurs, or if Commit is
// called on a read-only transaction.
ErrorCode Tx::Commit() {
  _assert(!this->managed, "managed tx commit not allowed");
  if (this->db == nullptr) {
    return ErrorCode::ErrTxClosed;
  } else if (!this->writable) {
    return ErrorCode::ErrTxNotWritable;
  }

  // TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

  // Rebalance nodes which have had deletions.
  auto startTime = std::chrono::high_resolution_clock::now();

  this->root->rebalance();
  if (this->stats.Rebalance > 0) {
    this->stats.RebalanceTime +=
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startTime);
  }

  // spill data onto dirty pages.
  startTime = std::chrono::high_resolution_clock::now();
  if (auto err = this->root->spill(); err != ErrorCode::OK) {
    this->rollback();
    return err;
  }
  this->stats.SpillTime +=
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startTime);

  // Free the old root bucket.
  this->meta->root.root = this->root->bucket.root;

  // Free the old freelist because commit writes out a fresh freelist.
  if (this->meta->freelist != pgidNoFreelist) {
    this->db->freelist->free(this->meta->txid, this->db->GetPage(this->meta->freelist));
  }

  if (!this->db->NoFreelistSync) {
    auto err = this->commitFreelist();
    if (err != ErrorCode::OK) {
      return err;
    }
  } else {
    this->meta->freelist = pgidNoFreelist;
  }

  // Write dirty pages to disk.
  startTime = std::chrono::high_resolution_clock::now();
  if (auto err = this->write(); err != ErrorCode::OK) {
    this->rollback();
    return err;
  }

  // If strict mode is enabled then perform a consistency check.
  if (this->db->StrictMode) {
    std::vector<Error> ch = this->Check();
    std::vector<Error> errs;
    for (auto e : ch) {
      if (e.OK()) {
        continue;
      }
      errs.emplace_back(e);
    }
    if (errs.size() > 0) {
      panic(absl::StrFormat("check fail: %s ", absl::StrJoin(errs, " ").c_str()));
    }
  }

  // Write meta to disk.
  if (auto err = this->writeMeta(); err != ErrorCode::OK) {
    this->rollback();
    return err;
  }
  this->stats.WriteTime +=
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - startTime);

  // Finalize the transaction.
  this->close();

  // Execute commit handlers now that the locks have been removed.
  for (auto &fn : this->commitHandlers) {
    fn();
  }

  return ErrorCode::OK;
}

ErrorCode Tx::commitFreelist() {
  // Allocate new pages for the new free list. This will overestimate
  // the size of the freelist but not underestimate the size (which would be bad).
  const pgid_t opgid = this->meta->pgid;
  auto [p, err] = this->allocate((this->db->freelist->size() / this->db->pageSize) + 1);
  if (err != ErrorCode::OK) {
    this->rollback();
    return err;
  }
  if (auto err = this->db->freelist->write(p); err != ErrorCode::OK) {
    this->rollback();
    return err;
  }
  this->meta->freelist = p->id;
  // If the high water mark has moved up then attempt to grow the database.
  if (this->meta->pgid > opgid) {
    if (auto err = this->db->grow(int(this->meta->pgid + 1) * this->db->pageSize); not err.OK()) {
      this->rollback();
      return err.err_code_;
    }
  }

  return ErrorCode::OK;
}

// Rollback closes the transaction and ignores all previous updates. Read-only
// transactions must be rolled back and not committed.
ErrorCode Tx::Rollback() {
  _assert(!this->managed, "managed tx rollback not allowed");
  if (this->db == nullptr) {
    return ErrorCode::ErrTxClosed;
  }
  this->nonPhysicalRollback();
  return ErrorCode::OK;
}

// nonPhysicalRollback is called when user calls Rollback directly, in this case we do not need to reload the free pages
// from disk.
void Tx::nonPhysicalRollback() {
  if (this->db == nullptr) {
    return;
  }
  if (this->writable) {
    this->db->freelist->rollback(this->meta->txid);
  }
  this->close();
}

// rollback needs to reload the free pages from disk in case some system error happens like fsync error.
void Tx::rollback() {
  if (this->db == nullptr) {
    return;
  }
  if (this->writable) {
    this->db->freelist->rollback(this->meta->txid);
    if (!this->db->hasSyncedFreelist()) {
      // Reconstruct free page list by scanning the DB to get the whole free page list.
      // Note: scaning the whole db is heavy if your db size is large in NoSyncFreeList mode.
      auto free_pg_id_vec = this->db->freepages();
      this->db->freelist->noSyncReload(free_pg_id_vec);
    } else {
      // Read free page list from freelist page.
      this->db->freelist->reload(this->db->GetPage(this->db->GetMeta()->freelist));
    }
  }
  this->close();
}

void Tx::close() {
  if (this->db == nullptr) {
    return;
  }
  if (this->writable) {
    // Grab freelist stats.
    auto freelistFreeN = this->db->freelist->FreeCount();
    auto freelistPendingN = this->db->freelist->pending_count();
    auto freelistAlloc = this->db->freelist->size();

    // Remove transaction ref & writer lock.
    this->db->rwtx = nullptr;
    this->db->rwlock.unlock();

    // Merge statistics.
    this->db->statlock.lock();
    this->db->stats.FreePageN = freelistFreeN;
    this->db->stats.PendingPageN = freelistPendingN;
    this->db->stats.FreeAlloc = (freelistFreeN + freelistPendingN) * this->db->pageSize;
    this->db->stats.FreelistInuse = freelistAlloc;
    this->db->stats.tx_stats_.add(this->stats);
    this->db->statlock.unlock();
  } else {
    this->db->removeTx(this);
  }

  // Clear all references.
  this->db = nullptr;
  this->meta = nullptr;
  this->root = newBucket(this);
  this->pages.clear();
}

// Copy writes the entire database to a writer.
// This function exists for backwards compatibility.
//
// Deprecated; Use WriteTo() instead.
//   ErrorCode Tx::Copy(std::ostream & w) {
//   	auto [_, err] = this->WriteTo(w);
//   	return err;
//   }

std::tuple<int64_t, ErrorCode> IOCopyN(File *dst, File *src, ssize_t n, size_t src_offset) {
  // TODO(me): mmap + write;
  std::string buff;
  const ssize_t ret = src->Pread(n, src_offset, &buff);  // fread(&buff[0], n, 1, src);
  if (ret != n) {
    return {0, ErrorCode::ErrCopyNReadError};
  }
  const size_t w_ret = dst->Write(buff);  // fwrite(buff.data(), buff.size(), 1, dst);
  if (w_ret != n) {
    return {0, ErrorCode::ErrCopyNWriteError};
  }
  return {n, ErrorCode::OK};
}

// WriteTo writes the entire database to a writer.
// If err == nullptr then exactly tx.Size() bytes will be written into the writer.
std::tuple<int64_t, ErrorCode> Tx::WriteTo(File *w) {
  // Attempt to open reader with WriteFlag
  // std::tuple<std::unique_ptr<File>, Error> f_err;
  std::unique_ptr<File> file;
  Error err;
  std::tie(file, err) = this->db->openFile(this->db->path, O_RDONLY | this->WriteFlag, 0);
  if (!err.OK()) {
    return {0, err.err_code_};
  }
  absl::Cleanup close_file = [&file, &err] {
    if (auto close_err = file->Close(); close_err.err_code_ == ErrorCode::OK) {
      err = close_err;
    }
  };

  int64_t n = 0;
  // Generate a meta page. We use the same page data for both meta pages.
  std::string buf(this->db->pageSize, '\0');
  auto page = reinterpret_cast<struct Page *>(&buf[0]);  // (*page)(unsafe.Pointer(&buf[0]));
  page->flags = metaPageFlag;
  *page->meta() = *this->meta;

  // Write meta 0.
  page->id = 0;
  page->meta()->checksum = page->meta()->sum64();
  const size_t nn = w->Write(buf);  // fwrite(buf.data(), buf.size(), 1, w);
  n += int64_t(nn);
  if (nn != buf.size()) {
    return {n, ErrorCode::ErrMeta0CopyErr};  // Errorf("meta 0 copy: %s", err)};
  }

  // Write meta 1 with a lower transaction id.
  page->id = 1;
  page->meta()->txid -= 1;
  page->meta()->checksum = page->meta()->sum64();
  const size_t nn2 = w->Write(buf);  // fwrite(buf.data(), buf.size(), 1, w);
  n += int64_t(nn2);
  if (nn2 != buf.size()) {
    return {n, ErrorCode::ErrMeta1CopyErr};  // fmt.Errorf("meta 1 copy: %s", err);
  }

  // Move past the meta pages in the file.
  // if (auto err = file->Seek(int64_t(this->db->pageSize * 2), SEEK_SET); err != 0) {
  //  return {n, static_cast<ErrorCode>(err)};  // fmt.Errorf("seek: %s", err)};
  // }

  // Copy data pages.
  auto [wn, copyn_err] =
      IOCopyN(w, file.get(), this->Size() - int64_t(this->db->pageSize * 2), (this->db->pageSize * 2));
  n += wn;
  if (copyn_err != ErrorCode::OK) {
    return {n, copyn_err};
  }

  return {n, ErrorCode::OK};
}

// CopyFile copies the entire database to file at the given path.
// A reader transaction is maintained during the copy so it is safe to continue
// using the database while a copy is in progress.
ErrorCode Tx::CopyFile(std::string_view path, uint32_t mode) {
  auto [f, err] = this->db->openFile(path, O_RDWR | O_CREAT | O_TRUNC, mode);
  if (!err.OK()) {
    return err.err_code_;
  }

  auto [_, err2] = this->WriteTo(f.get());
  if (err2 != ErrorCode::OK) {
    f->Close();
    return err2;
  }
  return f->Close().err_code_;
}

// Check performs several consistency checks on the database for this transaction.
// An error is returned if any inconsistency is found.
//
// It can be safely run concurrently on a writable transaction. However, this
// incurs a high cost for large databases and databases with a lot of subbuckets
// because of caching. This overhead can be removed if running on a read-only
// transaction, however, it is not safe to execute other writer transactions at
// the same time.
std::vector<Error> Tx::Check() {
  // <-chan error
  std::vector<Error> ch;
  // thread t([&ch](){
  //    this->check(ch);
  // })
  // t.join();
  //
  // 	ch = make(chan error);
  // 	go this->check(ch);
  // 	return ch;
  //
  //
  // Tx::check(ch chan error)
  // Force loading free list if opened in ReadOnly mode.
  this->db->loadFreelist();

  // Check if any pages are double freed.
  std::map<pgid_t, bool> freed;  //= make(map[pgid]bool);
  pgid_vec all(this->db->freelist->count(), 0);
  this->db->freelist->copyall(all);
  for (auto id : all) {
    if (freed[id]) {
      // ch <- fmt::format("page %d: already freed", id);
      ch.emplace_back(absl::StrFormat("page %lu: already freed", id));
    }
    freed[id] = true;
  }

  // Track every reachable page.
  std::map<pgid_t, struct Page *> reachable;
  reachable[0] = this->page(0);  // meta0
  reachable[1] = this->page(1);  // meta1
  if (this->meta->freelist != pgidNoFreelist) {
    for (uint32_t i = 0; i <= this->page(this->meta->freelist)->overflow; i++) {
      reachable[this->meta->freelist + pgid_t(i)] = this->page(this->meta->freelist);
    }
  }

  // Recursively check buckets.
  this->checkBucket(this->root, reachable, freed, ch);

  // Ensure all pages below high water mark are either reachable or freed.
  for (pgid_t i = 0; i < this->meta->pgid; i++) {
    bool isReachable = reachable[i];
    if (!isReachable && !freed[i]) {
      ch.emplace_back(absl::StrFormat("page %d: unreachable unfreed", int(i)));
    }
  }

  // Close the channel to signal completion.
  // close(ch);

  return ch;
}

void Tx::checkBucket(struct Bucket *b, std::map<pgid_t, struct Page *> &reachable, std::map<pgid_t, bool> &freed,
                     std::vector<Error> &ch) {
  // Ignore inline buckets.
  if (b->bucket.root == 0) {
    return;
  }

  // Check every page used by this bucket.
  // b.root, 0,
  b->forEachPage([this, &ch, &reachable, &freed](struct Page *p, int _) {
    if (p->id > this->meta->pgid) {
      ch.emplace_back(absl::StrFormat("page %d: out of bounds: %d", int(p->id), int(this->meta->pgid)));
    }

    // Ensure each page is only referenced once.
    for (pgid_t i = 0; i <= pgid_t(p->overflow); i++) {
      auto id = p->id + i;
      if (reachable.count(id)) {
        ch.emplace_back(absl::StrFormat("page %d: multiple references", int(id)));
      }
      reachable[id] = p;
    }

    // We should only encounter un-freed leaf and branch pages.
    if (freed[p->id]) {
      ch.emplace_back(absl::StrFormat("page %d: reachable freed", int(p->id)));
    } else if ((p->flags & branchPageFlag) == 0 && (p->flags & leafPageFlag) == 0) {
      ch.emplace_back(absl::StrFormat("page %d: invalid type: %s", int(p->id), p->type()));
    }
  });

  // Check each bucket within this bucket.
  b->ForEach([&](std::string_view k, std::string_view v) -> ErrorCode {
    if (auto child = b->FindBucketByName(k); child != nullptr) {
      this->checkBucket(child, reachable, freed, ch);
    }
    return ErrorCode::OK;
  });
}

// allocate returns a contiguous block of memory starting at a given page.
std::tuple<Page *, ErrorCode> Tx::allocate(int count) {
  auto [p, err] = this->db->allocate(this->meta->txid, count);
  if (!err.OK()) {
    return {nullptr, err.err_code_};
  }

  // Save to our page cache.
  this->pages[p->id] = p;

  // Update statistics.
  this->stats.PageCount += count;
  this->stats.PageAlloc += count * this->db->pageSize;

  return {p, ErrorCode::OK};
}

// write writes any dirty pages to disk.
ErrorCode Tx::write() {
  // Sort pages by id.
  std::vector<struct Page *> pages_vec;
  pages_vec.reserve(this->pages.size());
  for (auto &[id, p] : this->pages) {
    pages_vec.emplace_back(p);
  }
  // Clear out page cache early.
  this->pages.clear();
  std::sort(pages_vec.begin(), pages_vec.end());

  // Write pages to disk in order.
  for (auto p : pages_vec) {
    auto rem = (uint64_t(p->overflow) + 1) * uint64_t(this->db->pageSize);
    auto offset = int64_t(p->id) * int64_t(this->db->pageSize);
    uintptr_t written = 0;

    // Write out page in "max allocation" sized chunks.
    for (;;) {
      auto sz = rem;
      if (sz > maxAllocSize - 1) {
        sz = maxAllocSize - 1;
      }
      const auto buf = unsafeByteSlice(p, written, 0, int(sz));

      if (auto [ret, err] = this->db->ops.writeAt(buf, offset); not err.OK()) {
        return err.err_code_;
      }

      // Update statistics.
      this->stats.Write++;

      // Exit inner for loop if we've written all the chunks.
      rem -= sz;
      if (rem == 0) {
        break;
      }

      // Otherwise move offset forward and move pointer to next chunk.
      offset += int64_t(sz);
      written += uintptr_t(sz);
    }
  }

  // Ignore file sync if flag is set on DB.
  if (!this->db->NoSync || IgnoreNoSync()) {
    if (auto err = this->db->Sync(); not err.OK()) {
      return err.err_code_;
    }
  }

  // Put small pages back to page pool.
  for (auto p : pages_vec) {
    // Ignore page sizes over 1 page.
    // These are allocated using make() instead of the page pool.
    if (int(p->overflow) != 0) {
      continue;
    }

    auto buf = unsafeByteSlice(p, 0, 0, this->db->pageSize);

    // See https://go.googlesource.com/go/+/f03c9202c43e0abb130669852082117ca50aa9b1
    ::memset(const_cast<char *>(buf.data()), 0, buf.size());
    // this->db->pagePool.Put(buf);
  }

  return ErrorCode::OK;
}

// writeMeta writes the meta to the disk.
ErrorCode Tx::writeMeta() {
  // Create a temporary buffer for the meta page.
  std::string buf(this->db->pageSize, '\0');
  auto p = this->db->pageInBuffer(buf, 0);
  this->meta->write(p);

  // Write the meta page to file.
  if (auto [ret, err] = this->db->ops.writeAt(buf, int64_t(p->id) * int64_t(this->db->pageSize)); not err.OK()) {
    return err.err_code_;
  }
  if (!this->db->NoSync || IgnoreNoSync()) {
    if (auto err = this->db->Sync(); not err.OK()) {
      return err.err_code_;
    }
  }

  // Update statistics.
  this->stats.Write++;

  return ErrorCode::OK;
}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary buffered page is returned.
struct Page *Tx::page(pgid_t id) {
  // Check the dirty pages first.
  if (!this->pages.empty()) {
    if (auto it = this->pages.find(id); it != this->pages.end()) {
      return it->second;
    }
  }

  // Otherwise return directly from the mmap.
  return this->db->GetPage(id);
}

// forEachPage iterates over every page within a given page and executes a function.
void Tx::forEachPage(pgid_t pgid, int depth, std::function<void(struct Page *, int)> fn) {
  auto p = this->page(pgid);

  // Execute function.
  fn(p, depth);

  // Recursively loop over children.
  if ((p->flags & branchPageFlag) != 0) {
    for (int i = 0; i < int(p->count); i++) {
      auto elem = p->branchPageElement(uint16_t(i));
      this->forEachPage(elem->pgid, depth + 1, fn);
    }
  }
}

// Page returns page information for a given page number.
// This is only safe for concurrent use when used by a writable transaction.
std::tuple<std::unique_ptr<struct PageInfo>, ErrorCode> Tx::GetPageInfo(int id) {
  if (this->db == nullptr) {
    return {nullptr, ErrorCode::ErrTxClosed};
  } else if (pgid_t(id) >= this->meta->pgid) {
    return {nullptr, ErrorCode::OK};
  }

  // Build the page info.
  auto p = this->db->GetPage(pgid_t(id));
  std::unique_ptr<PageInfo> info = std::make_unique<PageInfo>();
  info->ID = id;
  info->Count = int(p->count);
  info->OverflowCount = int(p->overflow);

  // Determine the type (or if it's free).
  if (this->db->freelist->freed(pgid_t(id))) {
    info->Type = "free";
  } else {
    info->Type = p->type();
  }

  return {std::move(info), ErrorCode::OK};
}

TxStats &TxStats::add(const TxStats &other) {
  this->PageCount += other.PageCount;
  this->PageAlloc += other.PageAlloc;
  this->CursorCount += other.CursorCount;
  this->NodeCount += other.NodeCount;
  this->NodeDeref += other.NodeDeref;
  this->Rebalance += other.Rebalance;
  this->RebalanceTime += other.RebalanceTime;
  this->Split += other.Split;
  this->Spill += other.Spill;
  this->SpillTime += other.SpillTime;
  this->Write += other.Write;
  this->WriteTime += other.WriteTime;
  return *this;
}

// Sub calculates and returns the difference between two sets of transaction stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
TxStats TxStats::Sub(const TxStats &other) const {
  TxStats diff;
  diff.PageCount = this->PageCount - other.PageCount;
  diff.PageAlloc = this->PageAlloc - other.PageAlloc;
  diff.CursorCount = this->CursorCount - other.CursorCount;
  diff.NodeCount = this->NodeCount - other.NodeCount;
  diff.NodeDeref = this->NodeDeref - other.NodeDeref;
  diff.Rebalance = this->Rebalance - other.Rebalance;
  diff.RebalanceTime = this->RebalanceTime - other.RebalanceTime;
  diff.Split = this->Split - other.Split;
  diff.Spill = this->Spill - other.Spill;
  diff.SpillTime = this->SpillTime - other.SpillTime;
  diff.Write = this->Write - other.Write;
  diff.WriteTime = this->WriteTime - other.WriteTime;
  return diff;
}
}  // namespace bboltpp
