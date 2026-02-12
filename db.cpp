#include "db.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <unistd.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>
#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "errors.h"
#include "freelist.h"
#include "page.h"
#include "tx.h"

namespace bboltpp {

bool IgnoreNoSync() {
  static bool s_IgnoreNoSync = false;
  static std::once_flag init_once;
  std::call_once(init_once, [&]() {
    struct utsname uname = {};
    int ret = ::uname(&uname);
    assert(0 == ret);
    s_IgnoreNoSync = (0 == strcasecmp(uname.sysname, "openbsd"));
  });
  return s_IgnoreNoSync;
}

inline size_t defaultPageSize() { return getpagesize(); }

// Path returns the path to currently open database file.
std::string DB::Path() { return path; }

// GoString returns the Go string representation of the database.
std::string DB::GoString() { return absl::StrFormat("bolt.DB{path:%s}", path.c_str()); }

// String returns the string representation of the database.
std::string DB::String() { return absl::StrFormat("DB<%s>", path.c_str()); }

const Options &DefaultOptions();

// Open creates and opens a database at the given path.
// If the file does not exist then it will be created automatically.
// Passing in nullptr options will cause Bolt to open the database with the default options.

std::tuple<std::shared_ptr<DB>, Error> DB::Open(std::string_view path, uint32_t mode, const Options *options_ptr) {
  std::shared_ptr<DB> db_ptr = std::make_shared<DB>();
  DB &db = *db_ptr;
  db.opened = true;

  // Set default options if no options are provided.
  if (options_ptr == nullptr) {
    options_ptr = &DefaultOptions();
  }
  const auto &options = *options_ptr;
  db.NoSync = options.NoSync;
  db.NoGrowSync = options.NoGrowSync;
  db.MmapFlags = options.MmapFlags;
  db.NoFreelistSync = options.NoFreelistSync;
  db.freelist_type = options.free_list_type_;
  db.Mlock = options.Mlock;

  // Set default values for later DB operations.
  db.MaxBatchSize = DefaultMaxBatchSize;
  db.MaxBatchDelay = DefaultMaxBatchDelay;
  db.AllocSize = DefaultAllocSize;

  int flag = O_RDWR;
  if (options.ReadOnly) {
    flag = O_RDONLY;
    db.readOnly = true;
  }

  db.openFile = options.openFile;
  if (db.openFile == nullptr) {
    db.openFile = File::OSOpenFile;
  }

  // Open data file and separate sync handler for metadata writes.
  Error err;
  std::tie(db.file, err) = db.openFile(path, flag | O_CREAT, mode);
  if (!err.OK()) {
    auto _ = db.close();
    return {nullptr, err};
  }
  // db.path = db.file.Name();
  db.path = path;

  // Lock file so that other processes using Bolt in read-write mode cannot
  // use the database  at the same time. This would cause corruption since
  // the two processes would write meta pages and free pages separately.
  // The database file is locked exclusively (only one process can grab the lock)
  // if !options.ReadOnly.
  // The database file is locked using the shared lock (more than one process may
  // hold a lock at the same time) otherwise (options.ReadOnly is set).
  if (auto err = db.file->FLock(!db.readOnly, options.Timeout); !err.OK()) {
    auto _ = db.close();
    return {nullptr, err};
  }

  // Default values for test hooks
  db.ops.writeAt = [db_ptr](std::string_view buff, int64_t offset) { return db_ptr->file->WriteAt(buff, offset); };

  if (db.pageSize = options.PageSize; db.pageSize == 0) {
    // Set the default page size to the OS page size.
    db.pageSize = defaultPageSize();
  }

  // Initialize the database if it doesn't exist.
  if (auto [info, err] = db.file->Stat(); not err.OK()) {
    auto _ = db.close();
    return {nullptr, err};
  } else if (info.Size() == 0) {
    // Initialize new files with meta pages.
    if (auto err = db.init(); not err.OK()) {
      // clean up file descriptor on initialization fail
      auto _ = db.close();
      return {nullptr, err};
    }
  } else {
    // Read the first meta page to determine the page size.
    std::string buf(0x1000, '\0');
    // If we can't read the page size, but can read a page, assume
    // it's the same as the OS or one given -- since that's how the
    // page size was chosen in the first place.
    //
    // If the first page is invalid and this OS uses a different
    // page size than what the database was created with then we
    // are out of luck and cannot access the database.
    //
    // TODO: scan for next page
    if (auto [bw, err] = db.file->ReadAt(0, &buf); err.OK() && (bw == static_cast<int>(buf.size()))) {
      if (auto m = db.pageInBuffer(buf, 0)->meta(); m->validate() == ErrorCode::OK) {
        db.pageSize = int(m->pageSize);
      }
    } else {
      auto _ = db.close();
      return {nullptr, Error(ErrorCode::ErrInvalid)};
    }
  }

  // Initialize page pool.
  // db.pagePool = sync.Pool{
  // 	New: func() interface{} {
  // 		return make([]byte, db.pageSize);
  // 	},
  // };

  // Memory map the data file.
  if (auto err = db.mmap(options.InitialMmapSize); not err.OK()) {
    auto _ = db.close();
    return {nullptr, err};
  }

  if (db.readOnly) {
    return {std::move(db_ptr), Error()};
  }

  db.loadFreelist();

  // Flush freelist when transitioning from no sync to sync so
  // NoFreelistSync unaware boltdb can open the db later.
  if (!db.NoFreelistSync && !db.hasSyncedFreelist()) {
    auto [tx, err] = db.Begin(true);
    if (tx != nullptr) {
      err = Error{tx->Commit()};
    }
    if (not err.OK()) {
      auto _ = db.close();
      return {nullptr, err};
    }
  }

  // Mark the database as opened and return.
  return {std::move(db_ptr), Error()};
}

// loadFreelist reads the freelist if it is synced, or reconstructs it
// by scanning the DB if it is not synced. It assumes there are no
// concurrent accesses being made to the freelist.
void DB::loadFreelist() {
  std::call_once(freelistLoad, [this]() {
    this->freelist = newFreelist(this->freelist_type);
    if (!this->hasSyncedFreelist()) {
      // Reconstruct free list by scanning the DB.
      auto pgids = this->freepages();
      this->freelist->ReadIDs(pgids);
    } else {
      // Read free list from freelist page.
      this->freelist->read(this->GetPage(this->GetMeta()->freelist));
    }
    this->stats.FreePageN = this->freelist->FreeCount();
  });
}

bool DB::hasSyncedFreelist() { return GetMeta()->freelist != pgidNoFreelist; }

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
Error DB::mmap(int minsz) {
  std::lock_guard g(mmaplock);

  auto [info, err] = file->Stat();
  if (not err.OK()) {
    return Error(absl::StrFormat("mmap stat error: %v", err));
  } else if (int(info.Size()) < this->pageSize * 2) {
    return Error{absl::StrFormat("file size too small")};
  }

  // Ensure the size is at least the minimum size.
  const auto fileSize = info.Size();
  auto size = fileSize;
  if (size < minsz) {
    size = minsz;
  }
  std::tie(size, err) = mmapSize(size);
  if (not err.OK()) {
    return err;
  }

  if (this->Mlock) {
    // Unlock db memory
    if (auto err = this->munlock(fileSize); not err.OK()) {
      return err;
    }
  }

  // Dereference all mmap references before unmapping.
  if (auto t = this->rwtx.lock(); t != nullptr) {
    t->root->dereference();
  }

  // Unmap existing data before continuing.
  if (auto err = this->munmap(); not err.OK()) {
    return err;
  }

  // Memory-map the data file as a byte slice.
  const auto ret = ::mmap(nullptr, size, PROT_READ, MAP_SHARED | MmapFlags, file->Fd(), 0);
  if (ret == MAP_FAILED) {
    return Error{errno};
  }

  // Advise the kernel that the mmap is accessed randomly.
  int const madvise_err = ::madvise(ret, size, MADV_RANDOM);
  if (madvise_err != 0 && errno != ENOSYS) {
    // Ignore not implemented error in kernel because it still works.
    return Error{absl::StrFormat("madvise: %d errno %d ", madvise_err, errno)};
  }

  // Save the original byte slice and convert to a byte array pointer.
  dataref = std::string_view(static_cast<const char *>(ret), size);
  data = std::string_view(static_cast<const char *>(ret), size);
  datasz = size;

  if (this->Mlock) {
    // Don't allow swapping of data file
    if (auto err = this->mlock(fileSize); not err.OK()) {
      return err;
    }
  }

  // Save references to the meta pages.
  this->meta0 = this->GetPage(0)->meta();
  this->meta1 = this->GetPage(1)->meta();

  // Validate the meta pages. We only return an error if both meta pages fail
  // validation, since meta0 failing validation means that it wasn't saved
  // properly -- but we can recover using meta1. And vice-versa.
  auto err0 = this->meta0->validate();
  auto err1 = this->meta1->validate();

  if (err0 != ErrorCode::OK && err1 != ErrorCode::OK) {
    return Error{err0};
  }

  return Error{};
}

// munmap unmaps the data file from memory.
Error DB::munmap() {
  // Ignore the unmap if we have no mapped data.
  if (dataref.empty()) {
    return Error{};
  }

  // Unmap using the original byte slice.
  auto ret = ::munmap(const_cast<void *>(static_cast<const void *>(dataref.data())), dataref.size());
  dataref = std::string_view{};
  data = std::string_view{};
  datasz = 0;
  return Error{ret};
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
// Returns an error if the new mmap size is greater than the max allowed.
std::tuple<int, Error> DB::mmapSize(int size) {
  // Double the size from 32KB until 1GB.
  for (int i = 15; i <= 30; i++) {
    if (size <= 1 << i) {
      return {1 << i, Error()};
    }
  }

  // Verify the requested size is not above the maximum allowed.
  if (size > maxMapSize) {
    return {0, Error{absl::StrFormat("mmap too large")}};
  }

  // If larger than 1GB then grow by 1GB at a time.
  auto sz = int64_t(size);
  if (auto remainder = sz % int64_t(maxMmapStep); remainder > 0) {
    sz += int64_t(maxMmapStep) - remainder;
  }

  // Ensure that the mmap size is a multiple of the page size.
  // This should always be true since we're incrementing in MBs.
  auto pageSize = int64_t(this->pageSize);
  if ((sz % pageSize) != 0) {
    sz = ((sz / pageSize) + 1) * pageSize;
  }

  // If we've exceeded the max size then only grow up to the max size.
  if (sz > maxMapSize) {
    sz = maxMapSize;
  }

  return {int(sz), Error{}};
}

Error DB::munlock(int fileSize) {
  if (data.empty()) {
    return Error{};
  }

  size_t size_to_unlock = fileSize;
  if (size_to_unlock > data.size()) {
    size_to_unlock = data.size();
  }
  auto ret = ::munlock(data.data(), size_to_unlock);
  return Error{ret};
}

Error DB::mlock(int fileSize) {
  if (data.empty()) {
    return Error{};
  }

  size_t size_to_lock = fileSize;
  if (size_to_lock > data.size()) {
    size_to_lock = data.size();
  }
  auto ret = ::mlock(data.data(), size_to_lock);
  return Error{ret};
}

Error DB::mrelock(int fileSizeFrom, int fileSizeTo) {
  if (auto err = this->munlock(fileSizeFrom); not err.OK()) {
    return err;
  }
  if (auto err = this->mlock(fileSizeTo); not err.OK()) {
    return err;
  }
  return Error{};
}

// init creates a new database file and initializes its meta pages.
Error DB::init() {
  // Create two meta pages on a buffer.
  std::string buf(this->pageSize * 4, '\0');
  for (int i = 0; i < 2; i++) {
    auto p = this->pageInBuffer(buf, pgid_t(i));
    p->id = pgid_t(i);
    p->flags = metaPageFlag;

    // Initialize the meta page.
    auto m = p->meta();
    m->magic = kMetaMagic;
    m->version = kVersion;
    m->pageSize = uint32_t(this->pageSize);
    m->freelist = 2;
    m->root = BucketStored{.root = 3};
    m->pgid = 4;
    m->txid = txid_t(i);
    m->checksum = m->sum64();
  }

  // Write an empty freelist at page 3.
  auto p = this->pageInBuffer(buf, pgid_t(2));
  p->id = pgid_t(2);
  p->flags = freelistPageFlag;
  p->count = 0;

  // Write an empty leaf page at page 4.
  p = this->pageInBuffer(buf, pgid_t(3));
  p->id = pgid_t(3);
  p->flags = leafPageFlag;
  p->count = 0;

  // Write the buffer to our data file.
  if (auto [_, err] = this->ops.writeAt(buf, 0); not err.OK()) {
    return err;
  }
  if (auto err = file->FDataSync(); not err.OK()) {
    return err;
  }
  this->filesz = buf.size();

  return Error{};
}

// Close releases all database resources.
// It will block waiting for any open transactions to finish
// before closing the database and returning.
Error DB::Close() {
  std::lock_guard rw_g(this->rwlock);
  std::lock_guard m_g(this->metalock);
  std::lock_guard mmap_g(this->mmaplock);

  return this->close();
}

Error DB::close() {
  if (!this->opened) {
    return Error{};
  }

  this->opened = false;

  this->freelist = nullptr;

  // Clear ops.
  this->ops.writeAt = nullptr;

  // Close the mmap.
  if (auto err = this->munmap(); not err.OK()) {
    return err;
  }

  // Close file handles.
  if (this->file != nullptr) {
    // No need to unlock read-only file.
    if (!this->readOnly) {
      // Unlock the file.
      if (auto err = file->FUnLock(); not err.OK()) {
        return Error{absl::StrFormat("bolt.Close(): funlock error: %v", err)};
      }
    }

    // Close the file descriptor.
    if (auto err = this->file->Close(); not err.OK()) {
      return Error{absl::StrFormat("db file close: %v", err)};
    }
    this->file = nullptr;
  }

  this->path = "";
  return Error{};
}

// Begin starts a new transaction.
// Multiple read-only transactions can be used concurrently but only one
// write transaction can be used at a time. Starting multiple write transactions
// will cause the calls to block and be serialized until the current write
// transaction finishes.
//
// Transactions should not be dependent on one another. Opening a read
// transaction and a write transaction in the same goroutine can cause the
// writer to deadlock because the database periodically needs to re-mmap itself
// as it grows and it cannot do that while a read transaction is open.
//
// If a long running read transaction (for example, a snapshot transaction) is
// needed, you might want to set DB.InitialMmapSize to a large enough value
// to avoid potential blocking of write transaction.
//
// IMPORTANT: You must close read-only transactions after you are finished or
// else the database will not reclaim old pages.
std::tuple<std::shared_ptr<Tx>, Error> DB::Begin(bool writable) {
  if (writable) {
    return this->beginRWTx();
  }
  return this->beginTx();
}

std::tuple<std::shared_ptr<Tx>, Error> DB::beginTx() {
  // Lock the meta pages while we initialize the transaction. We obtain
  // the meta lock before the mmap lock because that's the order that the
  // write transaction will obtain them.

  std::lock_guard meta_g(this->metalock);

  // Obtain a read-only lock on the mmap. When the mmap is remapped it will
  // obtain a write lock so all transactions must finish before it can be
  // remapped.
  std::shared_lock mmap_read_g(this->mmaplock);

  // Exit if the database is not open yet.
  if (!this->opened) {
    return {nullptr, Error{ErrorCode::ErrDatabaseNotOpen}};
  }

  // Create a transaction associated with the database.
  auto t = std::make_shared<Tx>();
  t->init(shared_from_this());

  // Keep track of transaction until it closes.
  this->txs.emplace_back(t);
  const size_t n = this->txs.size();

  // Unlock the meta pages.

  // Update the transaction stats.

  std::lock_guard stat_g(this->statlock);
  this->stats.TxN++;
  this->stats.OpenTxN = n;

  return {std::move(t), Error{}};
}

std::tuple<std::shared_ptr<Tx>, Error> DB::beginRWTx() {
  // If the database was opened with Options.ReadOnly, return an error.
  if (this->readOnly) {
    return {nullptr, Error{ErrorCode::ErrDatabaseReadOnly}};
  }

  // Obtain writer lock. This is released by the transaction when it closes.
  // This enforces only one writer transaction at a time.
  std::unique_lock w_g(this->rwlock);

  // Once we have the writer lock then we can lock the meta pages so that
  // we can set up the transaction.
  std::lock_guard meta_g(this->metalock);

  // Exit if the database is not open yet.
  if (!this->opened) {
    return {nullptr, Error{ErrorCode::ErrDatabaseNotOpen}};
  }

  // Create a transaction associated with the database.
  auto t = std::make_shared<Tx>();
  t->writable = true;
  t->init(shared_from_this());
  this->rwtx = t;
  this->freePages();
  return {std::move(t), Error{}};
}

// using txsById = std::vector<Tx *>;  //[]*Tx;

inline bool TxIDLess(const std::weak_ptr<Tx> &a, const std::weak_ptr<Tx> &b) {
  auto a_ptr = a.lock();
  auto b_ptr = b.lock();
  if (!a_ptr || !b_ptr) return false;
  return a_ptr->meta->txid < b_ptr->meta->txid;
}

// func (t txsById) Len() int           { return len(t) }
// func (t txsById) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
// func (t txsById) Less(i, j int) bool { return t[i].meta.txid < t[j].meta.txid }

// freePages releases any pages associated with closed read-only transactions.
void DB::freePages() {
  // Free all pending pages prior to earliest open transaction.
  sort(this->txs.begin(), this->txs.end(), TxIDLess);
  // sort.Sort(txsById(this->txs));
  txid_t minid = 0xFFFFFFFFFFFFFFFF;
  if (this->txs.size() > 0) {
    auto t = txs[0].lock();
    if (t != nullptr) {
      minid = t->meta->txid;
    }
  }
  if (minid > 0) {
    this->freelist->release(minid - 1);
  }
  // Release unused txid extents.
  for (auto &tx : this->txs) {
    auto t = tx.lock();
    if (t == nullptr) {
      continue;
    }
    this->freelist->releaseRange(minid, t->meta->txid - 1);
    minid = t->meta->txid + 1;
  }
  this->freelist->releaseRange(minid, txid_t(0xFFFFFFFFFFFFFFFF));
  // Any page both allocated and freed in an extent is safe to release.
}

// removeTx removes a transaction from the database.
void DB::removeTx(Tx *tx) {
  // Release the read lock on the mmap.
  // this->mmaplock.RUnlock();

  // Use the meta lock to restrict access to the DB object.
  size_t n = 0;
  {
    std::lock_guard meta_g(this->metalock);

    // TODO(byronhe): std::remove_if();
    // Remove the transaction.
    for (size_t i = 0; i < this->txs.size(); ++i) {
      auto t = txs[i].lock();
      if (t.get() == tx) {
        auto last = this->txs.size() - 1;
        this->txs[i] = this->txs[last];
        this->txs[last].reset();
        this->txs.pop_back();
        break;
      }
    }
    n = this->txs.size();

    // Unlock the meta pages.
  }

  // Merge statistics.
  std::lock_guard stat_g(this->statlock);
  this->stats.OpenTxN = n;
  this->stats.tx_stats_.add(tx->stats);
}

// Update executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.
Error DB::Update(std::function<Error(Tx *)> fn) {
  std::shared_ptr<Tx> t;
  Error err;
  tie(t, err) = this->Begin(true);
  if (not err.OK()) {
    return err;
  }

  // Make sure the transaction rolls back in the event of a panic.
  absl::Cleanup rollback([&t]() {
    if (t->db != nullptr) {
      t->rollback();
    }
  });

  // Mark as a managed tx so that the inner function cannot manually commit.
  t->managed = true;

  // If an error is returned from the function then rollback and return error.
  err = fn(t.get());
  t->managed = false;
  if (not err.OK()) {
    t->Rollback();
    return err;
  }

  return Error{t->Commit()};
}

// View executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
//
// Attempting to manually rollback within the function will cause a panic.
Error DB::View(std::function<Error(Tx *)> fn) {
  std::shared_ptr<Tx> t;
  Error err;
  tie(t, err) = this->Begin(false);
  if (not err.OK()) {
    return err;
  }

  // Make sure the transaction rolls back in the event of a panic.
  absl::Cleanup rollback([&t]() {
    if (t->db != nullptr) {
      t->rollback();
    }
  });

  // Mark as a managed tx so that the inner function cannot manually rollback.
  t->managed = true;

  // If an error is returned from the function then pass it through.
  err = fn(t.get());
  t->managed = false;
  if (not err.OK()) {
    t->Rollback();
    return err;
  }

  return Error{t->Rollback()};
}

const static Error &trySolo();

// Batch calls fn as part of a batch. It behaves similar to Update,
// except:
//
// 1. concurrent Batch calls can be combined into a single Bolt
// transaction.
//
// 2. the function passed to Batch may be called multiple times,
// regardless of whether it returns error or not.
//
// This means that Batch function side effects must be idempotent and
// take permanent effect only after a successful return is seen in
// caller.
//
// The maximum batch size and delay can be adjusted with DB.MaxBatchSize
// and DB.MaxBatchDelay, respectively.
//
// Batch is only useful when there are multiple goroutines calling it.
Error DB::BatchRun(std::function<Error(Tx *)> fn) {
  // errCh := make(chan error, 1);

  std::vector<Error> *errCh = nullptr;

  {
    std::lock_guard batch_g(this->batchMu);
    // this->batchMu.Lock();
    if ((this->batch_ == nullptr) || (this->batch_ != nullptr && this->batch_->calls.size() >= this->MaxBatchSize)) {
      // There is no existing batch, or the existing batch is full; start a new one.
      this->batch_ = std::make_unique<struct Batch>();
      batch_->db = this;
      // TODO(timer) std::future ; std::async;
      // this->batch_->timer = time.AfterFunc(this->MaxBatchDelay, this->batch.trigger);
    }
    auto &c = this->batch_->calls.emplace_back();
    c.fn = fn;
    errCh = &c.err;
    if (this->batch_->calls.size() >= this->MaxBatchSize) {
      // wake up batch, it's ready to run
      // go this->batch.trigger();
      this->batch_->trigger();
    }
    // this->batchMu.Unlock();
  }

  auto err = errCh->back();
  errCh->pop_back();
  if (err == trySolo()) {
    err = this->Update(fn);
  }
  return err;
}

// trigger runs the batch if it hasn't already been run.
void Batch::trigger() {
  std::call_once(start, [this]() { this->run(); });
}

// run performs the transactions in the batch and communicates results
// back to this->Batch.
void Batch::run() {
  {
    std::lock_guard g(db->batchMu);
    // this->timer.Stop();
    // Make sure no new work is added to this batch, but don't break
    // other batches.
    if (db->batch_.get() == this) {
      db->batch_ = nullptr;
    }
  }

  for (; calls.size() > 0;) {
    auto failIdx = -1;
    auto err = db->Update([this, &failIdx](Tx *tx) -> Error {
      for (int i = 0; i < calls.size(); ++i) {
        auto c = calls[i];
        if (auto err = db->safelyCall(c.fn, tx); not err.OK()) {
          failIdx = i;
          return err;
        }
      }
      return Error{};
    });

    if (failIdx >= 0) {
      // take the failing transaction out of the batch. it's
      // safe to shorten b.calls here because this->batch no longer
      // points to us, and we hold the mutex anyway.
      auto c = calls[failIdx];
      calls[failIdx] = calls.back();
      calls.pop_back();
      // tell the submitter re-run it solo, continue with the rest of the batch
      c.err.emplace_back(trySolo());
      continue;
    }

    // pass success, or bolt internal errors, to all callers
    for (auto &c : calls) {
      c.err.emplace_back(err);
    }
    break;
  }
}

// trySolo is a special sentinel error value used for signaling that a
// transaction function should be re-run. It should never be seen by
// callers.
const static Error &trySolo() {
  const static Error e{"batch function returned an error and should be re-run solo"};
  return e;
}

struct Panicked {
  // reason interface{}
  Error reason;

  std::string ToError() {
    // TODO
    // if ( auto [err, ok] = reason(error); ok) {
    //     return err.Error();
    // }
    return absl::StrFormat("panic: %v", reason);
  }
};

Error DB::safelyCall(std::function<Error(Tx *)> fn, Tx *tx) {
  Error err;
  absl::Cleanup c{[&]() {
    // TODO
    // if (auto p = recover(); p != nullptr) {
    // 	err = panicked{p};
    // }
  }};
  return fn(tx);
}

// Sync executes fdatasync() against the database file handle.
//
// This is not necessary under normal operation, however, if you use NoSync
// then it allows you to force the database file to sync against the disk.
Error DB::Sync() { return file->FDataSync(); }

// Stats retrieves ongoing performance stats for the database.
// This is only updated when a transaction closes.
Stats DB::GetStats() {
  std::shared_lock s(statlock);
  return this->stats;
}

// This is for internal access to the raw data bytes from the C cursor, use
// carefully, or not at all.
Info DB::GetInfo() {
  Info info;
  info.Data = &this->data[0];
  info.PageSize = pageSize;
  return info;
}

// page retrieves a page reference from the mmap based on the current page size.
struct Page *DB::GetPage(pgid_t id) {
  auto pos = id * pgid_t(this->pageSize);
  return reinterpret_cast<struct Page *>(const_cast<char *>(&this->data[pos]));
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
Page *DB::pageInBuffer(std::string_view b, pgid_t id) {
  return reinterpret_cast<Page *>(const_cast<char *>(&b[id * pgid_t(this->pageSize)]));
}

// meta retrieves the current meta page reference.
Meta *DB::GetMeta() {
  // We have to return the meta with the highest txid which doesn't fail
  // validation. Otherwise, we can cause errors when in fact the database is
  // in a consistent state. metaA is the one with the higher txid.
  auto metaA = this->meta0;
  auto metaB = this->meta1;
  if (this->meta1->txid > this->meta0->txid) {
    metaA = this->meta1;
    metaB = this->meta0;
  }

  // Use higher meta page if valid. Otherwise fallback to previous, if valid.
  if (auto err = metaA->validate(); err == ErrorCode::OK) {
    return metaA;
  } else if (auto err = metaB->validate(); err == ErrorCode::OK) {
    return metaB;
  }

  // This should never be reached, because both meta1 and meta0 were validated
  // on mmap() and we do fsync() on every write.
  panic("bolt.DB.meta(): invalid meta pages");
}

// allocate returns a contiguous block of memory starting at a given page.
std::tuple<Page *, Error> DB::allocate(txid_t txid, int count) {
  auto tx = this->rwtx.lock();
  if (!tx) {
    return {nullptr, Error{"no write transaction"}};
  }

  // Allocate a temporary buffer for the page.
  auto buf = std::make_unique<char[]>(count * pageSize);
  ::memset(buf.get(), 0, count * pageSize);
  auto *p = reinterpret_cast<Page *>(buf.get());
  p->overflow = uint32_t(count - 1);

  // Store the buffer in the transaction so it lives long enough.
  tx->page_buffers.emplace_back(std::move(buf));

  // Use pages from the freelist if they are available.
  if (p->id = this->freelist->Allocate(txid, count); p->id != 0) {
    return {p, Error{}};
  }

  // Resize mmap() if we're at the end.
  p->id = tx->meta->pgid;
  auto minsz = int((p->id + pgid_t(count)) + 1) * this->pageSize;
  if (minsz >= this->datasz) {
    if (auto err = this->mmap(minsz); not err.OK()) {
      return {nullptr, Error{absl::StrFormat("mmap allocate error: %v", err)}};
    }
  }

  // Move the page id high water mark.
  tx->meta->pgid += pgid_t(count);

  return {p, Error{}};
}

// grow grows the size of the database to the given sz.
Error DB::grow(int sz) {
  // Ignore if the new size is less than available file size.
  if (sz <= this->filesz) {
    return Error{};
  }

  // If the data is smaller than the alloc size then only allocate what's needed.
  // Once it goes over the allocation size then allocate in chunks.
  if (this->datasz < this->AllocSize) {
    sz = this->datasz;
  } else {
    sz += this->AllocSize;
  }

  // Truncate and fsync to ensure file size metadata is flushed.
  // https://github.com/boltdb/bolt/issues/284
  if (!this->NoGrowSync && !this->readOnly) {
    // if (runtime.GOOS != "windows") {
    if (auto err = this->file->Truncate(sz); not err.OK()) {
      return Error{absl::StrFormat("file resize error: %v", err)};
    }
    //}
    if (auto err = this->file->Sync(); not err.OK()) {
      return Error{absl::StrFormat("file sync error: %v", err)};
    }
    if (this->Mlock) {
      // unlock old file and lock new one
      if (auto err = this->mrelock(this->filesz, sz); not err.OK()) {
        return Error{absl::StrFormat("mlock/munlock error: %v", err)};
      }
    }
  }

  this->filesz = sz;
  return Error{};
}

bool DB::IsReadOnly() { return this->readOnly; }

pgid_vec DB::freepages() {
  std::shared_ptr<Tx> tx;
  Error err;
  std::tie(tx, err) = this->beginTx();
  absl::Cleanup rollback_clean{[&tx]() {
    auto err = tx->Rollback();
    if (err != ErrorCode::OK) {
      panic("freepages: failed to rollback tx");
    }
  }};
  if (not err.OK()) {
    panic("freepages: failed to open read only tx");
  }

  std::map<pgid_t, Page *> reachable;
  std::map<pgid_t, bool> nofreed;
  std::vector<Error> ech;
  tx->checkBucket(tx->root.get(), reachable, nofreed, ech);
  // go func() {
  for (const auto &e : ech) {
    panic(absl::StrFormat("freepages: failed to get all reachable pages (%v)", e));
  }
  //}();
  // close(ech);

  pgid_vec fids;
  for (pgid_t i = 2; i < this->GetMeta()->pgid; i++) {
    if (auto ok = reachable[i]; !ok) {
      fids.emplace_back(i);
    }
  }
  return fids;
}

// DefaultOptions represent the options used if nullptr options are passed into Open().
// No timeout is used which will cause Bolt to wait indefinitely for a lock.
const Options &DefaultOptions() {
  const static Options opt = {
      .Timeout = std::chrono::milliseconds(0),
      .NoGrowSync = false,
      .free_list_type_ = FreelistType::FreelistArrayType,
  };
  return opt;
}

// Sub calculates and returns the difference between two sets of database stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
Stats Stats::Sub(const Stats &other) {
  Stats diff;
  const auto &s = *this;
  diff.FreePageN = s.FreePageN;
  diff.PendingPageN = s.PendingPageN;
  diff.FreeAlloc = s.FreeAlloc;
  diff.FreelistInuse = s.FreelistInuse;
  diff.TxN = s.TxN - other.TxN;
  diff.tx_stats_ = s.tx_stats_.Sub(other.tx_stats_);
  return diff;
}

// validate checks the markej bytes and version of the meta page to ensure it matches this binary.
ErrorCode Meta::validate() {
  if (magic != kMetaMagic) {
    return ErrorCode::ErrInvalid;
  } else if (this->version != kVersion) {
    return ErrorCode::ErrVersionMismatch;
  } else if (this->checksum != 0 && this->checksum != this->sum64()) {
    return ErrorCode::ErrChecksum;
  }
  return ErrorCode::OK;
}

// copy copies one meta object to another.
void Meta::copy(Meta *dest) const { *dest = *this; }

// write writes the meta onto a page.
void Meta::write(Page *p) {
  if (this->root.root >= this->pgid) {
    panic(absl::StrFormat("root bucket pgid (%d) above high water mark (%d)", this->root.root, this->pgid));
  } else if (this->freelist >= this->pgid && this->freelist != pgidNoFreelist) {
    // TODO: reject pgidNoFreeList if !NoFreelistSync
    panic(absl::StrFormat("freelist pgid (%d) above high water mark (%d)", this->freelist, this->pgid));
  }

  // Page id is either going to be 0 or 1 which we can determine by the transaction ID.
  p->id = pgid_t(this->txid % 2);
  p->flags |= metaPageFlag;

  // Calculate the checksum.
  this->checksum = this->sum64();

  this->copy(p->meta());
}

// generates the checksum for the meta using FNV-1a (compatible with Go's hash/fnv.New64a).
uint64_t Meta::sum64() {
  // Hash all bytes up to (but not including) the checksum field,
  // matching Go's: h.Write((*[unsafe.Offsetof(Meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
  const auto *data = reinterpret_cast<const uint8_t *>(this);
  const size_t len = offsetof(Meta, checksum);

  // FNV-1a 64-bit
  uint64_t hash = 14695981039346656037ULL;  // FNV offset basis
  for (size_t i = 0; i < len; i++) {
    hash ^= static_cast<uint64_t>(data[i]);
    hash *= 1099511628211ULL;  // FNV prime
  }
  return hash;
}

}  // namespace bboltpp
