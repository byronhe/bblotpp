#pragma once
#include <absl/time/time.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string_view>
#include <tuple>
#include <vector>

#include "bucket.h"
#include "errors.h"
#include "file.h"
#include "freelist.h"
#include "page.h"
#include "tx.h"
namespace bboltpp {

// The largest step that can be taken when remapping the mmap.
constexpr size_t maxMmapStep = 1UL << 30;  // 1GB

// The data file format version.
constexpr uint32_t kVersion = 2;

// Represents a marker value to indicate that a file is a Bolt DB.
constexpr uint32_t kMetaMagic = 0xED0CDAED;

constexpr pgid_t pgidNoFreelist = 0xffffffffffffffff;

// IgnoreNoSync specifies whether the NoSync field of a DB is ignored when
// syncing changes to a file.  This is required as some operating systems,
// such as OpenBSD, do not have a unified buffer cache (UBC) and writes
// must be synchronized using the msync(2) syscall.
bool IgnoreNoSync();

// Default values if not set in a DB instance.

constexpr int DefaultMaxBatchSize = 1000;
constexpr std::chrono::milliseconds DefaultMaxBatchDelay{10};  //* time.Millisecond;
constexpr size_t DefaultAllocSize = 16 * 1024 * 1024;

// default page size for db is set to the OS page size.
size_t defaultPageSize();

struct Info {
  const char *Data = 0;
  int PageSize = 0;
};

struct Meta {
  uint32_t magic = 0;
  uint32_t version = 0;
  uint32_t pageSize = 0;
  uint32_t flags = 0;
  BucketStored root;
  pgid_t freelist = 0;
  pgid_t pgid = 0;
  txid_t txid = 0;
  uint64_t checksum = 0;

  ErrorCode validate();
  void copy(struct Meta *dest) const;
  void write(struct Page *p);

  template <typename H>
  friend H AbslHashValue(H h, const Meta &m) {
    return H::combine(std::move(h), m.magic, m.version, m.pageSize, m.flags, m.flags, m.root, m.freelist, m.pgid,
                      m.txid);
  }

  uint64_t sum64();
};

// Stats represents statistics about the database.
struct Stats {
  // Freelist stats
  int FreePageN = 0;      // total number of free pages on the freelist
  int PendingPageN = 0;   // total number of pending pages on the freelist
  int FreeAlloc = 0;      // total bytes allocated in free pages
  int FreelistInuse = 0;  // total bytes used by the freelist

  // Transaction stats
  int TxN = 0;      // total number of started read transactions
  int OpenTxN = 0;  // number of currently open read transactions

  TxStats tx_stats_;  // global, ongoing stats.

  Stats Sub(const Stats &other);
};

class DB;
struct Options;

struct Call {
  std::function<Error(Tx *)> fn;
  std::vector<Error> err;
  // fn  func(*Tx) error
  // err chan<- error
};

struct Batch {
  DB *db = nullptr;
  // timer *time.Timer
  std::once_flag start = {};
  std::vector<Call> calls;

 public:
  Batch() = default;
  void trigger();
  void run();
};

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained
// through the DB. All the functions on DB will return a ErrDatabaseNotOpen if
// accessed before Open() is called.
class DB {
 public:
  // When enabled, the database will perform a Check() after every commit.
  // A panic is issued if the database is in an inconsistent state. This
  // flag has a large performance impact so it should only be used for
  // debugging purposes.
  bool StrictMode = false;

  // Setting the NoSync flag will cause the database to skip fsync()
  // calls after each commit. This can be useful when bulk loading data
  // into a database and you can restart the bulk load in the event of
  // a system failure or database corruption. Do not set this flag for
  // normal use.
  //
  // If the package global IgnoreNoSync constant is true, this value is
  // ignored.  See the comment on that constant for more details.
  //
  // THIS IS UNSAFE. PLEASE USE WITH CAUTION.
  bool NoSync = false;

  // When true, skips syncing freelist to disk. This improves the
  // database write performance under normal operation, but requires a
  // full database re-sync during recovery.
  bool NoFreelistSync = false;

  // FreelistType sets the backend freelist type. There are two
  // options. Array which is simple but endures dramatic
  // performance degradation if database is large and
  // framentation in freelist is common. The alternative one is
  // using hashmap, it is faster in almost all circumstances but
  // it doesn't guarantee that it offers the smallest page id
  // available. In normal case it is safe. The default type is
  // array
  FreelistType freelist_type = FreelistType::FreelistMapType;

  // When true, skips the truncate call when growing the database. Setting this
  // to true is only safe on non-ext3/ext4 systems. Skipping truncation avoids
  // preallocation of hard drive space and bypasses a truncate() and fsync()
  // syscall on remapping.  https://github.com/boltdb/bolt/issues/284
  bool NoGrowSync = false;

  // If you want to read the entire database fast, you can set MmapFlag to
  // syscall.MAP_POPULATE on Linux 2.6.23+ for sequential read-ahead.
  int MmapFlags = false;

  // MaxBatchSize is the maximum size of a batch. Default value is copied from
  // DefaultMaxBatchSize in Open.  If <=0, disables batching.  Do not change
  // concurrently with calls to Batch.
  int MaxBatchSize = false;

  // MaxBatchDelay is the maximum delay before a batch starts. Default value is
  // copied from DefaultMaxBatchDelay in Open.  If <=0, effectively disables
  // batching.  Do not change concurrently with calls to Batch.
  std::chrono::milliseconds MaxBatchDelay;  // time.Duration
  absl::Duration max_batch_delay;           // time.Duration

  // AllocSize is the amount of space allocated when the database needs to
  // create new pages. This is done to amortize the cost of truncate() and
  // fsync() when growing the data file.
  int AllocSize = 0;

  // Mlock locks database file in memory when set to true.  It prevents major
  // page faults, however used memory can't be reclaimed. Supported only on Unix
  // via mlock/munlock syscalls.
  bool Mlock = false;

  std::string path;
  OpenFileFunc openFile;
  std::unique_ptr<File> file;
  std::string_view dataref;  // mmap'ed readonly, write throws SEGV
  std::string_view data;     //*[maxMapSize]byte
  int datasz = 0;
  int filesz = 0;  // current on disk file size
  Meta *meta0 = nullptr;
  Meta *meta1 = nullptr;
  int pageSize = 0;
  bool opened = false;
  Tx *rwtx = nullptr;
  std::vector<Tx *> txs;
  Stats stats;
  std::unique_ptr<FreeList> freelist;

  std::once_flag freelistLoad;

  // pagePool sync.Pool

  std::mutex batchMu;
  std::unique_ptr<Batch> batch_;

  std::mutex rwlock;           // Allows only one writer at a time.
  std::mutex metalock;         // Protects meta page access.
  std::shared_mutex mmaplock;  // Protects mmap access during remapping.
  std::shared_mutex statlock;  // Protects stats access.

  struct ops {
    std::function<std::tuple<int, Error>(std::string_view b, int64_t offset)> writeAt;
    // writeAt func(b[] byte, off int64)(n int, err error)
  } ops;

  // Read only mode.
  // When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
  bool readOnly = false;

 public:
  void trigger();
  std::string Path();
  std::string GoString();
  std::string String();
  static std::tuple<std::unique_ptr<DB>, Error> Open(std::string_view path, uint32_t mode, const Options *options_ptr);
  void loadFreelist();
  bool hasSyncedFreelist();
  Error mmap(int minsz);
  Error munmap();
  std::tuple<int, Error> mmapSize(int size);
  Error munlock(int fileSize);
  Error mlock(int fileSize);
  Error mrelock(int fileSizeFrom, int fileSizeTo);
  Error init();
  Error Close();
  Error close();
  std::tuple<std::unique_ptr<Tx>, Error> Begin(bool writable);
  std::tuple<std::unique_ptr<Tx>, Error> beginTx();
  std::tuple<std::unique_ptr<Tx>, Error> beginRWTx();
  void freePages();
  void removeTx(Tx *tx);
  Error Update(std::function<Error(Tx *)> fn);
  Error View(std::function<Error(Tx *)> fn);
  Error BatchRun(std::function<Error(Tx *)> fn);
  Error safelyCall(std::function<Error(Tx *)> fn, Tx *tx);
  Error Sync();
  Stats GetStats();
  Info GetInfo();
  struct Page *GetPage(pgid_t id);
  struct Page *pageInBuffer(std::string_view b, pgid_t id);
  Meta *GetMeta();
  std::tuple<Page *, Error> allocate(txid_t txid, int count);
  Error grow(int sz);
  bool IsReadOnly();
  pgid_vec freepages();
};

// Options represents the options that can be set when opening a database.
struct Options {
  // Timeout is the amount of time to wait to obtain a file lock.
  // When set to zero it will wait indefinitely. This option is only
  // available on Darwin and Linux.
  std::chrono::milliseconds Timeout;  // time.Duration

  // Sets the DB.NoGrowSync flag before memory mapping the file.
  bool NoGrowSync;

  // Do not sync freelist to disk. This improves the database write
  // performance under normal operation, but requires a full database
  // re-sync during recovery.
  bool NoFreelistSync;

  // FreelistType sets the backend freelist type. There are two options. Array
  // which is simple but endures dramatic performance degradation if database is
  // large and framentation in freelist is common. The alternative one is using
  // hashmap, it is faster in almost all circumstances but it doesn't guarantee
  // that it offers the smallest page id available. In normal case it is safe.
  // The default type is array
  FreelistType free_list_type_;

  // Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to grab
  // a shared lock (UNIX).
  bool ReadOnly;

  // Sets the DB.MmapFlags flag before memory mapping the file.
  int MmapFlags;

  // InitialMmapSize is the initial mmap size of the database in bytes. Read
  // transactions won't block write transaction if the InitialMmapSize is large
  // enough to hold database mmap size. (See DB.Begin for more information) If
  // <=0, the initial map size is 0.  If initialMmapSize is smaller than the
  // previous database size, it takes no effect.
  int InitialMmapSize;

  // PageSize overrides the default OS page size.
  int PageSize;

  // NoSync sets the initial value of DB.NoSync. Normally this can just be set
  // directly on the DB itself when returned from Open(), but this option is
  // useful in APIs which expose Options but not the underlying DB.
  bool NoSync;

  // OpenFile is used to open files.  It defaults to os.OpenFile.  This option
  // is useful for writing hermetic tests.
  OpenFileFunc openFile;
  // OpenFile func(string, int, os.FileMode)(*os.File, error)

  // Mlock locks database file in memory when set to true.  It prevents
  // potential page faults, however used memory can't be reclaimed. (UNIX
  // only)
  bool Mlock;
};

}  // namespace bboltpp
