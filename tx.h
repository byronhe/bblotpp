#pragma once
#include <absl/time/time.h>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string_view>
#include <tuple>
#include <vector>
#include "errors.h"
#include "file.h"
#include "node.h"
#include "page.h"

namespace bboltpp {
class DB;
struct Bucket;
struct Cursor;

// txid represents the internal transaction identifier.
using txid_t = uint64_t;

struct TxStats {
  // Page statistics.
  int PageCount;  // number of page allocations
  int PageAlloc;  // total bytes allocated

  // Cursor statistics.
  int CursorCount;  // number of cursors created

  // Node statistics
  int NodeCount;  // number of node allocations
  int NodeDeref;  // number of node dereferences

  // Rebalance statistics.
  int Rebalance;                            // number of node rebalances
  std::chrono::microseconds RebalanceTime;  // total time spent rebalancing
  absl::Duration rebalance_time_;

  // Split/Spill statistics.
  int Split;                            // number of nodes split
  int Spill;                            // number of nodes spilled
  std::chrono::microseconds SpillTime;  // total time spent spilling
  absl::Duration spill_time_;

  // Write statistics.
  int Write;                            // number of writes performed
  std::chrono::microseconds WriteTime;  // total time spent writing to disk
  absl::Duration write_time_;

  TxStats &add(const TxStats &other);
  TxStats Sub(const TxStats &other) const;
};

// Tx represents a read-only or read/write transaction on the database.
// Read-only transactions can be used for retrieving values for keys and
// creating cursors. Read/write transactions can create and remove buckets and
// create and remove keys.
//
// IMPORTANT: You must commit or rollback transactions when you are done with
// them. Pages can not be reclaimed by the writer until no more transactions
// are using them. A long running read transaction can cause the database to
// quickly grow.
struct Tx : public std::enable_shared_from_this<Tx> {
  bool writable = false;
  bool managed;
  std::shared_ptr<DB> db;
  std::unique_ptr<Meta> meta;
  std::shared_ptr<Bucket> root;
  std::map<pgid_t, Page *> pages;
  std::vector<std::unique_ptr<char[]>> page_buffers;  // owns allocated page memory
  TxStats stats;
  std::vector<std::function<void()>> commitHandlers;

  // WriteFlag specifies the flag for write-related methods like WriteTo().
  // Tx opens the database file with the specified flag to copy the data.
  //
  // By default, the flag is unset, which works well for mostly in-memory
  // workloads. For databases that are much larger than available RAM,
  // set the flag to syscall.O_DIRECT to avoid trashing the page cache.
  int WriteFlag;

  void init(std::shared_ptr<DB> db);
  int ID();
  std::shared_ptr<DB> GetDB();
  int64_t Size();
  bool Writable();
  std::unique_ptr<Cursor> GetCursor();
  const TxStats &Stats();
  std::shared_ptr<Bucket> FindBucketByName(std::string_view name);
  std::tuple<std::shared_ptr<Bucket>, ErrorCode> CreateBucket(std::string_view name);
  std::tuple<std::shared_ptr<Bucket>, ErrorCode> CreateBucketIfNotExists(std::string_view name);
  ErrorCode DeleteBucket(std::string_view name);
  ErrorCode ForEach(std::function<ErrorCode(std::string_view, const std::shared_ptr<Bucket> &)> fn);
  void OnCommit(std::function<void()> fn);
  ErrorCode Commit();
  ErrorCode commitFreelist();
  ErrorCode Rollback();
  void nonPhysicalRollback();
  void rollback();
  void close();
  // ErrorCode Copy(std::ostream &os);
  std::tuple<int64_t, ErrorCode> WriteTo(File *w);
  ErrorCode CopyFile(std::string_view path, uint32_t mode);
  std::vector<Error> Check();  // <-chan error
  ErrorCode check();
  void checkBucket(struct Bucket *b, std::map<pgid_t, struct Page *> &reachable, std::map<pgid_t, bool> &freed,
                   std::vector<Error> &ch);
  std::tuple<Page *, ErrorCode> allocate(int count);
  ErrorCode write();
  ErrorCode writeMeta();
  Page *page(pgid_t id);
  void forEachPage(pgid_t pgid, int depth, std::function<void(Page *, int)> fn);
  std::tuple<std::unique_ptr<struct PageInfo>, ErrorCode> GetPageInfo(int id);
};

// TxStats represents statistics about the actions performed by the transaction.

}  // namespace bboltpp
