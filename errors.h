#pragma once
#include <string>
#include "absl/strings/str_format.h"

namespace bboltpp {

enum class ErrorCode {

  OK = 0,

  // These errors can be returned when opening or calling methods on a DB.

  // ErrDatabaseNotOpen is returned when a DB instance is accessed before it
  // is opened or after it is closed.
  ErrDatabaseNotOpen = -10000,  // = errors.New("database not open")

  // ErrDatabaseOpen is returned when opening a database that is
  // already open.
  ErrDatabaseOpen = -10001,  // errors.New("database already open")

  // ErrInvalid is returned when both meta pages on a database are invalid.
  // This typically occurs when a file is not a bolt database.
  ErrInvalid = -10002,  // errors.New("invalid database")

  // ErrVersionMismatch is returned when the data file was created with a
  // different version of Bolt.
  ErrVersionMismatch,  //= errors.New("version mismatch")

  // ErrChecksum is returned when either meta page checksum does not match.
  ErrChecksum,  //= errors.New("checksum error")

  // ErrTimeout is returned when a database cannot obtain an exclusive lock
  // on the data file after the timeout passed to Open().
  ErrTimeout,  //= errors.New("timeout")

  // These errors can occur when beginning or committing a Tx.

  // ErrTxNotWritable is returned when performing a write operation on a
  // read-only transaction.
  ErrTxNotWritable,  //= errors.New("tx not writable")

  // ErrTxClosed is returned when committing or rolling back a transaction
  // that has already been committed or rolled back.
  ErrTxClosed,  //= errors.New("tx closed")

  // ErrDatabaseReadOnly is returned when a mutating transaction is started on a
  // read-only database.
  ErrDatabaseReadOnly,  //= errors.New("database is in read-only mode")

  // These errors can occur when putting or deleting a value or a bucket.

  // ErrBucketNotFound is returned when trying to access a bucket that has
  // not been created yet.
  ErrBucketNotFound,  //= errors.New("bucket not found")

  // ErrBucketExists is returned when creating a bucket that already exists.
  ErrBucketExists,  //= errors.New("bucket already exists")

  // ErrBucketNameRequired is returned when creating a bucket with a blank name.
  ErrBucketNameRequired,  //= errors.New("bucket name required")

  // ErrKeyRequired is returned when inserting a zero-length key.
  ErrKeyRequired,  //= errors.New("key required")

  // ErrKeyTooLarge is returned when inserting a key that is larger than
  // MaxKeySize.
  ErrKeyTooLarge,  //= errors.New("key too large")

  // ErrValueTooLarge is returned when inserting a value that is larger than
  // MaxValueSize.
  ErrValueTooLarge,  //= errors.New("value too large")

  // ErrIncompatibleValue is returned when trying create or delete a bucket
  // on an existing non-bucket key or when trying to create or delete a
  // non-bucket key on an existing bucket key.
  ErrIncompatibleValue,  //= errors.New("incompatible value")

  ErrMeta0CopyErr,
  ErrMeta1CopyErr,
  ErrCopyNReadError,
  ErrCopyNWriteError,
};

class Error {
 public:
  int int_code_ = 0;
  ErrorCode err_code_ = ErrorCode::OK;
  std::string err_msg_;

  explicit Error() = default;
  explicit Error(ErrorCode ec) : err_code_(ec) {}
  explicit Error(int c) : int_code_(c == 0 ? 0 : errno), err_code_(c == 0 ? ErrorCode::OK : ErrorCode::ErrInvalid) {}
  explicit Error(std::string msg) : err_msg_(std::move(msg)) {}

  bool OK() const { return err_code_ == ErrorCode::OK && int_code_ == 0 && err_msg_.empty(); }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const Error& e) {
    absl::Format(&sink, " %d %d %s", e.int_code_, static_cast<int>(e.err_code_), e.err_msg_.c_str());
  }

  bool operator==(const Error& other) const {
    return int_code_ == other.int_code_ && err_code_ == other.err_code_ && err_msg_ == other.err_msg_;
  }
};

}  // namespace bboltpp
