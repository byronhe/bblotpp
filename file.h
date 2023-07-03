#pragma once
#include <sys/types.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include "errors.h"

namespace bboltpp {

// The time elapsed between consecutive file locking attempts.
constexpr std::chrono::milliseconds flockRetryTimeout{50};  //* time.Millisecond;

class File;
using OpenFileFunc = std::function<std::tuple<std::unique_ptr<File>, Error>(std::string_view, int, int)>;

class File {
  int fd_ = -1;
  std::string name_;
  void *mmap_addr_ = nullptr;
  size_t mmap_size_ = 0;

 public:
  File(const File &) = delete;
  File &operator=(File &) = delete;
  File() = default;
  File(File &&from) noexcept
      : fd_(from.fd_), name_(std::move(from.name_)), mmap_addr_(from.mmap_addr_), mmap_size_(from.mmap_size_) {
    from.fd_ = -1;
    from.mmap_addr_ = nullptr;
    from.mmap_size_ = 0;
  }
  ~File();

  static std::tuple<std::unique_ptr<File>, Error> OSOpenFile(std::string_view path, int flags, int mode);

  // int Open(std::string_view path, int flags, int mode);

  Error Close();

  std::string Name() const { return name_; }

  Error FDataSync();

  Error Sync();

  Error FLock(bool exclusive, std::chrono::milliseconds timeout);

  Error FUnLock();

  Error Truncate(size_t to_size);

  Error Mmap(size_t sz, int MmapFlags);

  Error Munmap();

  Error Mlock(size_t fileSize);

  Error MUnlock(size_t fileSize);

  ssize_t Pread(size_t count, off_t offset, std::string *to_buf);

  ssize_t Write(std::string_view buff);

  std::tuple<int, Error> WriteAt(std::string_view b, int64_t offset);

  std::tuple<int, Error> ReadAt(int64_t offset, std::string *to_buf);

  struct FileInfo {
    size_t size_ = 0;
    size_t Size() const { return size_; }
  };

  std::tuple<FileInfo, Error> Stat();
};

}  // namespace bboltpp
