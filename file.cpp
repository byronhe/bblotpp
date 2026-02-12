#include "file.h"
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <memory>
#include <thread>
#include "absl/strings/str_format.h"
#include "errors.h"

namespace bboltpp {

File::~File() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
  if (mmap_addr_ != nullptr) {
    ::munmap(mmap_addr_, mmap_size_);
  }
}

Error File::Close() {
  if (fd_ >= 0) {
    int ret = ::close(fd_);
    fd_ = -1;
    return Error{ret};
  }
  return Error{};
}

std::tuple<std::unique_ptr<File>, Error> File::OSOpenFile(std::string_view path, int flags, int mode) {
  auto f = std::make_unique<File>();
  int const fd = ::open(path.data(), flags, mode);
  if (fd >= 0) {
    f->fd_ = fd;
    return {std::move(f), Error{}};
  }
  return {nullptr, Error{errno}};
}

// fdatasync flushes written data to a file descriptor.
Error File::FDataSync() {
#ifdef __APPLE__
  // macOS doesn't have fdatasync, use fsync instead
  return Error{::fsync(fd_)};
#else
  return Error{::fdatasync(fd_)};
#endif
}

Error File::Sync() { return Error{::fsync(fd_)}; }

// flock acquires an advisory lock on a file descriptor.
Error File::FLock(bool exclusive, std::chrono::milliseconds timeout) {
  std::chrono::high_resolution_clock::time_point t{};
  if (timeout.count() != 0) {
    t = std::chrono::high_resolution_clock::now();
  }
  int flag = LOCK_NB;
  if (exclusive) {
    flag |= LOCK_EX;
  } else {
    flag |= LOCK_SH;
  }
  for (;;) {
    // Attempt to obtain an exclusive lock.
    const int err = flock(fd_, flag);
    if (err == 0) {
      return Error{};
    }
    if (errno != EWOULDBLOCK) {
      return Error{errno};
    }

    // If we timed out then return an error.
    if (timeout.count() != 0 && (std::chrono::high_resolution_clock::now() + flockRetryTimeout) > t + timeout) {
      return Error{ErrorCode::ErrTimeout};
    }

    // Wait for a bit and try again.
    std::this_thread::sleep_for(flockRetryTimeout);
  }
}

// funlock releases an advisory lock on a file descriptor.
Error File::FUnLock() { return Error{flock(fd_, LOCK_UN)}; }

Error File::Truncate(size_t to_size) { return Error{ftruncate(fd_, to_size)}; }

// mmap memory maps a DB's data file.
Error File::Mmap(size_t sz, int MmapFlags) {
  // Map the data file to memory.
  const auto ret = ::mmap(nullptr, sz, PROT_READ, MAP_SHARED | MmapFlags, fd_, 0);

  if (ret == MAP_FAILED) {
    return Error{errno};
  }

  // Advise the kernel that the mmap is accessed randomly.
  int const err = ::madvise(ret, sz, MADV_RANDOM);
  if (err != 0 && errno != ENOSYS) {
    // Ignore not implemented error in kernel because it still works.
    return Error{absl::StrFormat("madvise: %d errno %d ", err, errno)};
  }

  // Save the original byte slice and convert to a byte array pointer.
  mmap_addr_ = ret;
  mmap_size_ = sz;
  return Error{};
}

// munmap unmaps a DB's data file from memory.
Error File::Munmap() {
  // Ignore the unmap if we have no mapped data.
  if (mmap_addr_ == nullptr) {
    return Error{};
  }

  // Unmap using the original byte slice.
  auto ret = ::munmap(mmap_addr_, mmap_size_);
  mmap_addr_ = nullptr;
  mmap_size_ = 0;
  return Error{ret};
}

// mlock locks memory of db file
Error File::Mlock(size_t fileSize) {
  auto size_to_lock = fileSize;
  if (size_to_lock > mmap_size_) {
    // Can't lock more than mmaped slice
    size_to_lock = mmap_size_;
  }
  auto ret = ::mlock(mmap_addr_, size_to_lock);
  return Error{ret};
}

// munlock unlocks memory of db file
Error File::MUnlock(size_t fileSize) {
  if (mmap_addr_ == nullptr) {
    return Error{};
  }

  auto size_to_unlock = fileSize;
  if (size_to_unlock > mmap_size_) {
    // Can't unlock more than mmaped slice
    size_to_unlock = mmap_size_;
  }

  auto ret = ::munlock(mmap_addr_, mmap_size_);
  return Error{ret};
}

ssize_t File::Pread(size_t count, off_t offset, std::string *to_buf) {
  const auto before_size = to_buf->size();
  to_buf->resize(before_size + count);

  return ::pread(fd_, &to_buf->at(before_size), count, offset);
}

ssize_t File::Write(std::string_view buff) { return ::write(fd_, buff.data(), buff.size()); }

std::tuple<int, Error> File::WriteAt(std::string_view b, int64_t offset) {
  const ssize_t ret = ::pwrite(fd_, b.data(), b.size(), offset);
  if (-1 == ret) {
    return {(int)ret, Error((int)ret)};
  }

  return {(int)ret, Error()};
}

std::tuple<int, Error> File::ReadAt(int64_t offset, std::string *to_buf) {
  auto sz = to_buf->size();
  to_buf->clear();
  ssize_t const ret = Pread(sz, offset, to_buf);
  if (-1 == ret) {
    return {ret, Error(ret)};
  }

  return {ret, Error()};
}

std::tuple<File::FileInfo, Error> File::Stat() {
  struct stat file_stat = {};
  const int ret = ::fstat(fd_, &file_stat);
  FileInfo info;
  info.size_ = file_stat.st_size;
  return {info, Error{ret}};
}

}  // namespace bboltpp
