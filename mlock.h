#pragma once

#include <cstddef>
#include "errors.h"

namespace bboltpp {

// Mlock locks database file in memory when set to true. It prevents major
// page faults, however used memory can't be reclaimed. Supported only on Unix
// via mlock/munlock syscalls.
class MLock {
 public:
  virtual ~MLock() = default;

  // Lock memory of the specified size starting from the given address
  virtual Error Lock(void* addr, size_t size) = 0;

  // Unlock memory of the specified size starting from the given address
  virtual Error Unlock(void* addr, size_t size) = 0;

  // Check if mlock is supported on this platform
  virtual bool IsSupported() const = 0;
};

// Unix implementation of MLock
class UnixMLock : public MLock {
 public:
  Error Lock(void* addr, size_t size) override;
  Error Unlock(void* addr, size_t size) override;
  bool IsSupported() const override;
};

// Windows implementation of MLock (stub for now)
class WindowsMLock : public MLock {
 public:
  Error Lock(void* addr, size_t size) override;
  Error Unlock(void* addr, size_t size) override;
  bool IsSupported() const override;
};

// Factory function to create appropriate MLock implementation
std::unique_ptr<MLock> CreateMLock();

}  // namespace bboltpp
