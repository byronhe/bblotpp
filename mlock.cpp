#include "mlock.h"
#include <memory>

#ifdef __unix__
#  include <sys/mman.h>
#  include <unistd.h>
#endif

namespace bboltpp {

#ifdef __unix__

Error UnixMLock::Lock(void* addr, size_t size) {
  if (::mlock(addr, size) != 0) {
    return Error{errno};
  }
  return Error{};
}

Error UnixMLock::Unlock(void* addr, size_t size) {
  if (::munlock(addr, size) != 0) {
    return Error{errno};
  }
  return Error{};
}

bool UnixMLock::IsSupported() const { return true; }

#else

Error UnixMLock::Lock(void* addr, size_t size) { return Error{ErrorCode::ErrInvalid}; }

Error UnixMLock::Unlock(void* addr, size_t size) { return Error{ErrorCode::ErrInvalid}; }

bool UnixMLock::IsSupported() const { return false; }

#endif

Error WindowsMLock::Lock(void* addr, size_t size) {
  // TODO: Implement Windows VirtualLock
  return Error{ErrorCode::ErrInvalid};
}

Error WindowsMLock::Unlock(void* addr, size_t size) {
  // TODO: Implement Windows VirtualUnlock
  return Error{ErrorCode::ErrInvalid};
}

bool WindowsMLock::IsSupported() const {
  return false;  // Not implemented yet
}

std::unique_ptr<MLock> CreateMLock() {
#ifdef __unix__
  return std::make_unique<UnixMLock>();
#elif defined(_WIN32)
  return std::make_unique<WindowsMLock>();
#else
  return std::make_unique<UnixMLock>();  // Default to Unix implementation
#endif
}

}  // namespace bboltpp
