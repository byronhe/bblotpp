#pragma once
#include <cstdint>
#include <string_view>

namespace bboltpp {

template <typename T>
T* unsafeAdd(T* base, uint64_t offset) {
  return reinterpret_cast<T*>(reinterpret_cast<char*>(base) + offset);
}

template <typename T>
T* unsafeIndex(T* base, uint64_t offset, uint64_t elemsz, int n) {
  return reinterpret_cast<T*>(reinterpret_cast<char*>(base) + offset + static_cast<uint64_t>(n) * elemsz);
}

template <typename T>
std::string_view unsafeByteSlice(T* base, uint64_t offset, int i, int j) {
  // Returns a string_view over the byte range [offset+i, offset+j) from base.
  // All indexing is in bytes (matching Go's unsafe.Pointer semantics).
  char* start = reinterpret_cast<char*>(base) + offset + i;
  return {start, static_cast<size_t>(j - i)};
}

// unsafeSlice modifies the data, len, and cap of a slice variable pointed to by
// the slice parameter.  This helper should be used over other direct
// manipulation of reflect.SliceHeader to prevent misuse, namely, converting
// from reflect.SliceHeader to a Go slice type.
// template<typename T>
// void unsafeSlice(T* slice, T* data, int len) {
// 	s := (*reflect.SliceHeader)(slice)
// 	s.Data = uintptr(data)
// 	s.Cap = len
// 	s.Len = len
// }

// LoadBucket loads a bucket from a byte buffer
template <typename T>
T* LoadBucket(void* buf) {
  return reinterpret_cast<T*>(buf);
}

// LoadPage loads a page from a byte buffer
template <typename T>
T* LoadPage(void* buf) {
  return reinterpret_cast<T*>(buf);
}

// LoadPageMeta loads a page meta from a byte buffer at the specified offset
template <typename T>
T* LoadPageMeta(void* buf, size_t offset) {
  return reinterpret_cast<T*>(static_cast<char*>(buf) + offset);
}

}  // namespace bboltpp
