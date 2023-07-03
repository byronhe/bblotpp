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
  // See: https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
  //
  // This memory is not allocated from C, but it is unmanaged by Go's
  // garbage collector and should behave similarly, and the compiler
  // should produce similar code.  Note that this conversion allows a
  // subslice to begin after the base address, with an optional offset,
  // while the URL above does not cover this case and only slices from
  // index 0.  However, the wiki never says that the address must be to
  // the beginning of a C allocation (or even that malloc was used at
  // all), so this is believed to be correct.
  T* arr = unsafeAdd<T>(base, offset);
  return {reinterpret_cast<char*>(&arr[i]),
          static_cast<size_t>(reinterpret_cast<char*>(&arr[j]) - reinterpret_cast<char*>(&arr[i]))};
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
}  // namespace bboltpp
