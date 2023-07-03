#include "page.h"
#include <iterator>
#include "absl/strings/escaping.h"
namespace bboltpp {

std::string hexdump(Page* p, size_t n) {
  return absl::BytesToHexString({reinterpret_cast<const char*>(p), n});
  // buf := unsafeByteSlice(unsafe.Pointer(p), 0, 0, n)
  // fmt.Fprintf(os.Stderr, "%x\n", buf)
}

// typ returns a human readable page type string used for debugging.
const char* Page::type() const {
  if (flags & branchPageFlag) {
    return "branch";
  } else if (flags & leafPageFlag) {
    return "leaf";
  } else if (flags & metaPageFlag) {
    return "meta";
  } else if (flags & freelistPageFlag) {
    return "freelist";
  }
  return "unknown";
}

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
void mergepgids(pgid_vec& dst, const pgid_vec& a, const pgid_vec& b) {
  std::merge(a.begin(), a.end(), b.begin(), b.end(), std::back_inserter(dst));
}
}  // namespace bboltpp
