#include <gtest/gtest.h>

#include <string>

#include "../verify.h"
#include "../freelist.h"
#include "../page.h"

using namespace bboltpp;

// Regression guard for the "panic diagnostics are silently dropped" bug.
//
// page.h used to define a function-like macro `#define panic(code) { (void)code;
// abort(); }` that was in scope in every engine TU (page.h is ubiquitous). It
// intercepted every `panic(...)` call, computed the diagnostic message, cast it
// to void, and called abort() directly -- so nothing was ever written to
// stderr. The real implementation in verify.cpp -- bboltpp::panic(const
// std::string&), which prints "PANIC: <msg>" to stderr before std::abort() --
// had zero callers. The macro is now removed and every panic site resolves to
// bboltpp::panic.
//
// These death tests fail if the macro (or any shadow that discards the
// message) is reintroduced: the child would still die but stderr would be
// empty, so the "PANIC:" regex would not match.

// Direct: bboltpp::panic prints "PANIC: <msg>" to stderr and aborts. Catches
// reintroduction of a panic macro / shadow that drops the diagnostic.
TEST(PanicDeathTest, PanicPrintsPanicPrefixBeforeAborting) {
  EXPECT_DEATH(panic("boom"), "PANIC: boom");
}

// End-to-end: a real engine call site (FreeList::free on a meta page id 0)
// routes through bboltpp::panic and surfaces the diagnostic on stderr.
TEST(FreelistPanicDeathTest, FreeOfMetaPageEmitsPanicMessage) {
  ArrayFreeList freelist;
  Page p{};
  p.id = 0;
  EXPECT_DEATH(freelist.free(1, &p), "PANIC: cannot free page 0 or 1: 0");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
