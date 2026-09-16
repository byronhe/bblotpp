// Regression test for the `Error(std::string)` constructor defect.
//
// Context (see bug report):
//   `db.cpp::DB::grow` and `DB::allocate` build `Error` objects from formatted
//   strings (e.g. `"file resize error: ..."`, `"mmap allocate error: ..."`).
//   `Tx::commitFreelist` and `Tx::allocate` reduce those `Error`s to `ErrorCode`
//   via `err.err_code_`. When `Error(std::string)` left `err_code_ == ErrorCode::OK`
//   (its default), a *failing* string-`Error` was silently down-converted to
//   `ErrorCode::OK`, so `Tx::Commit` treated a rolled-back commit as success
//   and proceeded into `Tx::write()`, which dereferenced the now-empty
//   `this->db` (cleared by `rollback()`) and crashed with `SIGSEGV` -- or, when
//   the crash was avoided, returned `OK` (silent commit failure / data loss).
//
// The fix sets `err_code_ = ErrorCode::ErrInvalid` in the string constructor so
// that any string-`Error` carries a non-OK code and cannot be down-converted to
// success.
//
// This file contains:
//   1. Deterministic unit tests for the `Error` class: the constructor defect
//      (a string-`Error` must carry a non-OK `err_code_`) and the down-conversion
//      invariant that `Tx::commitFreelist`/`Tx::allocate` rely on.
//   2. An end-to-end fork-based reproduction that forces `DB::grow`'s
//      `File::Truncate` to fail via `RLIMIT_FSIZE` (see bug report "Evidence").

#include <gtest/gtest.h>

#include <errno.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>

#include "absl/strings/str_format.h"

#include "../bucket.h"
#include "../db.h"
#include "../errors.h"
#include "../logger.h"
#include "../tx.h"
#include "test_common.h"

using bboltpp::DB;
using bboltpp::Error;
using bboltpp::ErrorCode;
using bboltpp::Tx;

namespace {

// Exit statuses the forked children report back to the parent via `_exit`.
// (`_exit` is used instead of `exit` so gtest/atexit handlers do not run in the
// child and mask a crash.)
enum ChildExit : int {
  kControlCleanOK = 0,         // control: the tiny commit returned OK cleanly
  kReproPropagatedNonOK = 10,  // repro: Tx::Commit returned a non-OK code (fix)
  kSilentOK = 11,              // control/repro: Tx::Commit returned OK when it
                              //            should not have (silent data loss)
  kSetupError = 12,            // open/begin/stat/setrlimit/etc. failed
};

struct ChildOutcome {
  bool wait_ok = false;  // fork + waitpid succeeded
  bool exited = false;   // WIFEXITED
  int exit_code = -1;     // WEXITSTATUS (valid iff exited)
  int signal = -1;        // WTERMSIG (valid iff !exited && wait_ok)
  int sys_err = 0;        // errno from failed fork/waitpid
};

// Runs one tiny write transaction against a fresh database at `dbpath`.
//
// A single commit (no preceding/following rw transaction) is used deliberately:
// this port has an unrelated freelist bug that corrupts `FreeList::release`
// when a *second* read-write transaction begins, which would mask the
// grow/crash path under test here.
//
// When `cap_file_size` is set, after `Open` the child caps `RLIMIT_FSIZE` at
// `db->datasz - db->pageSize` and ignores `SIGXFSZ`. The single tiny commit
// pushes `meta->pgid` past the post-open file size, so `commitFreelist` calls
// `DB::grow`, whose `File::Truncate`(to `datasz`) exceeds the cap and fails --
// the bug's crash chain. The cap is chosen strictly below `datasz` (so the
// truncate fails) but large enough that the commit's own page `WriteAt`s (which
// live below `datasz`) succeed; with the bug that lets `Tx::Commit` fall through
// into `Tx::write`, which dereferences the now-empty `this->db` (cleared by
// `rollback()`) and crashes with `SIGSEGV`.
//
// `expect_ok_commit` selects whether the commit is expected to return OK
// (control) or non-OK (repro, with the fix).
ChildOutcome run_commit_child(const std::string &dbpath, bool cap_file_size, bool expect_ok_commit) {
  const char *role = cap_file_size ? "repro" : "control";

  ChildOutcome out{};
  pid_t pid = ::fork();
  if (pid == -1) {
    out.sys_err = errno;
    return out;
  }
  if (pid == 0) {
    // ---------------- child ----------------
    // Quiet logger so child output only carries our own diagnostics.
    bboltpp::SetLogger(std::make_unique<bboltpp::DiscardLogger>());

    // Start from a fresh file.
    std::error_code rm_ec;
    std::filesystem::remove(dbpath, rm_ec);

    // Open with default options: NoFreelistSync=false (so commitFreelist runs)
    // and NoGrowSync=false (so grow's File::Truncate runs).
    std::shared_ptr<DB> db;
    Error oerr;
    std::tie(db, oerr) = DB::Open(dbpath, 0600, nullptr);
    if (!oerr.OK() || db == nullptr) {
      std::fprintf(stderr, "[%s] open failed: code=%d msg=%s\n", role, static_cast<int>(oerr.err_code_),
                   oerr.err_msg_.c_str());
      std::fflush(stderr);
      _exit(kSetupError);
    }

    // Optionally cap RLIMIT_FSIZE just below datasz (so DB::grow's
    // Truncate(datasz) fails) and ignore SIGXFSZ so ftruncate past the cap
    // returns EFBIG instead of killing the process.
    long long capped_size = -1;
    if (cap_file_size) {
      const long long datasz = (long long)db->datasz;
      const long long pgsize = (long long)db->pageSize;
      const long long filesz = (long long)db->filesz;
      // cap must be < datasz (truncate target) and >= the largest page WriteAt
      // the tiny commit issues (those live well under datasz). datasz-pageSize
      // safely sits in that window for the single tiny commit.
      rlim_t cap = (rlim_t)(datasz - pgsize);
      struct rlimit rl { cap, cap };
      if (::setrlimit(RLIMIT_FSIZE, &rl) != 0) {
        std::fprintf(stderr, "[%s] setrlimit failed: errno=%d cap=%lld (filesz=%lld datasz=%lld pgsize=%lld)\n",
                     role, errno, (long long)cap, filesz, datasz, pgsize);
        std::fflush(stderr);
        _exit(kSetupError);
      }
      ::signal(SIGXFSZ, SIG_IGN);
      capped_size = (long long)cap;
      std::fprintf(stderr, "[%s] filesz=%lld datasz=%lld pgsize=%lld -> cap=%lld\n", role, filesz, datasz, pgsize,
                   capped_size);
      std::fflush(stderr);
    }

    // Single tiny commit: create a bucket, put one tiny key, commit. Under the
    // cap, allocate pushes meta->pgid past filesz, commitFreelist calls
    // DB::grow, and File::Truncate fails the cap.
    ErrorCode cerr = ErrorCode::OK;
    {
      std::shared_ptr<Tx> tx;
      Error berr;
      std::tie(tx, berr) = db->Begin(true);
      if (!berr.OK() || tx == nullptr) {
        std::fprintf(stderr, "[%s] begin failed: code=%d\n", role, static_cast<int>(berr.err_code_));
        std::fflush(stderr);
        _exit(kSetupError);
      }
      auto [bucket, berrc] = tx->CreateBucketIfNotExists("data");
      if (berrc != ErrorCode::OK || bucket == nullptr) {
        std::fprintf(stderr, "[%s] createbucket failed: code=%d\n", role, static_cast<int>(berrc));
        std::fflush(stderr);
        _exit(kSetupError);
      }
      if (auto perr = bucket->Put("k", "v"); perr != ErrorCode::OK) {
        std::fprintf(stderr, "[%s] put failed: code=%d\n", role, static_cast<int>(perr));
        std::fflush(stderr);
        _exit(kSetupError);
      }
      cerr = tx->Commit();
      std::fprintf(stderr, "[%s] commit code=%d\n", role, static_cast<int>(cerr));
      std::fflush(stderr);
    }

    if (expect_ok_commit) {
      _exit(cerr == ErrorCode::OK ? kControlCleanOK : kSilentOK);
    }
    _exit(cerr != ErrorCode::OK ? kReproPropagatedNonOK : kSilentOK);
  }

  // ---------------- parent ----------------
  int status = 0;
  if (::waitpid(pid, &status, 0) == -1) {
    out.sys_err = errno;
    return out;
  }
  out.wait_ok = true;
  if (WIFEXITED(status)) {
    out.exited = true;
    out.exit_code = WEXITSTATUS(status);
  } else if (WIFSIGNALED(status)) {
    out.exited = false;
    out.signal = WTERMSIG(status);
  }
  return out;
}

}  // namespace

// ---------------------------------------------------------------------------
// 1. Deterministic unit tests for the `Error` class (the constructor defect).
// ---------------------------------------------------------------------------

// The core defect: a string-constructed Error reports a failure through OK()
// (because err_msg_ is non-empty) but -- before the fix -- carried an OK
// err_code_, so down-conversion via err_code_ silently reclassified the
// failure as success. With the fix, err_code_ must be non-OK.
TEST(ErrorsString, StringErrorReportsFailureAndNonOKCode) {
  Error e{std::string("some io failure")};
  EXPECT_FALSE(e.OK());                     // failure surface (err_msg_ non-empty)
  EXPECT_NE(e.err_code_, ErrorCode::OK);   // the fix: code must be non-OK
}

// The string constructor must mirror the int constructor's contract: a
// non-success input produces a non-OK err_code_. Both failure ctors must agree.
TEST(ErrorsString, StringErrorMirrorsIntCtorNonOKCode) {
  Error si{int(EFBIG)};
  Error ss{std::string("x")};
  EXPECT_FALSE(si.OK());
  EXPECT_FALSE(ss.OK());
  ASSERT_NE(si.err_code_, ErrorCode::OK);
  EXPECT_EQ(ss.err_code_, si.err_code_);  // both ErrInvalid
}

// The down-conversion invariant Tx::commitFreelist / Tx::allocate rely on:
// an Error built from a formatted string (exactly as DB::grow / DB::allocate
// produce) must carry a non-OK err_code_, so `return err.err_code_` cannot
// reclassify the failure as success.
TEST(ErrorsString, GrowAndAllocateStringErrorsDownconvertToNonOK) {
  // DB::grow: File::Truncate returns Error{ftruncate(...)}, i.e. an int-Error;
  // grow wraps it as Error{absl::StrFormat("file resize error: %v", err)}.
  Error inner{int(-1)};  // mirrors a failed ftruncate return value
  Error grow_err{absl::StrFormat("file resize error: %v", inner)};
  EXPECT_FALSE(grow_err.OK());
  EXPECT_NE(grow_err.err_code_, ErrorCode::OK);

  // DB::allocate: Error{absl::StrFormat("mmap allocate error: %v", err)}.
  Error mmap_err{absl::StrFormat("mmap allocate error: %v", inner)};
  EXPECT_FALSE(mmap_err.OK());
  EXPECT_NE(mmap_err.err_code_, ErrorCode::OK);

  // DB::allocate's "no write transaction" path uses a bare string too.
  Error nwt{std::string("no write transaction")};
  EXPECT_FALSE(nwt.OK());
  EXPECT_NE(nwt.err_code_, ErrorCode::OK);
}

// ---------------------------------------------------------------------------
// 2. End-to-end reproduction (RLIMIT_FSIZE forces DB::grow's Truncate to fail).
// ---------------------------------------------------------------------------

class GrowSegfaultRepro : public ::testing::Test {
 protected:
  void SetUp() override {
    cleanupTestFiles();
    bboltpp::SetLogger(std::make_unique<bboltpp::DiscardLogger>());
  }
  void TearDown() override { cleanupTestFiles(); }

  static std::string controlPath() { return "test_grow_segfault_control.db"; }
  static std::string reproPath() { return "test_grow_segfault_repro.db"; }
};

TEST_F(GrowSegfaultRepro, ControlCommitsCleanlyAndReproDoesNotCrashAndReturnsNonOK) {
  // ---- control: no rlimit, the single tiny commit must succeed cleanly ----
  auto control = run_commit_child(controlPath(), /*cap_file_size=*/false, /*expect_ok_commit=*/true);
  ASSERT_TRUE(control.wait_ok) << "fork/waitpid failed for control child, errno=" << control.sys_err;
  ASSERT_TRUE(control.exited)
      << "control child was killed by signal " << control.signal
      << " (unexpected: the healthy commit path must not crash)";
  ASSERT_NE(control.exit_code, kSetupError)
      << "control child reported a setup error; cannot validate in this environment";
  EXPECT_EQ(control.exit_code, kControlCleanOK)
      << "control: the commit did not return OK cleanly (exit=" << control.exit_code
      << ") -- test setup is invalid or the healthy path regressed";

  // ---- repro: cap RLIMIT_FSIZE so DB::grow's Truncate fails ----
  auto repro = run_commit_child(reproPath(), /*cap_file_size=*/true, /*expect_ok_commit=*/false);
  ASSERT_TRUE(repro.wait_ok) << "fork/waitpid failed for repro child, errno=" << repro.sys_err;
  ASSERT_TRUE(repro.exited)
      << "REPRODUCED (crash): repro child killed by signal " << repro.signal
      << " -- Tx::Commit crashed after DB::grow failure (bug present: the "
         "string-Error from grow was down-converted to OK, so Commit proceeded "
         "into Tx::write() and dereferenced the now-empty this->db)";
  ASSERT_NE(repro.exit_code, kSetupError)
      << "repro child reported a setup error (open/stat/setrlimit failed); "
         "cannot validate the grow-failure path in this environment";
  EXPECT_EQ(repro.exit_code, kReproPropagatedNonOK)
      << "repro: Tx::Commit did NOT propagate the grow failure as non-OK (exit=" << repro.exit_code << "). "
      << (repro.exit_code == kSilentOK
              ? "This is the silent-success variant of the bug (Commit returned OK "
                "after rollback -> data loss / durability violation)."
              : "Unexpected status.");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
