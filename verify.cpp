#include "verify.h"
#include <cstdlib>
#include <iostream>
#include <sstream>

namespace bboltpp {

std::string GetEnvVerify() {
  const char* env = std::getenv(kEnvVerify);
  if (env == nullptr) {
    return "";
  }

  std::string result = env;
  // Convert to lowercase
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}

bool IsVerificationEnabled(VerificationType verification) {
  std::string env = GetEnvVerify();

  switch (verification) {
    case VerificationType::kAll:
      return env == "all";
    case VerificationType::kAssert:
      return env == "all" || env == "assert";
    default:
      return false;
  }
}

std::function<void()> EnableVerifications(VerificationType verification) {
  std::string previousEnv = GetEnvVerify();

  std::string value;
  switch (verification) {
    case VerificationType::kAll:
      value = "all";
      break;
    case VerificationType::kAssert:
      value = "assert";
      break;
  }

  setenv(kEnvVerify, value.c_str(), 1);

  return [previousEnv]() {
    if (previousEnv.empty()) {
      unsetenv(kEnvVerify);
    } else {
      setenv(kEnvVerify, previousEnv.c_str(), 1);
    }
  };
}

std::function<void()> EnableAllVerifications() { return EnableVerifications(VerificationType::kAll); }

std::function<void()> DisableVerifications() {
  std::string previousEnv = GetEnvVerify();
  unsetenv(kEnvVerify);

  return [previousEnv]() {
    if (previousEnv.empty()) {
      unsetenv(kEnvVerify);
    } else {
      setenv(kEnvVerify, previousEnv.c_str(), 1);
    }
  };
}

void Verify(std::function<void()> f) {
  if (IsVerificationEnabled(VerificationType::kAssert)) {
    f();
  }
}

void Assert(bool condition, const std::string& msg) {
  if (!condition) {
    panic("assertion failed: " + msg);
  }
}

void panic(const std::string& message) {
  std::cerr << "PANIC: " << message << std::endl;
  std::abort();
}

}  // namespace bboltpp
