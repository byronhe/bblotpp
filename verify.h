#pragma once

#include <cstdlib>
#include <functional>
#include <string>

namespace bboltpp {

constexpr const char* kEnvVerify = "BBOLT_VERIFY";

enum class VerificationType { kAll, kAssert };

// Get the verification setting from environment
std::string GetEnvVerify();

// Check if verification is enabled for the given type
bool IsVerificationEnabled(VerificationType verification);

// Enable verifications and return a cleanup function
std::function<void()> EnableVerifications(VerificationType verification);

// Enable all verifications and return a cleanup function
std::function<void()> EnableAllVerifications();

// Disable verifications and return a cleanup function
std::function<void()> DisableVerifications();

// Panic function that can be overridden for testing
void panic(const std::string& message);

// Verify performs verification if the assertions are enabled.
// In the default setup running in tests and skipped in the production code.
void Verify(std::function<void()> f);

// Assert will panic with a given formatted message if the given condition is false.
void Assert(bool condition, const std::string& msg);

// Assert with formatted message
template <typename... Args>
void Assert(bool condition, const std::string& format, Args&&... args) {
  if (!condition) {
    // Simple formatting - in a real implementation you'd use a proper formatter
    std::string message = "assertion failed: " + format;
    panic(message);
  }
}

}  // namespace bboltpp
