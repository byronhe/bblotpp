#include "logger.h"
#include <cstdlib>
#include <iostream>
#include <memory>

namespace bboltpp {

std::unique_ptr<Logger> g_logger = std::make_unique<DiscardLogger>();

void SetLogger(std::unique_ptr<Logger> logger) { g_logger = std::move(logger); }

Logger* GetLogger() { return g_logger.get(); }

std::string DefaultLogger::FormatMessage(const std::string& level, const std::string& msg) const {
  return level + ": " + msg;
}

void DefaultLogger::Debug(const std::string& msg) {
  if (debug_enabled_) {
    *output_stream_ << FormatMessage("DEBUG", msg) << std::endl;
  }
}

void DefaultLogger::Debugf(std::string_view format, const std::string& msg) {
  if (debug_enabled_) {
    // Simple format implementation - in a real implementation you'd use a proper formatter
    std::string formatted = std::string(format);
    size_t pos = formatted.find("{}");
    if (pos != std::string::npos) {
      formatted.replace(pos, 2, msg);
    }
    *output_stream_ << FormatMessage("DEBUG", formatted) << std::endl;
  }
}

void DefaultLogger::Error(const std::string& msg) { *output_stream_ << FormatMessage("ERROR", msg) << std::endl; }

void DefaultLogger::Errorf(std::string_view format, const std::string& msg) {
  std::string formatted = std::string(format);
  size_t pos = formatted.find("{}");
  if (pos != std::string::npos) {
    formatted.replace(pos, 2, msg);
  }
  *output_stream_ << FormatMessage("ERROR", formatted) << std::endl;
}

void DefaultLogger::Info(const std::string& msg) { *output_stream_ << FormatMessage("INFO", msg) << std::endl; }

void DefaultLogger::Infof(std::string_view format, const std::string& msg) {
  std::string formatted = std::string(format);
  size_t pos = formatted.find("{}");
  if (pos != std::string::npos) {
    formatted.replace(pos, 2, msg);
  }
  *output_stream_ << FormatMessage("INFO", formatted) << std::endl;
}

void DefaultLogger::Warning(const std::string& msg) { *output_stream_ << FormatMessage("WARN", msg) << std::endl; }

void DefaultLogger::Warningf(std::string_view format, const std::string& msg) {
  std::string formatted = std::string(format);
  size_t pos = formatted.find("{}");
  if (pos != std::string::npos) {
    formatted.replace(pos, 2, msg);
  }
  *output_stream_ << FormatMessage("WARN", formatted) << std::endl;
}

void DefaultLogger::Fatal(const std::string& msg) {
  *output_stream_ << FormatMessage("FATAL", msg) << std::endl;
  std::exit(1);
}

void DefaultLogger::Fatalf(std::string_view format, const std::string& msg) {
  std::string formatted = std::string(format);
  size_t pos = formatted.find("{}");
  if (pos != std::string::npos) {
    formatted.replace(pos, 2, msg);
  }
  *output_stream_ << FormatMessage("FATAL", formatted) << std::endl;
  std::exit(1);
}

void DefaultLogger::Panic(const std::string& msg) {
  *output_stream_ << FormatMessage("PANIC", msg) << std::endl;
  std::abort();
}

void DefaultLogger::Panicf(std::string_view format, const std::string& msg) {
  std::string formatted = std::string(format);
  size_t pos = formatted.find("{}");
  if (pos != std::string::npos) {
    formatted.replace(pos, 2, msg);
  }
  *output_stream_ << FormatMessage("PANIC", formatted) << std::endl;
  std::abort();
}

}  // namespace bboltpp
