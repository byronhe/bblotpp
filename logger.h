#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <string_view>

namespace bboltpp {

// Logger interface for bbolt logging
class Logger {
 public:
  virtual ~Logger() = default;

  virtual void Debug(const std::string& msg) = 0;
  virtual void Debugf(std::string_view format, const std::string& msg) = 0;

  virtual void Error(const std::string& msg) = 0;
  virtual void Errorf(std::string_view format, const std::string& msg) = 0;

  virtual void Info(const std::string& msg) = 0;
  virtual void Infof(std::string_view format, const std::string& msg) = 0;

  virtual void Warning(const std::string& msg) = 0;
  virtual void Warningf(std::string_view format, const std::string& msg) = 0;

  virtual void Fatal(const std::string& msg) = 0;
  virtual void Fatalf(std::string_view format, const std::string& msg) = 0;

  virtual void Panic(const std::string& msg) = 0;
  virtual void Panicf(std::string_view format, const std::string& msg) = 0;
};

// Default implementation of the Logger interface
class DefaultLogger : public Logger {
 private:
  bool debug_enabled_ = false;
  std::ostream* output_stream_ = nullptr;

  std::string FormatMessage(const std::string& level, const std::string& msg) const;

 public:
  explicit DefaultLogger(std::ostream* stream = &std::cerr) : output_stream_(stream) {}

  void EnableDebug() { debug_enabled_ = true; }
  void DisableDebug() { debug_enabled_ = false; }

  void Debug(const std::string& msg) override;
  void Debugf(std::string_view format, const std::string& msg) override;

  void Error(const std::string& msg) override;
  void Errorf(std::string_view format, const std::string& msg) override;

  void Info(const std::string& msg) override;
  void Infof(std::string_view format, const std::string& msg) override;

  void Warning(const std::string& msg) override;
  void Warningf(std::string_view format, const std::string& msg) override;

  void Fatal(const std::string& msg) override;
  void Fatalf(std::string_view format, const std::string& msg) override;

  void Panic(const std::string& msg) override;
  void Panicf(std::string_view format, const std::string& msg) override;
};

// Discard logger that outputs nothing
class DiscardLogger : public Logger {
 public:
  void Debug(const std::string& msg) override {}
  void Debugf(std::string_view format, const std::string& msg) override {}
  void Error(const std::string& msg) override {}
  void Errorf(std::string_view format, const std::string& msg) override {}
  void Info(const std::string& msg) override {}
  void Infof(std::string_view format, const std::string& msg) override {}
  void Warning(const std::string& msg) override {}
  void Warningf(std::string_view format, const std::string& msg) override {}
  void Fatal(const std::string& msg) override {}
  void Fatalf(std::string_view format, const std::string& msg) override {}
  void Panic(const std::string& msg) override {}
  void Panicf(std::string_view format, const std::string& msg) override {}
};

// Global logger instance
extern std::unique_ptr<Logger> g_logger;

// Convenience functions
void SetLogger(std::unique_ptr<Logger> logger);
Logger* GetLogger();

// Convenience macros for logging
#define BBOLT_DEBUG(msg) GetLogger()->Debug(msg)
#define BBOLT_DEBUGF(format, ...) GetLogger()->Debugf(format, std::to_string(__VA_ARGS__))
#define BBOLT_INFO(msg) GetLogger()->Info(msg)
#define BBOLT_INFOF(format, ...) GetLogger()->Infof(format, std::to_string(__VA_ARGS__))
#define BBOLT_WARNING(msg) GetLogger()->Warning(msg)
#define BBOLT_WARNINGF(format, ...) GetLogger()->Warningf(format, std::to_string(__VA_ARGS__))
#define BBOLT_ERROR(msg) GetLogger()->Error(msg)
#define BBOLT_ERRORF(format, ...) GetLogger()->Errorf(format, std::to_string(__VA_ARGS__))
#define BBOLT_FATAL(msg) GetLogger()->Fatal(msg)
#define BBOLT_FATALF(format, ...) GetLogger()->Fatalf(format, std::to_string(__VA_ARGS__))
#define BBOLT_PANIC(msg) GetLogger()->Panic(msg)
#define BBOLT_PANICF(format, ...) GetLogger()->Panicf(format, std::to_string(__VA_ARGS__))

}  // namespace bboltpp
