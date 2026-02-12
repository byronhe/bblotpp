#include "utils.h"
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <random>

namespace bboltpp {

Error CopyFile(std::string_view srcPath, std::string_view dstPath) {
  // Ensure source file exists
  if (!std::filesystem::exists(srcPath)) {
    return Error{"source file not found: " + std::string(srcPath)};
  }

  // Ensure output file doesn't exist
  if (std::filesystem::exists(dstPath)) {
    return Error{"output file already exists: " + std::string(dstPath)};
  }

  try {
    std::filesystem::copy_file(srcPath, dstPath);
    return Error{};
  } catch (const std::filesystem::filesystem_error& e) {
    return Error{"failed to copy file: " + std::string(e.what())};
  }
}

bool FileExists(std::string_view path) { return std::filesystem::exists(path); }

std::tuple<size_t, Error> GetFileSize(std::string_view path) {
  try {
    auto size = std::filesystem::file_size(path);
    return {size, Error{}};
  } catch (const std::filesystem::filesystem_error& e) {
    return {0, Error{"failed to get file size: " + std::string(e.what())}};
  }
}

std::tuple<std::string, Error> CreateTempFile(std::string_view prefix) {
  try {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);

    std::string filename = std::string(prefix);
    for (int i = 0; i < 8; ++i) {
      filename += "0123456789abcdef"[dis(gen)];
    }
    filename += ".tmp";

    auto tempPath = std::filesystem::temp_directory_path() / filename;

    // Create empty file
    std::ofstream file(tempPath);
    if (!file) {
      return {"", Error{"failed to create temporary file"}};
    }

    return {tempPath.string(), Error{}};
  } catch (const std::exception& e) {
    return {"", Error{"failed to create temporary file: " + std::string(e.what())}};
  }
}

Error RemoveFile(std::string_view path) {
  try {
    std::filesystem::remove(path);
    return Error{};
  } catch (const std::filesystem::filesystem_error& e) {
    return Error{"failed to remove file: " + std::string(e.what())};
  }
}

Error RenameFile(std::string_view oldPath, std::string_view newPath) {
  try {
    std::filesystem::rename(oldPath, newPath);
    return Error{};
  } catch (const std::filesystem::filesystem_error& e) {
    return Error{"failed to rename file: " + std::string(e.what())};
  }
}

Error EnsureDir(std::string_view path) {
  try {
    std::filesystem::create_directories(path);
    return Error{};
  } catch (const std::filesystem::filesystem_error& e) {
    return Error{"failed to create directory: " + std::string(e.what())};
  }
}

bool IsDir(std::string_view path) { return std::filesystem::is_directory(path); }

std::string JoinPath(std::string_view base, std::string_view path) {
  return (std::filesystem::path(base) / path).string();
}

std::string BaseName(std::string_view path) { return std::filesystem::path(path).filename().string(); }

std::string DirName(std::string_view path) { return std::filesystem::path(path).parent_path().string(); }

}  // namespace bboltpp
