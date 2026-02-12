#pragma once

#include <string>
#include <string_view>
#include "errors.h"

namespace bboltpp {

// CopyFile copies a file from srcPath to dstPath
Error CopyFile(std::string_view srcPath, std::string_view dstPath);

// FileExists checks if a file exists
bool FileExists(std::string_view path);

// GetFileSize returns the size of a file
std::tuple<size_t, Error> GetFileSize(std::string_view path);

// CreateTempFile creates a temporary file and returns its path
std::tuple<std::string, Error> CreateTempFile(std::string_view prefix = "bbolt_");

// RemoveFile removes a file
Error RemoveFile(std::string_view path);

// RenameFile renames a file from oldPath to newPath
Error RenameFile(std::string_view oldPath, std::string_view newPath);

// EnsureDir ensures that the directory exists
Error EnsureDir(std::string_view path);

// IsDir checks if the path is a directory
bool IsDir(std::string_view path);

// JoinPath joins path components
std::string JoinPath(std::string_view base, std::string_view path);

// BaseName returns the last element of path
std::string BaseName(std::string_view path);

// DirName returns all but the last element of path
std::string DirName(std::string_view path);

}  // namespace bboltpp
