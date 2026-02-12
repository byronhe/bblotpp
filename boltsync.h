#pragma once

#include "errors.h"

namespace bboltpp {

class DB;

// fdatasync flushes written data to a file descriptor.
// This is a platform-specific implementation for Unix-like systems.
Error fdatasync(DB* db);

}  // namespace bboltpp
