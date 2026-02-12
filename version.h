#pragma once

namespace bboltpp {

// Version shows the last bbolt binary version released.
constexpr const char* kVersionString = "1.4.0-alpha.0";

// GetVersion returns the current version string
constexpr const char* GetVersion() { return kVersionString; }

}  // namespace bboltpp
