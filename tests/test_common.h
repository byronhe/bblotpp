#pragma once

#include <filesystem>
#include <string>
#include <iostream>

// Helper function to clean up test database files
inline void cleanupTestFiles() {
    try {
        // Use glob pattern to find all test*.db files
        std::filesystem::path currentDir = std::filesystem::current_path();
        
        // Iterate through directory entries and remove all test*.db files
        for (const auto& entry : std::filesystem::directory_iterator(currentDir)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                // Check if filename starts with "test" and ends with ".db"
                if (filename.length() >= 7 && 
                    filename.substr(0, 4) == "test" && 
                    filename.substr(filename.length() - 3) == ".db") {
                    try {
                        std::filesystem::remove(entry.path());
                        std::cout << "Removed test file: " << filename << std::endl;
                    } catch (const std::filesystem::filesystem_error& e) {
                        std::cerr << "Warning: Could not remove " << filename << ": " << e.what() << std::endl;
                    }
                }
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Warning: Could not access directory for cleanup: " << e.what() << std::endl;
    }
}
