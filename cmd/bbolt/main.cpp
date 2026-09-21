#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <map>
#include "../../version.h"

namespace bboltpp {
namespace cmd {

class Command {
public:
  virtual ~Command() = default;
  virtual int Execute(const std::vector<std::string>& args) = 0;
};

class VersionCommand : public Command {
public:
  int Execute(const std::vector<std::string>& args) override {
    std::cout << "bbolt++ Version: " << GetVersion() << std::endl;
    std::cout << "C++ Standard: " << __cplusplus << std::endl;
    return 0;
  }
};

class HelpCommand : public Command {
public:
  int Execute(const std::vector<std::string>& args) override {
    std::cout << "bbolt++ - A modern C++ implementation of BoltDB" << std::endl;
    std::cout << std::endl;
    std::cout << "Available commands:" << std::endl;
    std::cout << "  version    - print the current version of bbolt++" << std::endl;
    std::cout << "  help       - show this help information" << std::endl;
    return 0;
  }
};

class CommandRunner {
public:
  CommandRunner() {
    commands_["version"] = std::make_unique<VersionCommand>();
    commands_["help"] = std::make_unique<HelpCommand>();
  }
  
  int Run(const std::vector<std::string>& args) {
    if (args.empty()) {
      std::cout << "bbolt++ - A modern C++ implementation of BoltDB" << std::endl;
      std::cout << "Use 'bbolt++ help' for more information." << std::endl;
      return 0;
    }
    
    std::string command_name = args[0];
    std::vector<std::string> command_args(args.begin() + 1, args.end());
    
    auto it = commands_.find(command_name);
    if (it == commands_.end()) {
      std::cerr << "Unknown command: " << command_name << std::endl;
      std::cerr << "Use 'bbolt++ help' for available commands." << std::endl;
      return 1;
    }
    
    return it->second->Execute(command_args);
  }

private:
  std::map<std::string, std::unique_ptr<Command>> commands_;
};

}  // namespace cmd
}  // namespace bboltpp

int main(int argc, char* argv[]) {
  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    args.emplace_back(argv[i]);
  }
  
  bboltpp::cmd::CommandRunner runner;
  return runner.Run(args);
}
