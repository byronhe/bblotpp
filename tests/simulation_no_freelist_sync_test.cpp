#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>
#include "../bucket.h"
#include "../db.h"
#include "../logger.h"
#include "../utils.h"
#include "test_common.h"

using namespace bboltpp;

class SimulationNoFreelistSyncTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cleanupTestFiles();
    bboltpp::SetLogger(std::make_unique<bboltpp::DefaultLogger>());
  }

  void TearDown() override { cleanupTestFiles(); }
};

// QuickDB is an in-memory database that replicates the functionality of the
// Bolt DB type except that it is entirely in-memory. It is meant for testing
// that the Bolt database is consistent.
class QuickDB {
 private:
  mutable std::shared_mutex mutex_;
  std::map<std::string, std::shared_ptr<void>> m_;

 public:
  QuickDB() = default;

  // Copy constructor
  QuickDB(const QuickDB& other) {
    std::shared_lock<std::shared_mutex> lock(other.mutex_);
    m_ = copyMap(other.m_);
  }

  // Copy assignment operator
  QuickDB& operator=(const QuickDB& other) {
    if (this != &other) {
      std::unique_lock<std::shared_mutex> lock1(mutex_, std::defer_lock);
      std::shared_lock<std::shared_mutex> lock2(other.mutex_, std::defer_lock);
      std::lock(lock1, lock2);
      m_ = copyMap(other.m_);
    }
    return *this;
  }

  // Get retrieves the value at a key path.
  std::string Get(const std::vector<std::string>& keys) {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    auto m = &m_;
    for (size_t i = 0; i < keys.size() - 1; i++) {
      auto it = m->find(keys[i]);
      if (it == m->end()) {
        return "";
      }

      auto* submap = std::static_pointer_cast<std::map<std::string, std::shared_ptr<void>>>(it->second).get();
      if (submap == nullptr) {
        return "";
      }
      m = submap;
    }

    auto it = m->find(keys.back());
    if (it == m->end()) {
      return "";
    }

    auto* value = std::static_pointer_cast<std::string>(it->second).get();
    return value ? *value : "";
  }

  // Put inserts a value into a key path.
  void Put(const std::vector<std::string>& keys, const std::string& value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    // Build buckets all the way down the key path.
    auto m = &m_;
    for (size_t i = 0; i < keys.size() - 1; i++) {
      auto it = m->find(keys[i]);
      if (it != m->end()) {
        auto* submap = std::static_pointer_cast<std::map<std::string, std::shared_ptr<void>>>(it->second).get();
        if (submap != nullptr) {
          m = submap;
          continue;
        }
      }

      // Create new submap
      auto submap = std::make_shared<std::map<std::string, std::shared_ptr<void>>>();
      (*m)[keys[i]] = submap;
      m = submap.get();
    }

    // Insert value into the last key.
    (*m)[keys.back()] = std::make_shared<std::string>(value);
  }

  // Rand returns a random key path that points to a simple value.
  std::vector<std::string> Rand() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (m_.empty()) {
      return {};
    }

    std::vector<std::string> keys;
    rand(m_, keys);
    return keys;
  }

  // Copy copies the entire database.
  QuickDB Copy() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    QuickDB copy;
    copy.m_ = copyMap(m_);
    return copy;
  }

 private:
  void rand(const std::map<std::string, std::shared_ptr<void>>& m, std::vector<std::string>& keys) {
    if (m.empty()) return;

    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, m.size() - 1);

    int index = dis(gen);
    int i = 0;
    for (const auto& [k, v] : m) {
      if (i == index) {
        keys.push_back(k);
        auto* submap = std::static_pointer_cast<std::map<std::string, std::shared_ptr<void>>>(v).get();
        if (submap != nullptr) {
          rand(*submap, keys);
        }
        return;
      }
      i++;
    }
  }

  std::map<std::string, std::shared_ptr<void>> copyMap(const std::map<std::string, std::shared_ptr<void>>& m) {
    std::map<std::string, std::shared_ptr<void>> clone;
    for (const auto& [k, v] : m) {
      auto* submap = std::static_pointer_cast<std::map<std::string, std::shared_ptr<void>>>(v).get();
      if (submap != nullptr) {
        clone[k] = std::make_shared<std::map<std::string, std::shared_ptr<void>>>(copyMap(*submap));
      } else {
        clone[k] = v;
      }
    }
    return clone;
  }
};

// Random key generation
std::string randKey() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<> sizeDis(1, 1024);
  std::uniform_int_distribution<> charDis(0, 255);

  int n = sizeDis(gen);
  std::string key(n, '\0');
  for (int i = 0; i < n; i++) {
    key[i] = static_cast<char>(charDis(gen));
  }
  return key;
}

std::vector<std::string> randKeys() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<> countDis(2, 3);  // 2-3 keys

  int count = countDis(gen);
  std::vector<std::string> keys;
  for (int i = 0; i < count; i++) {
    keys.push_back(randKey());
  }
  return keys;
}

std::string randValue() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<> sizeDis(0, 8192);
  std::uniform_int_distribution<> charDis(0, 255);

  int n = sizeDis(gen);
  std::string value(n, '\0');
  for (int i = 0; i < n; i++) {
    value[i] = static_cast<char>(charDis(gen));
  }
  return value;
}

// Handler function type
using SimulateHandler = std::function<void(std::shared_ptr<bboltpp::Tx>&, QuickDB&)>;

// Retrieves a key from the database and verifies that it is what is expected.
void simulateGetHandler(std::shared_ptr<bboltpp::Tx>& tx, QuickDB& qdb) {
  // Randomly retrieve an existing key.
  auto keys = qdb.Rand();
  if (keys.empty()) {
    return;
  }

  // Retrieve root bucket.
  auto bucket = tx->FindBucketByName(keys[0]);
  if (bucket == nullptr) {
    std::cout << "bucket[0] expected: " << keys[0].substr(0, 4) << std::endl;
    throw std::runtime_error("bucket[0] expected");
  }

  // Drill into nested buckets.
  for (size_t i = 1; i < keys.size() - 1; i++) {
    bucket = bucket->FindBucketByName(keys[i]);
    if (bucket == nullptr) {
      std::cout << "bucket[n] expected: " << keys[i] << std::endl;
      throw std::runtime_error("bucket[n] expected");
    }
  }

  // Verify key/value on the final bucket.
  std::string expected = qdb.Get(keys);
  auto actual = bucket->Get(keys.back());
  if (!actual.has_value() || actual.value() != expected) {
    std::cout << "=== EXPECTED ===" << std::endl;
    std::cout << expected << std::endl;
    std::cout << "=== ACTUAL ===" << std::endl;
    std::cout << (actual.has_value() ? actual.value() : "<null>") << std::endl;
    std::cout << "=== END ===" << std::endl;
    throw std::runtime_error("value mismatch");
  }
}

// Inserts a key into the database.
void simulatePutHandler(std::shared_ptr<bboltpp::Tx>& tx, QuickDB& qdb) {
  auto keys = randKeys();
  auto value = randValue();

  // Retrieve root bucket.
  auto bucket = tx->FindBucketByName(keys[0]);
  if (bucket == nullptr) {
    auto [newBucket, bucketErr] = tx->CreateBucket(keys[0]);
    if (bucketErr != bboltpp::ErrorCode::OK) {
      throw std::runtime_error("create bucket: " + std::to_string(static_cast<int>(bucketErr)));
    }
    bucket = newBucket;
  }

  // Create nested buckets, if necessary.
  for (size_t i = 1; i < keys.size() - 1; i++) {
    auto child = bucket->FindBucketByName(keys[i]);
    if (child != nullptr) {
      bucket = child;
    } else {
      auto [newBucket, bucketErr] = bucket->CreateBucket(keys[i]);
      if (bucketErr != bboltpp::ErrorCode::OK) {
        throw std::runtime_error("create bucket: " + std::to_string(static_cast<int>(bucketErr)));
      }
      bucket = newBucket;
    }
  }

  // Insert into database.
  auto putErr = bucket->Put(keys.back(), value);
  if (putErr != bboltpp::ErrorCode::OK) {
    throw std::runtime_error("put: " + std::to_string(static_cast<int>(putErr)));
  }

  // Insert into in-memory database.
  qdb.Put(keys, value);
}

// Randomly generate operations on a given database with multiple clients to ensure consistency and thread safety.
void testSimulateNoFreeListSync(int round, int threadCount, int parallelism) {
  // A list of operations that readers and writers can perform.
  std::vector<SimulateHandler> readerHandlers = {simulateGetHandler};
  std::vector<SimulateHandler> writerHandlers = {simulateGetHandler, simulatePutHandler};

  std::map<int, QuickDB> versions;
  versions[1] = QuickDB();

  bboltpp::Options options;
  options.NoFreelistSync = true;  // Key difference from regular simulation

  auto [db, err] = bboltpp::DB::Open("test_simulation_no_freelist_sync.db", 0644, &options);
  EXPECT_TRUE(err.OK());
  EXPECT_NE(nullptr, db);

  std::mutex mutex;

  for (int n = 0; n < round; n++) {
    // Run n threads in parallel, each with their own operation.
    std::atomic<int> availableThreads{parallelism};
    std::atomic<int64_t> opCount{0};
    std::atomic<int64_t> igCount{0};

    std::vector<std::exception_ptr> errors;
    std::mutex errorMutex;

    std::vector<std::thread> threads;

    for (int i = 0; i < threadCount; i++) {
      // Wait for available thread slot
      while (availableThreads.load() <= 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      availableThreads--;

      // 20% writers
      static std::random_device rd;
      static std::mt19937 gen(rd());
      std::uniform_int_distribution<> dis(0, 99);
      bool writable = (dis(gen) < 20);

      // Choose an operation to execute.
      SimulateHandler handler;
      if (writable) {
        std::uniform_int_distribution<> handlerDis(0, writerHandlers.size() - 1);
        handler = writerHandlers[handlerDis(gen)];
      } else {
        std::uniform_int_distribution<> handlerDis(0, readerHandlers.size() - 1);
        handler = readerHandlers[handlerDis(gen)];
      }

      // Execute a thread for the given operation.
      threads.emplace_back([&, writable, handler]() {
        try {
          opCount++;

          // Start transaction.
          auto [tx, txErr] = db->Begin(writable);
          if (!txErr.OK()) {
            throw std::runtime_error("error tx begin: " + std::to_string(static_cast<int>(txErr.err_code_)));
          }

          // Obtain current state of the dataset.
          QuickDB qdb;
          {
            std::lock_guard<std::mutex> lock(mutex);
            if (writable) {
              qdb = versions[tx->ID() - 1].Copy();
            } else {
              qdb = versions[tx->ID()];
            }
          }

          // Make sure we commit/rollback the tx at the end and update the state.
          if (writable) {
            try {
              // Ignore operation if we don't have data yet.
              if (qdb.Rand().empty()) {
                igCount++;
                tx->Rollback();
                return;
              }

              // Execute handler.
              handler(tx, qdb);

              // Update state
              {
                std::lock_guard<std::mutex> lock(mutex);
                versions[tx->ID()] = qdb;
              }

              auto commitErr = tx->Commit();
              if (commitErr != bboltpp::ErrorCode::OK) {
                throw std::runtime_error("commit error: " + std::to_string(static_cast<int>(commitErr)));
              }
            } catch (...) {
              tx->Rollback();
              throw;
            }
          } else {
            // Ignore operation if we don't have data yet.
            if (qdb.Rand().empty()) {
              igCount++;
              tx->Rollback();
              return;
            }

            // Execute handler.
            handler(tx, qdb);

            tx->Rollback();
          }
        } catch (...) {
          std::lock_guard<std::mutex> lock(errorMutex);
          errors.push_back(std::current_exception());
        }

        availableThreads++;
      });
    }

    // Wait until all threads are done.
    for (auto& t : threads) {
      t.join();
    }

    std::cout << "transactions:" << opCount.load() << " ignored:" << igCount.load() << std::endl;

    // Check for errors
    if (!errors.empty()) {
      std::rethrow_exception(errors[0]);
    }

    db->Close();
    bboltpp::RemoveFile("test_simulation_no_freelist_sync.db");

    // Reopen for next round
    auto [newDb, newErr] = bboltpp::DB::Open("test_simulation_no_freelist_sync.db", 0644, &options);
    EXPECT_TRUE(newErr.OK());
    db = std::move(newDb);
  }

  db->Close();
  bboltpp::RemoveFile("test_simulation_no_freelist_sync.db");
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_1op_1p) {
  std::cout << "Running TestSimulateNoFreeListSync_1op_1p..." << std::endl;
  testSimulateNoFreeListSync(8, 1, 1);
  std::cout << "TestSimulateNoFreeListSync_1op_1p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_10op_1p) {
  std::cout << "Running TestSimulateNoFreeListSync_10op_1p..." << std::endl;
  testSimulateNoFreeListSync(8, 10, 1);
  std::cout << "TestSimulateNoFreeListSync_10op_1p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_100op_1p) {
  std::cout << "Running TestSimulateNoFreeListSync_100op_1p..." << std::endl;
  testSimulateNoFreeListSync(8, 100, 1);
  std::cout << "TestSimulateNoFreeListSync_100op_1p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_1000op_1p) {
  std::cout << "Running TestSimulateNoFreeListSync_1000op_1p..." << std::endl;
  testSimulateNoFreeListSync(8, 1000, 1);
  std::cout << "TestSimulateNoFreeListSync_1000op_1p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_10000op_1p) {
  std::cout << "Running TestSimulateNoFreeListSync_10000op_1p..." << std::endl;
  testSimulateNoFreeListSync(8, 10000, 1);
  std::cout << "TestSimulateNoFreeListSync_10000op_1p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_10op_10p) {
  std::cout << "Running TestSimulateNoFreeListSync_10op_10p..." << std::endl;
  testSimulateNoFreeListSync(8, 10, 10);
  std::cout << "TestSimulateNoFreeListSync_10op_10p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_100op_10p) {
  std::cout << "Running TestSimulateNoFreeListSync_100op_10p..." << std::endl;
  testSimulateNoFreeListSync(8, 100, 10);
  std::cout << "TestSimulateNoFreeListSync_100op_10p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_1000op_10p) {
  std::cout << "Running TestSimulateNoFreeListSync_1000op_10p..." << std::endl;
  testSimulateNoFreeListSync(8, 1000, 10);
  std::cout << "TestSimulateNoFreeListSync_1000op_10p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_10000op_10p) {
  std::cout << "Running TestSimulateNoFreeListSync_10000op_10p..." << std::endl;
  testSimulateNoFreeListSync(8, 10000, 10);
  std::cout << "TestSimulateNoFreeListSync_10000op_10p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_100op_100p) {
  std::cout << "Running TestSimulateNoFreeListSync_100op_100p..." << std::endl;
  testSimulateNoFreeListSync(8, 100, 100);
  std::cout << "TestSimulateNoFreeListSync_100op_100p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_1000op_100p) {
  std::cout << "Running TestSimulateNoFreeListSync_1000op_100p..." << std::endl;
  testSimulateNoFreeListSync(8, 1000, 100);
  std::cout << "TestSimulateNoFreeListSync_1000op_100p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_10000op_100p) {
  std::cout << "Running TestSimulateNoFreeListSync_10000op_100p..." << std::endl;
  testSimulateNoFreeListSync(8, 10000, 100);
  std::cout << "TestSimulateNoFreeListSync_10000op_100p passed" << std::endl;
}

TEST_F(SimulationNoFreelistSyncTest, TestSimulateNoFreeListSync_10000op_1000p) {
  std::cout << "Running TestSimulateNoFreeListSync_10000op_1000p..." << std::endl;
  testSimulateNoFreeListSync(8, 10000, 1000);
  std::cout << "TestSimulateNoFreeListSync_10000op_1000p passed" << std::endl;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
