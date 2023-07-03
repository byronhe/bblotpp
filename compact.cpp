#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>
#include <vector>
#include "absl/cleanup/cleanup.h"
#include "db.h"
#include "errors.h"

namespace bboltpp {

// walkFunc is the type of the function called for keys (buckets and "normal"
// values) discovered by Walk. keys is the list of keys to descend to the bucket
// owning the discovered key/value pair k/v.
// using walkFunc = std::function< Error(const std::vector<std::string_view> &keys, std::string_view k,
// std::string_view v , uint64_t seq ) >;

template <typename walkFunc>
Error walkBucket(Bucket *b, std::vector<std::string_view> &keypath, std::string_view k, std::string_view v,
                 uint64_t seq, walkFunc &fn) {
  // Execute callback.
  if (auto err = fn(keypath, k, v, seq); not err.OK()) {
    return err;
  }

  // If this is not a bucket then stop.
  if (!v.empty()) {
    return Error{};
  }

  // Iterate over each child key/value.
  keypath.emplace_back(k);
  return Error{b->ForEach([b, &keypath, &fn](auto k, auto v) -> ErrorCode {
    if (v.empty()) {
      auto bkt = b->FindBucketByName(k);
      return walkBucket(bkt, keypath, k, {}, bkt->Sequence(), fn).err_code_;
    }
    return walkBucket(b, keypath, k, v, b->Sequence(), fn).err_code_;
  })};
}

// walk walks recursively the bolt database db, calling walkFn for each key it finds.
template <typename walkFunc>
Error walk(DB *db, walkFunc &walkFn) {
  return db->View([&walkFn](Tx *tx) -> Error {
    return Error{tx->ForEach([&walkFn](std::string_view name, Bucket *b) -> ErrorCode {
      std::vector<std::string_view> keypath;
      return walkBucket(b, keypath, name, {}, b->Sequence(), walkFn).err_code_;
    })};
  });
}

// Compact will create a copy of the source DB and in the destination DB. This may
// reclaim space that the source database no longer has use for. txMaxSize can be
// used to limit the transactions size of this process and may trigger intermittent
// commits. A value of zero will ignore transaction sizes.
// TODO: merge with:
// https://github.com/etcd-io/etcd/blob/b7f0f52a16dbf83f18ca1d803f7892d750366a94/mvcc/backend/backend.go#L349
Error Compact(DB *dst, DB *src, int64_t txMaxSize) {
  // commit regularly, or we'll run out of memory for large datasets if using one transaction.
  int64_t size = 0;

  std::unique_ptr<Tx> tx;
  Error begin_err;
  std::tie(tx, begin_err) = dst->Begin(true);
  if (not begin_err.OK()) {
    return begin_err;
  }
  absl::Cleanup const source_closer = [&tx] { tx->Rollback(); };

  auto walk_fn = [&size, txMaxSize, &tx, dst](const std::vector<std::string_view> &keys, std::string_view k,
                                              std::string_view v, uint64_t seq) -> Error {
    // On each key/value, check if we have exceeded tx size.
    auto sz = static_cast<int64_t>(k.size() + v.size());
    if (size + sz > txMaxSize && txMaxSize != 0) {
      // Commit previous transaction.
      if (auto err = tx->Commit(); err != ErrorCode::OK) {
        return Error{err};
      }

      // Start new transaction.
      auto [tx, err] = dst->Begin(true);
      if (not err.OK()) {
        return err;
      }
      size = 0;
    }
    size += sz;

    // Create bucket on the root transaction if this is the first level.
    auto nk = keys.size();
    if (nk == 0) {
      auto [bkt, err] = tx->CreateBucket(k);
      if (err != ErrorCode::OK) {
        return Error{err};
      }
      if (auto err = bkt->SetSequence(seq); err != ErrorCode::OK) {
        return Error{err};
      }
      return Error{};
    }

    // Create buckets on subsequent levels, if necessary.
    auto *b = tx->FindBucketByName(keys[0]);
    if (nk > 1) {
      for (size_t i = 1; i < keys.size(); ++i) {
        const auto &k = keys[i];
        b = b->FindBucketByName(k);
      }
    }

    // Fill the entire page for best compaction.
    b->FillPercent = 1.0;

    // If there is no value then this is a bucket call.
    if (v.empty()) {
      auto [bkt, err] = b->CreateBucket(k);
      if (err != ErrorCode::OK) {
        return Error{err};
      }
      if (auto err = bkt->SetSequence(seq); err != ErrorCode::OK) {
        return Error{err};
      }
      return Error{};
    }

    // Otherwise treat it as a key/value pair.
    return Error{b->Put(k, v)};
  };

  auto walk_err = walk(src, walk_fn);

  if (not walk_err.OK()) {
    return walk_err;
  }

  return Error{tx->Commit()};
}

}  // namespace bboltpp
