#include "node.h"
#include <algorithm>
#include <cstdint>
#include <memory>
#include <string_view>
#include "absl/strings/str_format.h"
#include "bucket.h"
#include "db.h"
#include "errors.h"
#include "page.h"
#include "unsafe.h"

namespace bboltpp {

// root returns the top-level node this node is attached to.
std::shared_ptr<Node> Node::root() {
  auto p = parent.lock();
  if (p == nullptr) {
    return shared_from_this();
  }
  return p->root();
}

// minKeys returns the minimum number of inodes this node should have.
int Node::minKeys() {
  if (isLeaf) {
    return 1;
  }
  return 2;
}

// size returns the size of the node after serialization.
int Node::size() {
  auto sz = pageHeaderSize;
  auto elsz = pageElementSize();
  for (auto &item : inodes) {
    sz += elsz + item.key.size() + item.value.size();
  }
  return sz;
}

// sizeLessThan returns true if the node is less than a given size.
// This is an optimization to avoid calculating a large node when we only need
// to know if it fits inside a certain page size.
bool Node::sizeLessThan(int v) {
  auto sz = pageHeaderSize;
  auto elsz = pageElementSize();
  for (auto &item : inodes) {
    sz += elsz + item.key.size() + item.value.size();
    if (sz >= static_cast<size_t>(v)) {
      return false;
    }
  }
  return true;
}

// pageElementSize returns the size of each page element based on the type of node.
int Node::pageElementSize() {
  if (isLeaf) {
    return leafPageElementSize;
  }
  return branchPageElementSize;
}

// childAt returns the child node at a given index.
std::shared_ptr<Node> Node::childAt(int index) {
  if (isLeaf) {
    panic(absl::StrFormat("invalid childAt(%d) on a leaf node", index));
  }
  auto bucket_ptr = bucket.lock();
  if (!bucket_ptr) return nullptr;
  return bucket_ptr->node(inodes[index].pgid, shared_from_this());
}

// childIndex returns the index of a given child node.
int Node::childIndex(std::shared_ptr<Node> child) {
  auto it = std::lower_bound(inodes.begin(), inodes.end(), child->key,
                             [&](const auto &in, const auto &key) { return in.key < key; });
  return it - inodes.begin();
}

// numChildren returns the number of children.
int Node::numChildren() { return inodes.size(); }

// nextSibling returns the next node with the same parent.
std::shared_ptr<Node> Node::nextSibling() {
  if (parent.lock() == nullptr) {
    return nullptr;
  }
  auto parent_ptr = parent.lock();
  if (!parent_ptr) return nullptr;

  auto index = parent_ptr->childIndex(shared_from_this());
  if (index >= parent_ptr->numChildren() - 1) {
    return nullptr;
  }
  return parent_ptr->childAt(index + 1);
}

// prevSibling returns the previous node with the same parent.
std::shared_ptr<Node> Node::prevSibling() {
  if (parent.lock() == nullptr) {
    return nullptr;
  }
  auto parent_ptr = parent.lock();
  if (!parent_ptr) return nullptr;

  auto index = parent_ptr->childIndex(shared_from_this());
  if (index == 0) {
    return nullptr;
  }
  return parent_ptr->childAt(index - 1);
}

// put inserts a key/value.
void Node::put(std::string_view oldKey, std::string_view newKey, std::string_view value, pgid_t pgid, uint32_t flags) {
  auto bucket_ptr = bucket.lock();
  if (!bucket_ptr) return;
  auto tx_ptr = bucket_ptr->tx.lock();
  if (!tx_ptr) return;

  if (pgid >= tx_ptr->meta->pgid) {
    panic(absl::StrFormat("pgid (%lu) above high water mark (%lu)", pgid, tx_ptr->meta->pgid));
  } else if (oldKey.size() <= 0) {
    panic("put: zero-length old key");
  } else if (newKey.size() <= 0) {
    panic("put: zero-length new key");
  }

  // Find insertion index using binary search
  auto index = std::lower_bound(inodes.begin(), inodes.end(), oldKey,
                                [&](const auto &in, const auto &key) { return in.key < key; }) -
               inodes.begin();

  // Add capacity and shift nodes if we don't have an exact match and need to insert.
  bool const exact = (static_cast<size_t>(index) < inodes.size()) && (inodes[index].key == oldKey);
  if (!exact) {
    inodes.insert(inodes.begin() + index, INode{});
  }

  auto &inode = inodes[index];
  inode.flags = flags;
  inode.key = std::string(newKey);
  inode.value = std::string(value);
  inode.pgid = pgid;
  _assert(inode.key.size() > 0, "put: zero-length inode key");
}

// del removes a key from the node.
void Node::del(std::string_view key) {
  // Find index of key.
  auto it = std::lower_bound(inodes.begin(), inodes.end(), key,
                             [&](const auto &in, const auto &key) { return in.key < key; });
  // auto index = it - inodes.begin();

  // Exit if the key isn't found.
  if ((it == inodes.end()) || (it->key != key)) {
    return;
  }

  // Delete inode from the node.
  inodes.erase(it);

  // Mark the node as needing rebalancing.
  unbalanced = true;
}

// read initializes the node from a page.
void Node::read(Page *p) {
  pgid = p->id;
  isLeaf = ((p->flags & leafPageFlag) != 0);
  inodes.assign(p->count, {});

  for (auto i = 0; i < p->count; i++) {
    auto &inode = inodes[i];
    if (isLeaf) {
      auto elem = p->leafPageElement(i);
      inode.flags = elem->flags;
      inode.key = std::string(elem->key());
      inode.value = std::string(elem->value());
    } else {
      auto elem = p->branchPageElement(i);
      inode.pgid = elem->pgid;
      inode.key = std::string(elem->key());
    }
    _assert(inode.key.size() > 0, "read: zero-length inode key");
  }

  // Save first key so we can find the node in the parent when we spill.
  if (inodes.size() > 0) {
    key = inodes.front().key;
    _assert(key.size() > 0, "read: zero-length node key");
  } else {
    key.clear();
  }
}

// write writes the items onto one or more pages.
void Node::write(Page *p) {
  // Initialize page.
  if (isLeaf) {
    p->flags |= leafPageFlag;
  } else {
    p->flags |= branchPageFlag;
  }

  if (inodes.size() >= 0xFFFF) {
    panic(absl::StrFormat("inode overflow: %zu (pgid=%lu)", inodes.size(), p->id));
  }
  p->count = (inodes.size());

  // Stop here if there are no items to write.
  if (p->count == 0) {
    return;
  }

  // Loop over each item and write it to the page.
  // off tracks the offset into p of the start of the next data.
  auto off = sizeof(*p) + pageElementSize() * inodes.size();
  for (int i = 0; i < inodes.size(); ++i) {
    auto &item = inodes[i];
    _assert(item.key.size() > 0, "write: zero-length inode key");

    // Create a slice to write into of needed size and advance
    // byte pointer for next iteration.
    auto sz = item.key.size() + item.value.size();
    auto b = unsafeByteSlice(p, off, 0, sz);
    off += sz;

    // Write the page element.
    if (isLeaf) {
      auto *elem = p->leafPageElement(i);
      elem->pos = reinterpret_cast<const char *>(&b[0]) - reinterpret_cast<const char *>(elem);
      elem->flags = item.flags;
      elem->ksize = item.key.size();
      elem->vsize = item.value.size();
    } else {
      auto *elem = p->branchPageElement(i);
      elem->pos = reinterpret_cast<const char *>(&b[0]) - reinterpret_cast<const char *>(elem);
      elem->ksize = item.key.size();
      elem->pgid = item.pgid;
      _assert(elem->pgid != p->id, "write: circular dependency occurred");
    }

    // Write data for the element to the end of the page.
    ::memcpy(const_cast<char *>(b.data()), item.key.data(), item.key.size());
    ::memcpy(const_cast<char *>(b.data()) + item.key.size(), item.value.data(), item.value.size());
  }

  // DEBUG ONLY: n.dump()
}

// split breaks up a node into multiple smaller nodes, if appropriate.
// This should only be called from the spill() function.
std::vector<std::shared_ptr<Node>> Node::split(int pageSize) {
  std::vector<std::shared_ptr<Node>> nodes;

  auto node = this;
  for (;;) {
    // Split node into two.
    auto [a, b] = node->splitTwo(pageSize);
    nodes.push_back(a);

    // If we can't split then exit the loop.
    if (b == nullptr) {
      break;
    }

    // Set node to b so it gets split on the next iteration.
    node = b.get();
  }

  return nodes;
}

// splitTwo breaks up a node into two smaller nodes, if appropriate.
// This should only be called from the split() function.
std::tuple<std::shared_ptr<struct Node>, std::shared_ptr<struct Node>> Node::splitTwo(int pageSize) {
  // Ignore the split if the page doesn't have at least enough nodes for
  // two pages or if the nodes can fit in a single page.
  if (inodes.size() <= (minKeysPerPage * 2) || sizeLessThan(pageSize)) {
    return {shared_from_this(), nullptr};
  }

  // Determine the threshold before starting a new node.
  auto bucket_ptr = bucket.lock();
  if (!bucket_ptr) return {nullptr, nullptr};
  auto fillPercent = bucket_ptr->FillPercent;
  if (fillPercent < minFillPercent) {
    fillPercent = minFillPercent;
  } else if (fillPercent > maxFillPercent) {
    fillPercent = maxFillPercent;
  }
  auto threshold = int(double(pageSize) * fillPercent);

  // Determine split position and sizes of the two pages.
  auto [split_index, _] = splitIndex(threshold);

  // Split node into two separate nodes.
  // If there's no parent then we'll need to create one.
  if (parent.lock() == nullptr) {
    auto new_parent = std::make_shared<Node>();
    new_parent->bucket = bucket;
    new_parent->children.push_back(shared_from_this());
    parent = new_parent;
  }

  // Create a new node and add it to the parent.
  auto next = std::make_shared<Node>();
  next->bucket = bucket;
  next->isLeaf = isLeaf;
  next->parent = parent;
  auto parent_ptr = parent.lock();
  if (parent_ptr) {
    parent_ptr->children.push_back(next);
  }

  // Split inodes across two nodes.
  next->inodes.assign(inodes.begin() + split_index, inodes.end());  // = inodes[split_index:]
  inodes.resize(split_index);

  // Update the statistics.
  if (bucket_ptr) {
    auto tx_ptr = bucket_ptr->tx.lock();
    if (tx_ptr) {
      tx_ptr->stats.Split++;
    }
  }

  return {shared_from_this(), next};
}

// splitIndex finds the position where a page will fill a given threshold.
// It returns the index as well as the size of the first page.
// This is only be called from split().
std::tuple<int, int> Node::splitIndex(int threshold) {
  auto sz = pageHeaderSize;
  int index = 0;
  // Loop until we only have the minimum number of keys required for the second page.
  for (size_t i = 0; i < inodes.size() - minKeysPerPage; i++) {
    index = i;
    auto &inode = inodes[i];
    auto elsize = pageElementSize() + inode.key.size() + inode.value.size();

    // If we have at least the minimum number of keys and adding another
    // node would put us over the threshold then exit and return.
    if (index >= minKeysPerPage && sz + elsize > threshold) {
      break;
    }

    // Add the element size to the total size.
    sz += elsize;
  }

  return {index, sz};
}

// spill writes the nodes to dirty pages and splits nodes as it goes.
// Returns an error if dirty pages cannot be allocated.
ErrorCode Node::spill() {
  auto bucket_ptr = bucket.lock();
  if (!bucket_ptr) return ErrorCode::ErrTxClosed;
  auto tx = bucket_ptr->tx.lock();
  if (!tx) return ErrorCode::ErrTxClosed;
  if (spilled) {
    return ErrorCode::OK;
  }

  // Spill child nodes first. Child nodes can materialize sibling nodes in
  // the case of split-merge so we cannot use a range loop. We have to check
  // the children size on every loop iteration.
  std::sort(children.begin(), children.end());
  for (auto &child : children) {
    if (auto err = child->spill(); err != ErrorCode::OK) {
      return err;
    }
  }

  // We no longer need the child list because it's only used for spill tracking.
  children.clear();

  // Split nodes into appropriate sizes. The first node will always be n.
  auto db_ptr = tx->GetDB();
  if (!db_ptr) return ErrorCode::ErrTxClosed;
  auto nodes = split(db_ptr->pageSize);
  for (auto node : nodes) {
    // Add node's page to the freelist if it's not new.
    if (node->pgid > 0) {
      db_ptr->freelist->free(tx->meta->txid, tx->page(node->pgid));
      node->pgid = 0;
    }

    // Allocate contiguous space for the node.
    auto [p, err] = tx->allocate((node->size() + db_ptr->pageSize - 1) / db_ptr->pageSize);
    if (err != ErrorCode::OK) {
      return err;
    }

    // Write the node.
    if (p->id >= tx->meta->pgid) {
      panic(absl::StrFormat("pgid (%lu) above high water mark (%lu)", p->id, tx->meta->pgid));
    }
    node->pgid = p->id;
    node->write(p);  // TODO(use realloc);
    node->spilled = true;

    // Insert into parent inodes.
    if (node->parent.lock() != nullptr) {
      auto key = node->key;
      if (key.empty()) {
        key = node->inodes[0].key;
      }

      auto parent_ptr = node->parent.lock();
      if (parent_ptr) {
        parent_ptr->put(key, node->inodes[0].key, "", node->pgid, 0);
      }
      node->key = node->inodes[0].key;
      _assert(node->key.size() > 0, "spill: zero-length node key");
    }

    // Update the statistics.
    tx->stats.Spill++;
  }

  // If the root node split and created a new root then we need to spill that
  // as well. We'll clear out the children to make sure it doesn't try to respill.
  auto parent_ptr = parent.lock();
  if (parent_ptr != nullptr && parent_ptr->pgid == 0) {
    children.clear();
    return parent_ptr->spill();
  }

  return ErrorCode::OK;
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
void Node::rebalance() {
  if (!unbalanced) {
    return;
  }
  unbalanced = false;

  // Update statistics.
  auto bucket_ptr = bucket.lock();
  if (bucket_ptr) {
    auto tx_ptr = bucket_ptr->tx.lock();
    if (tx_ptr) {
      tx_ptr->stats.Rebalance++;
    }
  }

  // Ignore if node is above threshold (25%) and has enough keys.
  if (!bucket_ptr) return;
  auto tx_ptr = bucket_ptr->tx.lock();
  if (!tx_ptr) return;
  auto threshold = tx_ptr->GetDB()->pageSize / 4;
  if (size() > threshold && inodes.size() > minKeys()) {
    return;
  }

  // Root node has special handling.
  if (parent.lock() == nullptr) {
    // If root node is a branch and only has one node then collapse it.
    if (!isLeaf && inodes.size() == 1) {
      // Move root's child up.
      auto child = bucket_ptr->node(inodes[0].pgid, shared_from_this());
      isLeaf = child->isLeaf;
      inodes = child->inodes;
      children = child->children;

      // Reparent all child nodes being moved.
      for (auto inode : inodes) {
        if (auto it = bucket_ptr->nodes.find(inode.pgid); (it != bucket_ptr->nodes.end())) {
          it->second->parent = shared_from_this();
        }
      }

      // Remove old child.
      child->parent.reset();
      bucket_ptr->nodes.erase(child->pgid);
      child->free();
    }

    return;
  }

  // If node has no keys then just remove it.
  if (numChildren() == 0) {
    auto parent_ptr = parent.lock();
    if (parent_ptr) {
      parent_ptr->del(key);
      parent_ptr->removeChild(shared_from_this());
    }
    bucket_ptr->nodes.erase(pgid);
    this->free();
    if (parent_ptr) {
      parent_ptr->rebalance();
    }
    return;
  }

  auto parent_ptr = parent.lock();
  if (!parent_ptr) return;
  _assert(parent_ptr->numChildren() > 1, "parent must have at least 2 children");

  // Destination node is right sibling if idx == 0, otherwise left sibling.
  std::shared_ptr<Node> target = nullptr;
  auto useNextSibling = (parent_ptr->childIndex(shared_from_this()) == 0);
  if (useNextSibling) {
    target = nextSibling();
  } else {
    target = prevSibling();
  }

  // If both this node and the target node are too small then merge them.
  if (useNextSibling) {
    // Reparent all child nodes being moved.
    for (auto &inode : target->inodes) {
      if (auto it = bucket_ptr->nodes.find(inode.pgid); it != bucket_ptr->nodes.end()) {
        auto &child = it->second;
        auto child_parent = child->parent.lock();
        if (child_parent) {
          child_parent->removeChild(child);
        }
        child->parent = shared_from_this();
        children.emplace_back(child);
      }
    }

    // Copy over inodes from target and remove target.
    inodes.insert(inodes.end(), target->inodes.begin(), target->inodes.end());
    parent_ptr->del(target->key);
    parent_ptr->removeChild(target);
    bucket_ptr->nodes.erase(target->pgid);
    target->free();
  } else {
    // Reparent all child nodes being moved.
    for (auto &inode : inodes) {
      if (auto it = bucket_ptr->nodes.find(inode.pgid); it != bucket_ptr->nodes.end()) {
        auto &child = it->second;
        auto child_parent = child->parent.lock();
        if (child_parent) {
          child_parent->removeChild(child);
        }
        child->parent = target;
        target->children.emplace_back(child);
      }
    }

    // Copy over inodes to target and remove node.
    target->inodes.insert(target->inodes.end(), inodes.begin(), inodes.end());
    parent_ptr->del(key);
    parent_ptr->removeChild(shared_from_this());
    bucket_ptr->nodes.erase(pgid);
    this->free();
  }

  // Either this node or the target node was deleted from the parent so rebalance it.
  parent_ptr->rebalance();
}

// removes a node from the list of in-memory children.
// This does not affect the inodes.
void Node::removeChild(std::shared_ptr<Node> target) {
  for (auto it = children.begin(); it != children.end(); ++it) {
    if (*it == target) {
      children.erase(it);
      return;
    }
  }
}

// dereference causes the node to copy all its inode key/value references to heap memory.
// This is required when the mmap is reallocated so inodes are not pointing to stale data.
void Node::dereference() {
  if (!key.empty()) {
    auto tmp_key = this->key;
    this->key.assign(tmp_key.begin(), tmp_key.end());
    _assert(pgid == 0 || key.size() > 0, "dereference: zero-length node key on existing node");
  }

  for (auto &inode : inodes) {
    // No need to copy string to itself
    _assert(inode.key.size() > 0, "dereference: zero-length inode key");

    auto tmp_value = inode.value;
    inode.value = tmp_value;
  }

  // Recursively dereference children.
  for (auto &child : children) {
    child->dereference();
  }

  // Update statistics.
  auto bucket_ptr = bucket.lock();
  if (bucket_ptr) {
    auto tx_ptr = bucket_ptr->tx.lock();
    if (tx_ptr) {
      tx_ptr->stats.NodeDeref++;
    }
  }
}

// free adds the node's underlying page to the freelist.
void Node::free() {
  if (pgid != 0) {
    auto bucket_ptr = bucket.lock();
    if (bucket_ptr) {
      auto tx_ptr = bucket_ptr->tx.lock();
      if (tx_ptr) {
        tx_ptr->GetDB()->freelist->free(tx_ptr->meta->txid, tx_ptr->page(pgid));
      }
    }
    pgid = 0;
  }
}

// dump writes the contents of the node to STDERR for debugging purposes.
/*
func (n *node) dump() {
        // Write node header.
        var typ = "branch"
        if n.isLeaf {
                typ = "leaf"
        }
        warnf("[NODE %d {type=%s count=%d}]", n.pgid, typ, inodes.size())

        // Write out abbreviated version of each item.
        for _, item := range n.inodes {
                if n.isLeaf {
                        if item.flags&bucketLeafFlag != 0 {
                                bucket := (*bucket)(unsafe.Pointer(&item.value[0]))
                                warnf("+L %08x -> (bucket root=%d)", trunc(item.key, 4), bucket.root)
                        } else {
                                warnf("+L %08x -> %08x", trunc(item.key, 4), trunc(item.value, 4))
                        }
                } else {
                        warnf("+B %08x -> pgid=%d", trunc(item.key, 4), item.pgid)
                }
        }
        warn("")
}
*/

}  // namespace bboltpp
