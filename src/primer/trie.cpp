#include "primer/trie.h"
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stack>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  std::shared_ptr<const TrieNode> curnode = this->root_;
  uint64_t startkey = 0;
  uint64_t keylen = key.length();

  while (startkey < keylen && curnode) {
    char c = key[startkey++];
    if (curnode->children_.find(c) == curnode->children_.end()) {
      curnode = nullptr;
      break;
    }
    curnode = curnode->children_.at(c);
  }
  if (!curnode || startkey != keylen || !curnode->is_value_node_) {
    return nullptr;
  }
  const auto *valnode = dynamic_cast<const TrieNodeWithValue<T> *>(curnode.get());
  return valnode ? valnode->value_.get() : nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::shared_ptr<const TrieNode> node = this->root_;
  std::shared_ptr<T> shared_val = std::make_shared<T>(std::move(value));  // make shared value
  std::vector<std::shared_ptr<const TrieNode>> nodestack;
  uint64_t startkey = 0;
  uint64_t keylen = key.length();

  while (startkey < keylen && node) {
    char c = key[startkey++];
    nodestack.push_back(node);
    if (node->children_.find(c) == node->children_.end()) {
      node = nullptr;
      break;
    }
    node = node->children_.at(c);
  }

  // create the different node;
  std::shared_ptr<const TrieNodeWithValue<T>> new_leafnode =
      node == nullptr ? std::make_shared<const TrieNodeWithValue<T>>(shared_val)
                      : std::make_shared<const TrieNodeWithValue<T>>(node->children_, shared_val);
  std::shared_ptr<const TrieNode> child_node = new_leafnode;
  while (startkey < keylen) {
    char c = key[--keylen];
    std::map<char, std::shared_ptr<const TrieNode>> children{{c, child_node}};
    node = std::make_shared<const TrieNode>(children);
    child_node = node;
  }
  // copy the previous node;
  // construct the new tree from the bottom to top

  node = child_node;
  for (int i = nodestack.size() - 1; i >= 0; i--) {
    node = std::shared_ptr<TrieNode>(nodestack[i]->Clone());
    // get it's children
    char c = key[i];
    const_cast<TrieNode *>(node.get())->children_[c] = child_node;
    child_node = node;
  }
  // construct the trie and return
  /*
  for (size_t i = nodestack.size() - 1; i < nodestack.size(); --i) {
    node = std::shared_ptr<const TrieNode>(nodestack[i]->Clone());
    const_cast<TrieNode *>(node.get())->children_[key[i]] = child_node;
    child_node = node;
  }*/
  return Trie(node);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  auto node = this->root_;
  std::vector<std::shared_ptr<TrieNode>> nodestack;
  uint64_t startkey = 0;
  uint64_t keylen = key.size();

  // delete node
  while (startkey < keylen && node) {
    char c = key[startkey++];
    nodestack.push_back(std::shared_ptr<TrieNode>(node->Clone()));
    if (node->children_.find(c) == node->children_.end()) {
      node = nullptr;
      break;
    }
    node = node->children_.at(c);
  }
  if (startkey != keylen || !node || !node->is_value_node_) {
    return *this;
  }

  std::shared_ptr<const TrieNode> lastnode =
      node->children_.empty() ? nullptr : std::make_shared<const TrieNode>(node->children_);
  int startdel = nodestack.size() - 1;
  if (startdel >= 0) {
    nodestack[startdel]->is_value_node_ = false;
  }

  node = lastnode;
  for (; startdel >= 0; startdel--) {
    node = std::shared_ptr<const TrieNode>(nodestack[startdel]->Clone());
    char c = key[startdel];
    if (lastnode) {
      const_cast<TrieNode *>(node.get())->children_[c] = lastnode;
    } else {
      const_cast<TrieNode *>(node.get())->children_.erase(c);
    }
    lastnode = node;
    if (lastnode->children_.empty() && !lastnode->is_value_node_) {
      lastnode = nullptr;
    }
  }

  if (node->children_.empty() && !node->is_value_node_) {
    return Trie(nullptr);
  }
  // traversal the trie to delete node without children]
  return Trie(node);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
