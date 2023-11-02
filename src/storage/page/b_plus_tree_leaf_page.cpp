//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "type/value.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of range");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of range");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyValueAt(int index) const -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValueIndex(const KeyType &key, const KeyComparator &comparator) const
    -> std::pair<int, bool> {
  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    auto res = comparator(key, KeyAt(mid));
    if (res < 0) {
      right = mid - 1;
    } else if (res > 0) {
      left = mid + 1;
    } else {
      // find the answer
      return std::make_pair(mid, true);
    }
  }
  return std::make_pair(left, false);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValue(const KeyType &key, ValueType *value, const KeyComparator &comparator) const
    -> bool {
  // find the requested value in leafnode
  if (!GetSize()) {
    return false;
  }
  // use binarysearch to find the answer
  auto findval = FindValueIndex(key, comparator);
  if (findval.second) {
    *value = ValueAt(findval.first);
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertValue(const KeyType &key, const ValueType &value,
                                             const KeyComparator &comparator) -> bool {
  // insert node into the leaf page
  int n = GetSize();
  int startindex = 0;
  if (n != 0) {
    auto findval = FindValueIndex(key, comparator);
    if (findval.second) {
      // if user try to insert duplicate keys return false
      return false;
    }
    startindex = findval.first;
  }
  // can not insert into a full leaf
  BUSTUB_ASSERT(n + 1 <= GetMaxSize(), "no space to store the data");
  for (auto i = GetSize(); i > startindex; i--) {
    array_[i] = std::move(array_[i - 1]);
  }
  array_[startindex] = std::make_pair(key, value);
  IncreaseSize(1);
  // std::cout<<"b_plus_tree_leaf_page: insert data successfully"<<std::endl;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertValueAt(const KeyType &key, const ValueType &value, int position) {
  int n = GetSize();
  BUSTUB_ASSERT(n + 1 <= GetMaxSize(), "can not insert data into a full leafpage");
  BUSTUB_ASSERT(position >= 0 && position <= n, "position error");
  for (auto i = GetSize(); i > position; i--) {
    array_[i] = std::move(array_[i - 1]);
  }
  array_[position] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *newpage) {
  int size = GetSize() / 2;
  int startsize = GetSize() - size;
  int newpagecursize = newpage->GetSize();
  BUSTUB_ASSERT(newpagecursize + size <= newpage->GetMaxSize(), "no enough space to store the data");
  for (int i = newpagecursize; i < newpagecursize + size; i++) {
    newpage->array_[i] = std::move(this->array_[i - newpagecursize + startsize]);
  }
  newpage->IncreaseSize(size);
  this->IncreaseSize(-size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *newpage) {
  int size = GetSize();
  int newpagecursize = newpage->GetSize();
  BUSTUB_ASSERT(newpagecursize + size <= newpage->GetMaxSize(), "no enough space to store the data");
  for (int i = newpagecursize; i < newpagecursize + size; i++) {
    newpage->array_[i] = std::move(this->array_[i - newpagecursize]);
  }
  newpage->IncreaseSize(size);
  this->IncreaseSize(-size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveByIndex(int remove_index) {
  int size = GetSize();
  BUSTUB_ASSERT(remove_index < size, "index out of range");
  for (int i = remove_index; i < size - 1; i++) {
    array_[i] = std::move(array_[i + 1]);
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveBackToFront(BPlusTreeLeafPage *newpage) {
  int size = GetSize();
  int newpagesize = newpage->GetSize();
  BUSTUB_ASSERT(size > 0, "can not move any data");
  BUSTUB_ASSERT(newpagesize + 1 <= newpage->GetMaxSize(), "no enough space to store the data");
  for (int i = newpagesize; i >= 1; i--) {
    newpage->array_[i] = std::move(newpage->array_[i - 1]);
  }
  newpage->array_[0] = std::move(this->array_[size - 1]);
  newpage->IncreaseSize(1);
  this->IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFrontToBack(BPlusTreeLeafPage *newpage) {
  int size = GetSize();
  int newpagesize = newpage->GetSize();
  BUSTUB_ASSERT(size > 0, "can not move any data");
  BUSTUB_ASSERT(newpagesize + 1 <= newpage->GetMaxSize(), "no enough space to store the data");
  newpage->array_[newpagesize] = std::move(this->array_[0]);
  for (int i = 0; i < size - 1; i++) {
    this->array_[i] = std::move(this->array_[i + 1]);
  }
  newpage->IncreaseSize(1);
  this->IncreaseSize(-1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
