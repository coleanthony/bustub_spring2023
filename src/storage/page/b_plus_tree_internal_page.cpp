//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <iterator>
#include <sstream>
#include <utility>

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
// #include "test_util.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetInitVal(int max_size, const ValueType &left, const KeyType &mid,
                                                const ValueType &right) {
  Init(max_size);
  array_[0].second = left;
  array_[1] = std::make_pair(mid, right);
  SetSize(2);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // BUSTUB_ASSERT(index >= 0 && index < GetSize(), "index out of range");
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  auto it = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });
  return std::distance(array_, it);
}
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValue(const KeyType &key, const KeyComparator &comparator) const
    -> std::pair<ValueType, int> {
  // use binarysearch to find the answer
  int left = 1;
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
      return std::make_pair(ValueAt(mid), mid);
    }
  }
  return std::make_pair(ValueAt(right), right);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertValueAt(const KeyType &key, const ValueType &value, int position) {
  int n = GetSize();
  BUSTUB_ASSERT(n + 1 <= GetMaxSize(), "can not insert data into a full internalpage");
  BUSTUB_ASSERT(position >= 0 && position <= n, "position error");
  for (auto i = GetSize(); i > position; i--) {
    array_[i] = std::move(array_[i - 1]);
  }
  array_[position] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *newpage) {
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *newpage) {
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveBackToFront(BPlusTreeInternalPage *newpage) {
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFrontToBack(BPlusTreeInternalPage *newpage) {
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

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveByIndex(int remove_index) {
  int size = GetSize();
  BUSTUB_ASSERT(remove_index < size, "index out of range");
  for (int i = remove_index; i < size - 1; i++) {
    array_[i] = std::move(array_[i + 1]);
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::StoleFromLeftSibling(
    BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *internal) {
  for (int index = internal->GetSize(); index > 0; index--) {
    internal->SetKeyAt(index, internal->KeyAt(index - 1));
    internal->SetValueAt(index, internal->ValueAt(index - 1));
  }
  internal->SetKeyAt(0, KeyAt(GetSize() - 1));
  internal->SetValueAt(0, ValueAt(GetSize() - 1));
  IncreaseSize(-1);
  internal->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::StoleFromRightSibling(
    BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *internal) {
  internal->SetKeyAt(internal->GetSize(), KeyAt(0));
  internal->SetValueAt(internal->GetSize(), ValueAt(0));
  for (int index = 0; index < GetSize() - 1; index++) {
    SetKeyAt(index, KeyAt(index + 1));
    SetValueAt(index, ValueAt(index + 1));
  }
  IncreaseSize(-1);
  internal->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemovePage(const KeyType &key, page_id_t removed_page, KeyComparator cmp) -> bool {
  if (cmp(key, KeyAt(1)) < 0 && removed_page == ValueAt(0)) {
    // we need to remove page zero
    for (int index = 0; index < GetSize() - 1; index++) {
      array_[index] = std::move(array_[index + 1]);
    }
    IncreaseSize(-1);
  } else {
    int index = 1;
    for (; index < GetSize(); index++) {
      if (cmp(key, KeyAt(index)) == 0 && ValueAt(index) == removed_page) {
        break;
      }
    }
    for (; index < GetSize() - 1; index++) {
      array_[index] = std::move(array_[index + 1]);
    }
    IncreaseSize(-1);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CombieWithRightSibling(
    BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *right_sibling) {
  SetKeyAt(GetSize(), right_sibling->KeyAt(0));
  SetValueAt(GetSize(), right_sibling->ValueAt(0));
  for (int i = 1; i < right_sibling->GetSize(); i++) {
    SetKeyAt(GetSize() + i, right_sibling->KeyAt(i));
    SetValueAt(GetSize() + i, right_sibling->ValueAt(i));
  }
  SetSize(GetSize() + right_sibling->GetSize());
  right_sibling->SetSize(0);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
