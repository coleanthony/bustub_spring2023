//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"
#include "storage/page/hash_table_page_defs.h"
#include "type/value.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * |  NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  /**
   * After creating a new leaf page from buffer pool, must call initialize
   * method to set default values
   * @param max_size Max size of the leaf node
   */
  void Init(int max_size = LEAF_PAGE_SIZE);

  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;
  auto KeyValueAt(int index) const -> const MappingType &;
  void SetKeyAt(int index, const KeyType &key);
  void SetValueAt(int index, const ValueType &value);

  // use binarysearch find the target value's index
  auto FindValueIndex(const KeyType &key, const KeyComparator &comparator) const -> std::pair<int, bool>;
  // search the target value
  auto FindValue(const KeyType &key, ValueType *value, const KeyComparator &comparator) const -> bool;
  // insert value into leafpage
  auto InsertValue(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool;
  // insert value into leafpage in a given position
  void InsertValueAt(const KeyType &key, const ValueType &value, int pos);
  // move half of the data to newpage
  void MoveHalfTo(BPlusTreeLeafPage *newpage);
  // move all of the data to newpage
  void MoveAllTo(BPlusTreeLeafPage *newpage);
  // remove value from current page;
  void RemoveByIndex(int remove_index);
  // move the backdata to the front of the newpage
  void MoveBackToFront(BPlusTreeLeafPage *newpage);
  // move the frontdata to the back of the newpage
  void MoveFrontToBack(BPlusTreeLeafPage *newpage);

  auto DeleteKeyFromNode(const KeyType &key, KeyComparator cmp) -> bool;
  void StoleLastElement(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *thief);
  void StoleFirstElement(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *thief);
  void CombineWithRightSibling(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *right_sibling);

  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
