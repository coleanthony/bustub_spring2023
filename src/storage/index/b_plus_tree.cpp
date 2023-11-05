#include <deque>
#include <iostream>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "execution/plans/abstract_plan.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return bpm_->FetchPageRead(header_page_id_).PageId() == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  ReadPageGuard readpageguard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = readpageguard.As<BPlusTreeHeaderPage>();
  page_id_t page_id = root_page->root_page_id_;
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard readpg = bpm_->FetchPageRead(page_id);
  readpageguard.Drop();
  while (true) {
    auto internal_page = readpg.As<InternalPage>();
    if (internal_page->IsLeafPage()) {
      // isleafpage, get the data
      auto leaf_page = readpg.As<LeafPage>();
      ValueType val;
      bool findanswer = leaf_page->FindValue(key, &val, comparator_);
      if (findanswer) {
        result->push_back(val);
        return true;
      }
      return false;
    }
    // else, it's the internalpage. continue to find the leaf_node
    auto findval = internal_page->FindValue(key, comparator_);
    page_id = findval.first;
    ReadPageGuard readpg_temp = bpm_->FetchPageRead(page_id);
    readpg = std::move(readpg_temp);
  }
  // return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  WritePageGuard headerwg = bpm_->FetchPageWrite(header_page_id_);
  auto headpage = headerwg.AsMut<BPlusTreeHeaderPage>();
  ctx.header_page_=std::move(headerwg);
  if (headpage->root_page_id_ == INVALID_PAGE_ID) {
    // std::cout<<"insert value into header page"<<std::endl;
    // if current tree is empty, start new tree, update root page id and insert entry
    // get a new page
    auto alloc_page=bpm_->NewPageGuarded(&headpage->root_page_id_);
    if (headpage->root_page_id_==INVALID_PAGE_ID) {
      std::cout<<"allocate head page error"<<std::endl;
      return false;
    }
    auto leafpage=alloc_page.AsMut<LeafPage>();
    // inseat data into leaf page
    leafpage->Init(leaf_max_size_);
    leafpage->InsertValue(key, value, comparator_);
    return true;
  }
  // otherwise insert into leaf page.
  // find the right position to insert the value
  return FindLeafNode(headpage->root_page_id_,key,value,txn,&ctx);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafNode(page_id_t page_id, const KeyType &key,const ValueType &value,Transaction *txn, Context *ctx)->bool{
  WritePageGuard basic_wg = bpm_->FetchPageWrite(page_id);
  auto basic_page = basic_wg.AsMut<BPlusTreePage>();
  if (!basic_page->IsLeafPage()) {
    auto *internal=reinterpret_cast<InternalPage *>(basic_page);
    auto findval=internal->FindValue(key,comparator_);
    if (findval.first==INVALID_PAGE_ID) {
      return false;
    }
    ctx->write_set_.push_back(std::move(basic_wg));
    ctx->indexes_.push_back(findval.second);
    return FindLeafNode(findval.first, key, value,txn, ctx);
  }
  //is leaf page
  std::cout<<"successfully find the leafnode"<<std::endl;
  page_id_t leafpage_id=basic_wg.PageId();
  auto *leafpage=reinterpret_cast<LeafPage *>(basic_page);
  return InsertIntoNode(leafpage, leafpage_id, key, value, txn, ctx);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoNode(LeafPage *leafpage,page_id_t leaf_id, const KeyType &key,const ValueType &value,Transaction *txn, Context *ctx)->bool{
  std::pair<int, bool> findval = leafpage->FindValueIndex(key, comparator_);
  if (findval.second) {
    // the key is already in the leafpage
    std::cout<<"the key is already in the leafpage"<<std::endl;
    return false;
  }
  // !findval.second
  if (leafpage->GetSize() < leafpage->GetMaxSize()) {
    // the page is not full,just insert the page
    std::cout<<"the page is not full, just insert the page"<<std::endl;
    leafpage->InsertValueAt(key, value, findval.first);
    return true;
  }
  // !findval.second and the leafpage is full
  // get a new page

  LeafPage *buddy;
  page_id_t buddy_id=DivideLeafNode(leafpage, &buddy);
  if (buddy_id==INVALID_PAGE_ID) {
    return false;
  }
  const KeyType &key_to_insert = buddy->KeyAt(0);
  if (ctx->write_set_.empty()) {
    // No more parent to insert, this case you should allocate a new page to become root
    std::cout<<"No more parent to insert, this case you should allocate a new page to become root"<<std::endl;
    MakeNewRootNode(leaf_id, buddy_id, leafpage->KeyAt(0), key_to_insert, ctx);
  } else {
    std::cout<<"insert into internal node"<<std::endl;
    InsertKeyToInternalNode(key_to_insert, buddy_id, ctx);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertKeyToInternalNode(const KeyType &key, page_id_t value, Context *ctx){
  WritePageGuard guard=std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  int index=ctx->indexes_.back();
  ctx->indexes_.pop_back();
  auto *internal_page=guard.AsMut<InternalPage>();
  int internal_page_size = internal_page->GetSize();
  if (internal_page_size<internal_page->GetMaxSize()) {
      // auto findval=internal_page->FindValue(key,comparator_);
      internal_page->InsertValueAt(key, value, index + 1);
  }else{
    page_id_t internal_page_id=guard.PageId();
    InternalPage *buddy;
    page_id_t buddy_id=DivideInternalNode(internal_page, &buddy);
    if (buddy_id==INVALID_PAGE_ID) {
      return;
    }
    if (comparator_(key, buddy->KeyAt(0)) >= 0) {
      ctx->write_set_.push_back(bpm_->FetchPageWrite(buddy_id));
      InsertKeyToInternalNode(key, value, ctx);
    } else {
      ctx->write_set_.push_back(std::move(guard));
      InsertKeyToInternalNode(key, value, ctx);
    }
    if (internal_page->GetSize() > buddy->GetSize() + 1) {
      internal_page->MoveBackToFront(buddy);
    } else if (internal_page->GetSize() + 1 < buddy->GetSize()) {
      internal_page->MoveFrontToBack(buddy);
    }
    const KeyType &key_to_insert = buddy->KeyAt(0);
    if (ctx->write_set_.empty()) {
      // No more parent to insert, this case you should allocate a new page to become root
      MakeNewRootNode(internal_page_id, buddy_id, internal_page->KeyAt(0), key_to_insert, ctx);
    } else {
      InsertKeyToInternalNode(key_to_insert, buddy_id, ctx);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DivideLeafNode(LeafPage *leaf, LeafPage **buddy) -> page_id_t {
  page_id_t alloc_page_id = INVALID_PAGE_ID;
  auto alloc_page = bpm_->NewPageGuarded(&alloc_page_id);
  *buddy = alloc_page.AsMut<LeafPage>();
  if (*buddy == nullptr) {
    return INVALID_PAGE_ID;
  }
  (*buddy)->Init(leaf_max_size_);
  (*buddy)->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(alloc_page_id);
  int old_size = leaf->GetSize();
  (*buddy)->SetSize(old_size / 2);
  leaf->SetSize(old_size - (*buddy)->GetSize());
  for (int i = 0; i < (*buddy)->GetSize(); i++) {
    (*buddy)->SetKeyAt(i, leaf->KeyAt(leaf->GetSize() + i));
    (*buddy)->SetValueAt(i, leaf->ValueAt(leaf->GetSize() + i));
  }
  return alloc_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DivideInternalNode(InternalPage *internalpage, InternalPage **buddy) -> page_id_t {
  page_id_t alloc_page_id = INVALID_PAGE_ID;
  auto alloc_page = bpm_->NewPageGuarded(&alloc_page_id);
  *buddy = alloc_page.AsMut<InternalPage>();
  if (*buddy == nullptr) {
    return INVALID_PAGE_ID;
  }
  (*buddy)->Init(internal_max_size_);
  int old_size = internalpage->GetSize();
  (*buddy)->SetSize(old_size / 2);
  internalpage->SetSize(old_size - (*buddy)->GetSize());
  for (int i = 0; i < (*buddy)->GetSize(); i++) {
    (*buddy)->SetKeyAt(i, internalpage->KeyAt(internalpage->GetSize() + i));
    (*buddy)->SetValueAt(i, internalpage->ValueAt(internalpage->GetSize() + i));
  }
  return alloc_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MakeNewRootNode(page_id_t pg1_id, page_id_t pg2_id, const KeyType &key1, const KeyType &key2,
                                     Context *ctx) {
  page_id_t alloc_page_id = INVALID_PAGE_ID;
  auto alloc_page = bpm_->NewPageGuarded(&alloc_page_id);
  if (alloc_page_id == INVALID_PAGE_ID) {
    return;
  }
  auto *new_root = alloc_page.AsMut<InternalPage>();
  new_root->Init(internal_max_size_);
  new_root->SetSize(2);
  new_root->SetKeyAt(0, key1);
  new_root->SetKeyAt(1, key2);
  new_root->SetValueAt(0, pg1_id);
  new_root->SetValueAt(1, pg2_id);
  auto guard = std::move(ctx->header_page_);
  auto head_page = guard->AsMut<BPlusTreeHeaderPage>();
  head_page->root_page_id_ = alloc_page_id;
  std::cout<<"MakeNewRootNode successfully"<<std::endl;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  WritePageGuard headerwg = bpm_->FetchPageWrite(header_page_id_);
  auto headpage = headerwg.As<BPlusTreeHeaderPage>();
  page_id_t headpageid = headpage->root_page_id_;
  if (headpageid == INVALID_PAGE_ID) {
    // the b+tree is empty
    return;
  }

  // find the leafnode
  std::deque<int> indexes;
  std::deque<WritePageGuard> writeguards;
  page_id_t pageid = headpageid;

  while (true) {
    // std::cout<<"find the internal page"<<std::endl;
    WritePageGuard internal_wg = bpm_->FetchPageWrite(pageid);
    // std::cout<<"FetchPageWrite the internal page"<<std::endl;
    auto internalpage = internal_wg.As<InternalPage>();
    if (internalpage->GetSize() < internalpage->GetMaxSize()) {
      headerwg.Drop();
      while (!writeguards.empty()) {
        writeguards.pop_front();
      }
    }
    writeguards.push_back(std::move(internal_wg));
    if (internalpage->IsLeafPage()) {
      // find the leafpage to insert val
      break;
    }
    // else, it's the internalpage. continue to find the leaf_node
    auto findval = internalpage->FindValue(key, comparator_);
    pageid = findval.first;
    indexes.push_back(findval.second);
  }

  // std::cout<<"successfully find the leafnode"<<std::endl;
  // successfully find the leafnode
  auto &wg = writeguards.back();
  auto leafpage = wg.As<LeafPage>();
  std::pair<int, bool> findval = leafpage->FindValueIndex(key, comparator_);
  // the key is not in the leafpage
  if (!findval.second) {
    headerwg.Drop();
    while (!writeguards.empty()) {
      writeguards.pop_front();
    }
    return;
  }

  // the key is in the leafpage,delete it
  int removekeyindex = findval.first;
  auto leafpage_rm = wg.AsMut<LeafPage>();
  leafpage_rm->RemoveByIndex(removekeyindex);
  if (leafpage_rm->GetSize() >= leafpage_rm->GetMinSize()) {
    return;
  }

  // std::cout<<"no enough elements in the leafpage, we need to borrow from the left of right brother."<<std::endl;
  // no enough elements in the leafpage
  // we need to borrow from the left of right brother. If the requirement is not met, we need to implement the merge
  // strategy.
  bool is_child_leaf = true;
  while (writeguards.size() >= 2) {
    auto child_wg = std::move(writeguards.back());
    writeguards.pop_back();
    auto &parent_wg = writeguards.back();
    // borrow strategy
    // std::cout<<"borrow stategy"<<std::endl;
    if (Borrow(parent_wg, child_wg, indexes.back(), is_child_leaf)) {
      // can borrow, release the resources and return
      headerwg.Drop();
      while (!writeguards.empty()) {
        writeguards.pop_front();
      }
      return;
    }
    // can not borrow, merge
    // std::cout<<"merge stategy"<<std::endl;
    Merge(parent_wg, child_wg, indexes.back(), is_child_leaf);
    is_child_leaf = false;
    indexes.pop_back();
  }

  // there is one left in writeguards
  // consider it's condition
  // std::cout<<"there is one left in writeguards"<<std::endl;
  WritePageGuard wg_head = std::move(writeguards.back());
  writeguards.pop_back();
  auto internal_page_r = wg_head.As<InternalPage>();
  if (internal_page_r->GetSize() >= internal_page_r->GetMinSize() || internal_page_r->IsLeafPage()) {
    return;
  }
  if (internal_page_r->GetSize() == 1) {
    auto head_page_nxt = wg_head.AsMut<InternalPage>();
    auto headpage = headerwg.AsMut<BPlusTreeHeaderPage>();
    headpage->root_page_id_ = head_page_nxt->ValueAt(0);
  }
  // std::cout<<"delete successfully"<<std::endl;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Borrow(WritePageGuard &parent_wg, WritePageGuard &child_wg, int childindex, bool isChildLeaf)
    -> bool {
  auto parent_page_r = parent_wg.As<InternalPage>();
  int leftindex = -1;
  int rightindex = -1;
  if (childindex > 0) {
    leftindex = childindex - 1;
  }
  if (childindex < parent_page_r->GetSize() - 1) {
    rightindex = childindex + 1;
  }
  // first borrow from left
  if (leftindex != -1) {
    page_id_t sibpageid = parent_page_r->ValueAt(leftindex);
    BasicPageGuard sibpage_wg = bpm_->FetchPageBasic(sibpageid);
    auto siblingpage = sibpage_wg.As<BPlusTreePage>();
    if (siblingpage->GetSize() > siblingpage->GetMinSize()) {
      // can borrow
      auto parent_page_w = parent_wg.AsMut<InternalPage>();
      if (isChildLeaf) {
        auto sibling_page_w = sibpage_wg.AsMut<LeafPage>();
        auto child_page_w = child_wg.AsMut<LeafPage>();
        sibling_page_w->MoveBackToFront(child_page_w);
        parent_page_w->SetKeyAt(childindex, child_page_w->KeyAt(0));

      } else {
        auto sibling_page_w = sibpage_wg.AsMut<InternalPage>();
        auto child_page_w = child_wg.AsMut<InternalPage>();
        sibling_page_w->MoveBackToFront(child_page_w);
        parent_page_w->SetKeyAt(childindex, child_page_w->KeyAt(0));
      }
      return true;
    }
  }

  // borrow from right
  if (rightindex != -1) {
    page_id_t sibpageid = parent_page_r->ValueAt(rightindex);
    BasicPageGuard sibpage_wg = bpm_->FetchPageBasic(sibpageid);
    auto siblingpage = sibpage_wg.As<BPlusTreePage>();
    if (siblingpage->GetSize() > siblingpage->GetMinSize()) {
      // can borrow
      auto parent_page_w = parent_wg.AsMut<InternalPage>();
      if (isChildLeaf) {
        auto sibling_page_w = sibpage_wg.AsMut<LeafPage>();
        auto child_page_w = child_wg.AsMut<LeafPage>();
        sibling_page_w->MoveFrontToBack(child_page_w);
        parent_page_w->SetKeyAt(childindex + 1, sibling_page_w->KeyAt(0));

      } else {
        auto sibling_page_w = sibpage_wg.AsMut<InternalPage>();
        auto child_page_w = child_wg.AsMut<InternalPage>();
        sibling_page_w->MoveFrontToBack(child_page_w);
        parent_page_w->SetKeyAt(childindex + 1, sibling_page_w->KeyAt(0));
      }
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(WritePageGuard &parent_wg, WritePageGuard &child_wg, int childindex, bool isChildLeaf) {
  // merge and delete the corresponding key in parent node
  auto parent_page = parent_wg.AsMut<InternalPage>();
  int r = childindex;
  if (child_wg.AsMut<InternalPage>()->GetSize()) {
    int l = childindex > 0 ? childindex - 1 : childindex;
    r = l + 1;
    BasicPageGuard siblingpage_wg =
        r == childindex ? bpm_->FetchPageBasic(parent_page->ValueAt(l)) : bpm_->FetchPageBasic(parent_page->ValueAt(r));
    // merge the left sibling page and current page or the right sibling page and current page;
    if (isChildLeaf) {
      // is leaf page, change the nextpageid
      auto child_page_w = child_wg.AsMut<LeafPage>();
      auto sibling_page_w = siblingpage_wg.AsMut<LeafPage>();
      if (r == childindex) {
        sibling_page_w->SetNextPageId(child_page_w->GetNextPageId());
      } else {
        child_page_w->SetNextPageId(sibling_page_w->GetNextPageId());
      }
    } else {
      // is internal page
      auto child_page_w = child_wg.AsMut<InternalPage>();
      auto sibling_page_w = siblingpage_wg.AsMut<InternalPage>();
      if (r == childindex) {
        child_page_w->SetKeyAt(0, parent_page->KeyAt(r));
      } else {
        sibling_page_w->SetKeyAt(0, parent_page->KeyAt(r));
      }
    }
    // merge childpage and siblingpage
    if (isChildLeaf) {
      auto child_page_w = child_wg.AsMut<LeafPage>();
      auto sibling_page_w = siblingpage_wg.AsMut<LeafPage>();
      if (r == childindex) {
        child_page_w->MoveAllTo(sibling_page_w);
      } else {
        sibling_page_w->MoveAllTo(child_page_w);
      }
    } else {
      auto child_page_w = child_wg.AsMut<InternalPage>();
      auto sibling_page_w = siblingpage_wg.AsMut<InternalPage>();
      if (r == childindex) {
        child_page_w->MoveAllTo(sibling_page_w);
      } else {
        sibling_page_w->MoveAllTo(child_page_w);
      }
    }
  }
  // whether we need to delete the page?
  parent_page->RemoveByIndex(r);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, header_page_id_); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(bpm_, header_page_id_, key, comparator_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard wg = bpm_->FetchPageRead(header_page_id_);
  auto header_page = wg.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
