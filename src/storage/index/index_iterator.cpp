/**
 * index_iterator.cpp
 */
#include <cassert>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t head_page_id) : bpm_(bpm) {
  BasicPageGuard headerwg = bpm_->FetchPageBasic(head_page_id);
  auto headpage = headerwg.As<BPlusTreeHeaderPage>();
  auto headpageid = headpage->root_page_id_;
  if (headpageid == INVALID_PAGE_ID) {
    return;
  }
  page_ = bpm_->FetchPage(headpageid);
  page_->RLatch();
  headerwg.Drop();
  while (true) {
    auto internel_page = reinterpret_cast<const InternalPage *>(page_->GetData());
    if (internel_page->IsLeafPage()) {
      break;
    }
    headpageid = internel_page->ValueAt(0);
    Page *nextpage = bpm_->FetchPage(headpageid);
    nextpage->RLatch();
    page_->RUnlatch();
    page_ = nextpage;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t head_page_id, const KeyType &key,
                                  const KeyComparator &comparator)
    : bpm_(bpm) {
  BasicPageGuard headerwg = bpm_->FetchPageBasic(head_page_id);
  auto headpage = headerwg.As<BPlusTreeHeaderPage>();
  page_id_t headpageid = headpage->root_page_id_;
  if (headpageid != INVALID_PAGE_ID) {
    page_ = bpm_->FetchPage(headpageid);
    page_->RLatch();
    headerwg.Drop();
    while (true) {
      auto internel_page = reinterpret_cast<const InternalPage *>(page_->GetData());
      if (internel_page->IsLeafPage()) {
        break;
      }
      int index = internel_page->FindValue(key, comparator).first;
      headpageid = internel_page->ValueAt(index);
      Page *nextpage = bpm_->FetchPage(headpageid);
      nextpage->RLatch();
      page_->RUnlatch();
      page_ = nextpage;
    }
  }
  page_->RLatch();
  auto leafpage = reinterpret_cast<const LeafPage *>(page_->GetData());
  index_ = leafpage->FindValueIndex(key, comparator).first;
  page_->RUnlatch();
}

/*
#define ITERATOR_CONSTRUCTOR(cond) ReadPageGuard rg = bpm_->FetchPageRead(header_page_id); \
                                    auto root_page = rg.As<BPlusTreeHeaderPage>(); \
                                    auto pid = root_page->root_page_id_; \
                                    if (pid == INVALID_PAGE_ID) return; \
                                    page_ = bpm_->FetchPage(pid); \
                                    page_->RLatch(); \
                                    rg.Drop(); \
                                    while (1) { \
                                        auto internal_page = reinterpret_cast<const InternalPage *>(page_->GetData()); \
                                        if (internal_page->IsLeafPage()) break; \
                                        pid = cond; \
                                        Page *child = bpm_->FetchPage(pid); \
                                        child->RLatch(); \
                                        page_->RUnlatch(); \
                                        page_ = child; \
                                    } \

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator( BufferPoolManager *bpm,page_id_t header_page_id) : bpm_(bpm) {
    ITERATOR_CONSTRUCTOR(internal_page->ValueAt(0))
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm,page_id_t header_page_id, const KeyType &key, const
KeyComparator &comparator) : bpm_(bpm) { ITERATOR_CONSTRUCTOR(internal_page->FindValue(key, comparator).first)
    page_->RLatch();
    auto leaf_page = reinterpret_cast<const LeafPage *>(page_->GetData());
    index_ = leaf_page->FindValueIndex(key, comparator).first;
    page_->RUnlatch();
}*/

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  bool isend = true;
  if (page_ != nullptr) {
    page_->RLatch();
    auto leafpage = reinterpret_cast<const LeafPage *>(page_->GetData());
    if (index_ < leafpage->GetSize() || leafpage->GetNextPageId() != INVALID_PAGE_ID) {
      isend = false;
    }
    page_->RUnlatch();
  }
  return isend;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  // BUSTUB_ASSERT(page_, "the page is empty");
  page_->RLatch();
  auto leafpage = reinterpret_cast<const LeafPage *>(page_->GetData());
  const MappingType &res = leafpage->KeyValueAt(index_);
  page_->RUnlatch();
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // from the present page to the next
  auto curpage = page_;
  curpage->RLatch();
  auto leafpage = reinterpret_cast<const LeafPage *>(page_->GetData());
  if (index_ < leafpage->GetSize() - 1) {
    index_++;
  } else {
    int nextpageid = leafpage->GetNextPageId();
    index_ = 0;
    if (nextpageid == INVALID_PAGE_ID) {
      page_ = nullptr;
    } else {
      page_ = bpm_->FetchPage(nextpageid);
    }
  }
  curpage->RUnlatch();
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return itr.page_ == page_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return itr.page_ != page_ || itr.index_ != index_;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
