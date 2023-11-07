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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard &&guard, ReadPageGuard &&head, int index)
    : bpm_(bpm), guard_(std::move(guard)), head_(std::move(head)), index_(index) {
  if (bpm_ != nullptr || index_ != -1) {
    page_id_ = guard_.PageId();
  } else {
    page_id_ = INVALID_PAGE_ID;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto *leafpage = guard_.As<LeafPage>();
  return leafpage->KeyValueAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // from the present page to the next
  if (!IsEnd()) {
    auto leafpage = guard_.As<LeafPage>();
    index_++;
    if (index_ == leafpage->GetSize()) {
      if (leafpage->GetNextPageId() == INVALID_PAGE_ID) {
        bpm_ = nullptr;
        guard_.Drop();
        head_.Drop();
        index_ = -1;
        page_id_ = INVALID_PAGE_ID;
        return *this;
      }
      guard_ = bpm_->FetchPageRead(leafpage->GetNextPageId());
      index_ = 0;
      page_id_ = guard_.PageId();
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if (page_id_ == INVALID_PAGE_ID) {
    return itr.page_id_ == INVALID_PAGE_ID;
  }
  return page_id_ == itr.page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
