//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <pthread.h>
#include <cstddef>
#include <ostream>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //    "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //    "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock latch(latch_);
  bool has_free_page = false;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      has_free_page = true;
      break;
    }
  }
  // return nullptr if all frames are currently in use and not evictable (in another word, pinned).
  if (!has_free_page) {
    return nullptr;
  }
  // allocate the page
  *page_id = AllocatePage();

  // get the frame
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // get the frame_id to be evicted
    replacer_->Evict(&frame_id);
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();
    BUSTUB_ASSERT(!pages_[frame_id].GetPinCount(), "Pin count should be 0.");
    // If the replacement frame has a dirty page, you should write it back to the disk first. You also need to reset the
    // memory and metadata for the new page.
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();
    page_table_.erase(evicted_page_id);
  }
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  page_table_[*page_id] = frame_id;
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;

  return &pages_[frame_id];
}

/*
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock latch(latch_);
  Page *page;
  frame_id_t fid;
  page_id_t pid;
  if (page_table_.count(page_id) != 0U) {
    fid = page_table_[page_id];
  } else {
    if (!free_list_.empty()) {
      fid = free_list_.front();
      free_list_.pop_front();
    } else if (replacer_->Evict(&fid)) {
      page = &pages_[fid];
      pid = page->GetPageId();
      BUSTUB_ASSERT(!page->GetPinCount(), "Pin count should be 0.");
      if (page->IsDirty()) { disk_manager_->WritePage(pid, page->GetData());
}
      page->ResetMemory();
      page_table_.erase(pid);
    } else {
      return nullptr;
    }
    page = &pages_[fid];
    disk_manager_->ReadPage(page_id, page->GetData());
    page->page_id_ = page_id;
    page->is_dirty_ = false;
  }
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  pages_[fid].pin_count_++;
  page_table_[page_id] = fid;
  return &pages_[fid];
}*/


auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock latch(latch_);
  // First search for page_id in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    // find the page
    frame_id_t frame_id = page_table_[page_id];
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  // Return nullptr if page_id needs to be fetched from the disk, but all frames are currently in use and not evictable
  // (in another word, pinned).
  bool has_free_page = false;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      has_free_page = true;
      break;
    }
  }
  if (!has_free_page) {
    return nullptr;
  }

  // otherwise, pick a replacement frame from either the free list or the replacer (always find from the free list
  // first)
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // no free frame in free_list, evict one
    replacer_->Evict(&frame_id);
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();
    // if the old page is dirty, you need to write it back to disk and update the metadata of the new page
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();
    page_table_.erase(evicted_page_id);
  }

  // read the page from disk by calling disk_manager_->ReadPage(), and replace the old page in the frame.
  page_table_[page_id] = frame_id;
  pages_[frame_id].page_id_ = page_id;

  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_ ++;

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock latch(latch_);
  if (page_table_.find(page_id) == page_table_.end()) { return false;}
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount()<=0) {
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() <= 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock latch(latch_);
  // cannot be INVALID_PAGE_ID
  if (page_id == INVALID_PAGE_ID) { return false;}
  if (page_table_.find(page_id) == page_table_.end()) { return false;}
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto [page_id, frame_id] : page_table_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock latch(latch_);
  // If page_id is not in the buffer pool, do nothing and return true.
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  // If the page is pinned and cannot be deleted, return false immediately
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  // delete the page
  // stop tracking the frame in the replacer and add the frame back to the free list
  replacer_->Remove(frame_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  free_list_.push_back(frame_id);
  page_table_.erase(page_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *fetchpage = FetchPage(page_id);
  return {this, fetchpage};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *fetchpage = FetchPage(page_id);
  fetchpage->rwlatch_.RLock();
  return {this, fetchpage};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *fetchpage = FetchPage(page_id);
  // std::cout<<"fetchpage page"<<std::endl;
  std::cout<<"pageid:"<<fetchpage->GetPageId()<<"  ";
  std::cout<<"pincount: "<<fetchpage->GetPinCount()<<std::endl;
  fetchpage->rwlatch_.WLock();
  std::cout<<"fetchpage page ok"<<std::endl;
  return {this, fetchpage};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *newpage = NewPage(page_id);
  return {this, newpage};
}

}  // namespace bustub
