#include "storage/page/page_guard.h"
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  std::cout<<"get page "<<page_->GetPageId()<<std::endl;
  that.Drop();
}

void BasicPageGuard::Drop() {
  // clear all contents
  if ((bpm_ != nullptr) && (page_ != nullptr) ) {
    std::cout<<"drop page "<<page_->GetPageId()<<std::endl;
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  std::cout<<"get page "<<page_->GetPageId()<<std::endl;
  that.Drop();
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { 
  Drop();
  guard_ = std::move(that.guard_); 
  that.Drop();
  std::cout<<"readpageguard get page "<<guard_.PageId()<<std::endl;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  that.Drop();
  std::cout<<"readpageguard get page "<<guard_.PageId()<<std::endl;
  return *this;
}

void ReadPageGuard::Drop() {
  if ((guard_.page_ != nullptr) && (guard_.bpm_ != nullptr)) {
    std::cout<<"readpageguard drop page "<<guard_.PageId()<<std::endl;
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { 
  Drop();
  guard_ = std::move(that.guard_); 
  that.Drop();
  std::cout<<"writepageguard get page "<<guard_.PageId()<<std::endl;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  that.Drop();
  std::cout<<"writepageguard get page "<<guard_.PageId()<<std::endl;
  return *this;
}

void WritePageGuard::Drop() {
  if ((guard_.bpm_ != nullptr)&&(guard_.page_ != nullptr)) {
    std::cout<<"writepageguard drop page "<<guard_.PageId()<<std::endl;
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
