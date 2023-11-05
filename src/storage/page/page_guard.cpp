
#include "storage/page/page_guard.h"
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/page.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_=that.is_dirty_;
  that.bpm_=nullptr;
  that.page_=nullptr;
  that.is_dirty_=false;

}

void BasicPageGuard::Drop() {
  // clear all contents
  if ((bpm_ != nullptr) && (page_ != nullptr) ) {
    // std::cout<<"drop page "<<page_->GetPageId()<<std::endl;
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_=false;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this!=&that) {
    Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_=that.is_dirty_;
    // std::cout<<"get page "<<page_->GetPageId()<<std::endl;
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_=false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { 
  // Drop();
  guard_ = std::move(that.guard_); 
  // std::cout<<"readpageguard get page "<<guard_.PageId()<<std::endl;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this!=&that) {
    Drop();
    if (guard_.page_!=nullptr) {
      guard_.page_->RUnlatch();
    }
    guard_ = std::move(that.guard_);
  }
  // std::cout<<"readpageguard get page "<<guard_.PageId()<<std::endl;
  return *this;
}

void ReadPageGuard::Drop() {
  if ((guard_.page_ != nullptr) && (guard_.bpm_ != nullptr)) {
    // std::cout<<"readpageguard drop page "<<guard_.PageId()<<std::endl;
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { 
  // Drop();
  guard_ = std::move(that.guard_); 
  // std::cout<<"writepageguard get page "<<guard_.PageId()<<std::endl;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if(this!=&that){
    Drop();
    if (guard_.page_!=nullptr) {
      guard_.page_->WUnlatch();
    }
    guard_ = std::move(that.guard_);
  }
  // std::cout<<"writepageguard get page "<<guard_.PageId()<<std::endl;
  return *this;
}

void WritePageGuard::Drop() {
  if ((guard_.bpm_ != nullptr)&&(guard_.page_ != nullptr)) {
    // std::cout<<"writepageguard drop page "<<guard_.PageId()<<std::endl;
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub


