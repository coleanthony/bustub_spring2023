//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <exception>
#include <mutex>
#include <regex>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::lock_guard<std::mutex> lock(latch_);
    if (curr_size_==0) {
        return false;
    }
    //first find history list;
    for (auto it=history_list_.rbegin(); it!=history_list_.rend(); it++) {
        frame_id_t frmid=*it;
        if (is_evictable_[frmid]) {
            access_count_[frmid]=0;
            curr_size_--;
            *frame_id=frmid;
            history_list_.erase(history_map_[frmid]);
            history_map_.erase(frmid);
            is_evictable_[frmid]=false;
            return true;
        }
    }
    //then find cache_list
    for (auto it=lruk_cache_list_.rbegin(); it!=lruk_cache_list_.rend(); it++) {
        frame_id_t frmid=*it;
        if (is_evictable_[frmid]) {
            access_count_[frmid]=0;
            curr_size_--;
            *frame_id=frmid;
            lruk_cache_list_.erase(lruk_cache_map_[frmid]);
            lruk_cache_map_.erase(frmid);
            is_evictable_[frmid]=false;
            return true;
        }
    }
    return false; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::mutex> lock(latch_);
    if (frame_id>static_cast<int>(replacer_size_)) {
        throw std::exception();
    }
    access_count_[frame_id]++;

    if (access_count_[frame_id]==k_) {
        //remove the frame from history and put it into cache
        history_list_.erase(history_map_[frame_id]);
        history_map_.erase(frame_id);
        lruk_cache_list_.push_front(frame_id);
        lruk_cache_map_[frame_id]=lruk_cache_list_.begin();
    }else if (access_count_[frame_id]<k_) {
        //the frame is still in history
        if (history_map_.find(frame_id)!=history_map_.end()) {
            history_list_.erase(history_map_[frame_id]);
        }
        history_list_.push_front(frame_id);
        history_map_[frame_id]=history_list_.begin();
    }else{
        //the frame has already been in the cache
        if (lruk_cache_map_.find(frame_id)!=lruk_cache_map_.end()) {
            lruk_cache_list_.erase(lruk_cache_map_[frame_id]);
        }
        lruk_cache_list_.push_front(frame_id);
        lruk_cache_map_[frame_id]=lruk_cache_list_.begin();
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> lock(latch_);
    if (frame_id>static_cast<int>(replacer_size_)) {
        throw std::exception();
    }
    if (access_count_[frame_id]==0) {
        return;
    }

    if (set_evictable) {
        if (!is_evictable_[frame_id]) {
            is_evictable_[frame_id]=true;
            curr_size_++;
        }
    }else{
        if (is_evictable_[frame_id]) {
            is_evictable_[frame_id]=false;
            curr_size_--;
        }
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    //frame_id too large
    if (frame_id>static_cast<int>(replacer_size_)) {
        throw std::exception();
    }
    //not visited yet
    if (access_count_.find(frame_id)==access_count_.end()||access_count_[frame_id]==0) {
        return;
    }
    //non_evictable
    if (!is_evictable_[frame_id]) {
        throw std::exception();
    }

    //in history_list or cache_list
    if (history_map_.find(frame_id)!=history_map_.end()) {
        history_list_.erase(history_map_[frame_id]);
        history_map_.erase(frame_id);
    }
    if (lruk_cache_map_.find(frame_id)!=lruk_cache_map_.end()) {
        lruk_cache_list_.erase(lruk_cache_map_[frame_id]);
        lruk_cache_map_.erase(frame_id);
    }
    is_evictable_[frame_id]=false;
    access_count_.erase(frame_id);
    curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { 
    return curr_size_;
}

}  // namespace bustub
