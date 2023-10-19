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
#include <execution>
#include <mutex>
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

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {

}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    
}

auto LRUKReplacer::Size() -> size_t { 
    return curr_size_;
}

}  // namespace bustub
