#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <cassert>
#include <cstring>

class Arena {
public:
    explicit Arena(size_t block_size = 4 * 1024 * 1024)
        : block_size_(block_size), alloc_ptr_(nullptr), alloc_bytes_remaining_(0) {}

    ~Arena() {
        for (char* block : blocks_) {
            delete[] block;
        }
    }

    // 禁止拷贝
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    // 分配指定大小的内存，返回指针（字节对齐）
    char* Allocate(size_t bytes) {
        if(bytes==0)return nullptr;
        // 大块直接单独分配
        if (bytes > block_size_ / 4) {
            return AllocateFallback(bytes);
        }
        // 从当前块分配
        if (bytes <= alloc_bytes_remaining_) {
            char* result = alloc_ptr_;
            alloc_ptr_ += bytes;
            alloc_bytes_remaining_ -= bytes;
            return result;
        }
        // 当前块不足，分配新块
        AllocateNewBlock(block_size_);
        char* result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
    }

    // 分配并拷贝字符串内容
    char* AllocateCopy(const char* data, size_t len) {
        char* buf = Allocate(len);
        std::memcpy(buf, data, len);
        return buf;
    }

private:
    char* AllocateFallback(size_t bytes) {
        char* block = new char[bytes];
        blocks_.push_back(block);
        return block;
    }

    void AllocateNewBlock(size_t bytes) {
        char* block = new char[bytes];
        blocks_.push_back(block);
        alloc_ptr_ = block;
        alloc_bytes_remaining_ = bytes;
    }

    const size_t block_size_;
    char* alloc_ptr_;
    size_t alloc_bytes_remaining_;
    std::vector<char*> blocks_;
};