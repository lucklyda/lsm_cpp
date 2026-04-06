#pragma once
#include <type_traits>  // 用于 type traits
#include <stdint.h>
#include <optional>
template<typename Key, typename Value>
class BlockCache {
protected:
    uint32_t capacity_; 
public:
    // 编译期检查：Key 必须可拷贝构造且可拷贝赋值
    static_assert(std::is_copy_constructible<Key>::value,
                  "Key must be copy constructible");
    static_assert(std::is_copy_assignable<Key>::value,
                  "Key must be copy assignable");

    BlockCache(uint32_t capacity = 8192) : capacity_(capacity) {}
    virtual ~BlockCache() = default;

    virtual void insert(const Key& key, Value value) = 0;
    virtual std::optional<Value> get(const Key& key) = 0;    
    virtual uint32_t size() = 0;         
};