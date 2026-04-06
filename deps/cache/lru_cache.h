#pragma once
#include "cache.h"
#include <unordered_map>
#include <list>
#include <mutex>
template<typename Key, typename Value>
class LruBlockCache:public BlockCache<Key,Value>
{
private:
    /* data */
    using ListType = std::list<std::pair<Key,Value>>;
    using IterType = typename ListType::iterator;

    ListType lru_list_;
    std::unordered_map<Key,IterType> cache_map_;
    mutable std::mutex mutex;
private:
    void move_to_front(IterType it){
        auto node = std::move(*it);
        lru_list_.erase(it);
        lru_list_.push_front(std::move(node));
        // 更新哈希表中的迭代器指向新的头部
        cache_map_[node.first] = lru_list_.begin();
    }
public:
    LruBlockCache(uint32_t capacity=8192):BlockCache<Key,Value>(capacity){}
    ~LruBlockCache(){
        // std::cout<<"Current Cache Read:"<<read_count<<" And Hit:"<<cache_hit<<std::endl;
        // if(read_count!=0){
        //     double hit_rate = 100*1.0*cache_hit/read_count*1.0;
        //     std::cout<<"Cache Hit Rate:"<<hit_rate<<"%"<<std::endl;
        // }
    }
    void insert(const Key& key, Value value)override{
        std::unique_lock<std::mutex> guard{mutex};
        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            // Key 已存在：更新值，并将节点移到头部
            it->second->second = value;
            move_to_front(it->second);
        }else{
            if (lru_list_.size() >= this->capacity_) {
                auto last = lru_list_.end();
                --last;
                cache_map_.erase(last->first);
                lru_list_.pop_back();
            }
            lru_list_.emplace_front(key, value);
            cache_map_[key] = lru_list_.begin();
        }
    }
    std::optional<Value> get(const Key& key)override{
        std::unique_lock<std::mutex> guard{mutex};
        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            return std::nullopt;
        }
        move_to_front(it->second);
        return it->second->second;
    } 
    uint32_t size()override{
        std::lock_guard<std::mutex> lock(mutex);
        return lru_list_.size();
    }
};

template<typename Key, typename Value>
class ShardedBlockCache:public BlockCache<Key,Value>
{
private:
    /* data */
    using ListType = std::list<std::pair<Key,Value>>;
    using IterType = typename ListType::iterator;
    struct Shard {
        ListType lru_list_;
        std::unordered_map<Key,IterType> cache_map_;
        mutable std::mutex mutex;
        uint32_t inner_capacity;

        explicit Shard(size_t cap) : inner_capacity(cap) {}
    };

    std::vector<std::unique_ptr<Shard>> shards_;
    size_t num_shards_;

    Shard* get_shard(const Key& key) const{
        size_t hash = std::hash<Key>{}(key);
        return shards_[hash % num_shards_].get();
    }
public:
    ShardedBlockCache (uint32_t capacity=8192,size_t num_shards = 16):BlockCache<Key,Value>(capacity)
    {
        num_shards_ = num_shards;
        size_t per_shard = (capacity + num_shards - 1) / num_shards;
        shards_.reserve(num_shards);
        for (size_t i = 0; i < num_shards; ++i) {
            shards_.emplace_back(std::make_unique<Shard>(per_shard));
        }
    }
    ~ShardedBlockCache () = default;

    void insert(const Key& key, Value value)override{
        Shard* shard = get_shard(key);
        std::lock_guard<std::mutex> lock(shard->mutex);
        auto it = shard->map.find(key);
        if (it != shard->map.end()) {
            // 已存在：更新值并移至头部
            it->second->second = value;
            auto node = std::move(*(it->second));
            shard->lru_list.erase(it->second);
            shard->lru_list.push_front(std::move(node));
            it->second = shard->lru_list.begin();
        } else {
            if (shard->lru_list.size() >= shard->capacity) {
                auto last = --shard->lru_list.end();
                shard->map.erase(last->first);
                shard->lru_list.pop_back();
            }
            shard->lru_list.emplace_front(key, value);
            shard->map[key] = shard->lru_list.begin();
        }
    }
    std::optional<Value> get(const Key& key)override{
        Shard* shard = get_shard(key);
        std::lock_guard<std::mutex> lock(shard->mutex);

        auto it = shard->map.find(key);
        if (it == shard->map.end()) {
            return std::nullopt;
        }
        auto node = std::move(*(it->second));
        shard->lru_list.erase(it->second);
        shard->lru_list.push_front(std::move(node));
        it->second = shard->lru_list.begin();
        return it->second->second;
    }
    uint32_t size()override{
        size_t total = 0;
        for (auto& shard : shards_) {
            std::lock_guard<std::mutex> lock(shard->mutex);
            total += shard->lru_list.size();
        }
        return total;
    }
};


