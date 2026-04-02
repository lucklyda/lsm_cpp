#pragma once
#include "revocery/wal.h"
#include "table/table_builder.h"
#include <shared_mutex>
#include <atomic>

class MemTableIterator :public Iterators
{
private:
    mmstore<LsmKey,Value> *map_;
    mmstore_iterator<LsmKey,Value> *iter;
private:
    // void skip_deletes(){
    //     while (iter->is_valid() && iter->get_value()==nullptr)
    //     {
    //         iter->next();
    //     }
        
    // }
public:
    MemTableIterator(mmstore<LsmKey,Value> *map,const Bound<LsmKey>& lower,const Bound<LsmKey>& upper)
    {
        map_ = map;
        iter = map_->create_iterator(lower,upper);
        //skip_deletes();
    }

    MemTableIterator(mmstore<LsmKey,Value> *map)
    {
        map_ = map;
        iter = map_->create_iterator();
       // skip_deletes();
    }

    ~MemTableIterator(){
        delete iter;
    }

    const LsmKey& key()const override{
        if(iter->is_valid()){
            return iter->get_key();
        }else{
            static const LsmKey empty_key{};
            return empty_key;
        }
    }

    Value value()const override{
        if(iter->is_valid()){
            return iter->get_value();
        }else{
            return Value();
        }
    }

    bool is_valid()override{
        return iter->is_valid();
    }

    bool next() override{
        iter->next();
       // skip_deletes();
        return true;
    }

    uint32_t num_active_iterators() override{
        return is_valid()?1:0;
    }

};

class MemTable
{
private:
    std::shared_ptr<mmstore<Key,Value>> map;
    std::shared_ptr<Wal> wal;
    uint64_t id_;
    std::atomic<uint64_t> approximate_size_;
    mutable std::shared_mutex mutex;
public:
    MemTable(uint64_t id){
        map = std::make_shared<Map<Key,Value>>();
        wal = nullptr;
        id_=id;
        approximate_size_=0;
    }

    MemTable(uint64_t id,const char* path,bool recovery){
        map = std::make_shared<Map<Key,Value>>();
        if(recovery){
            wal = std::shared_ptr<Wal>(Wal::recover(path,*map.get()));
            if(wal==nullptr){
                throw std::logic_error("Recovery Error!");
            }
        }else{
            wal =std::shared_ptr<Wal>(new Wal(path));
        }
        id_=id;
        approximate_size_=0;
    }
    ~MemTable() = default;

    MemTable(const MemTable& other) {
        std::shared_lock lock(other.mutex);
        map = other.map;
        wal = other.wal;
        id_ = other.id_;
        approximate_size_ = other.approximate_size_.load();
    }

    uint64_t get_max_ts(){
        mutex.lock_shared();
        auto iter = map->create_iterator();
        uint64_t ts = 0 ;
        while (iter->is_valid())
        {
            ts = std::max(ts,iter->get_key().ts);
            iter->next();
        }
        mutex.unlock_shared();
        delete iter;
        return ts;
    }

    Value get(const LsmKey& key){
        mutex.lock_shared();
        try
        {
            auto res=map->get(key);
            mutex.unlock_shared();
            return res;
        }
        catch(const std::exception& e)
        {
            mutex.unlock_shared();
            return Value();
        }
    }
    
    bool put(const LsmKey& key,Value value){
        std::pair<Key,Value> pair(key,value);
        std::vector<std::pair<Key,Value>> pairs;
        pairs.push_back(pair);
        return put_batch(pairs);
    }

    bool put_batch(const std::vector<std::pair<Key,Value>> &pairs){
        mutex.lock();
        uint64_t size = 0;
        for(auto &pair:pairs){

            size+=pair.first.user_key.length()+sizeof(uint64_t);
            size+=pair.second.size();
            map->put(pair.first,pair.second);
        }
        mutex.unlock();
        approximate_size_.fetch_add(size,std::memory_order::memory_order_relaxed);
        if(wal){
            wal->put_batch(pairs);
        }
        return true;
    }

    Iterators* scan(const Bound<Key>& lower,const Bound<Key>& upper){
        return new MemTableIterator(map.get(),lower,upper);
    }

    Iterators* scan(){
        return new MemTableIterator(map.get());
    }

    void sync_wal(){
        if(wal)wal->sync();
    }

    bool flush(TableBuilder* builder){
        mutex.lock_shared();
        auto iter = map->create_iterator();
        while (iter->is_valid())
        {
            builder->add(iter->get_key(),iter->get_value());
            iter->next();
        }
        mutex.unlock_shared();
        delete iter;
        return true;
    }

    uint64_t id(){return id_;}
    uint64_t approximate_size() {return approximate_size_.load(std::memory_order::memory_order_relaxed);}
    bool is_empty(){
        return map->is_empty();
    }
};





