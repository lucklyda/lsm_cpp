#pragma once
#include <unordered_set>
#include <shared_mutex>
#include <memory>
#include <mutex>
#include <atomic>

#include "../iterators.h"
#include "watermark.h"
#include "../deps/mmstore/skipmap.h"
#include "../deps/mmstore/map.h"
#include "../key.h"

class LsmStorageInner;
class Transaction
{
friend class TxnIterator;
private:
    std::shared_mutex lock;
    uint64_t read_ts_;
    LsmStorageInner *inner_;
    std::unique_ptr<mmstore<std::string_view,Value>> local_storage;
    std::atomic<bool> committed{false};
    std::unordered_set<uint32_t> write_set;
    std::unordered_set<uint32_t> read_set;
    bool serializable_;
public:
    Transaction(uint64_t read_ts,LsmStorageInner *inner,bool serializable){
        read_ts_ = read_ts;
        inner_ = inner;
        serializable_ = serializable;
        local_storage = std::make_unique<Map<std::string_view,Value>>();
    }
    Value get(std::string_view);

    std::unique_ptr<Iterators> scan(const Bound<std::string_view>&lower,const Bound<std::string_view>&upper);

    std::unique_ptr<Iterators> scan_out(const Bound<std::string_view>&lower,const Bound<std::string_view>&upper);
    void put(std::string_view,Value);
    void delete_(std::string_view);

    /**
     * @note:如果返回false,说明串行检测不通过
     */
    bool commit();

    uint64_t read_ts(){ return read_ts_;}


    ~Transaction();
};

struct CommittedTxnData
{
    std::unordered_set<uint32_t> key_hashes;
    uint64_t read_ts;
    uint64_t commit_ts;
};

class LsmMvccInner
{
    friend class Transaction;
private:
    std::shared_mutex mutex_;
    std::mutex commit_lock;
    uint64_t ts;
    Watermark marks;
    std::map<uint64_t,CommittedTxnData> committed_txns;

public:
    LsmMvccInner(uint64_t initial_ts){
        ts = initial_ts;
    }
    uint64_t latest_commit_ts(){
        std::shared_lock<std::shared_mutex> lock{mutex_};
        return ts;
    }
    void update_commit_ts(uint64_t ts_){
        std::unique_lock<std::shared_mutex> lock(mutex_);
        ts = ts_;
    }

    uint64_t watermark(){
        std::shared_lock<std::shared_mutex> lock{mutex_};
        return marks.watermark().value_or(ts);
    }
    std::unique_ptr<Transaction> new_txn(LsmStorageInner* inner,bool serializable){
        std::unique_lock<std::shared_mutex> lock(mutex_);
        marks.add_reader(ts);
        return std::make_unique<Transaction>(ts,inner,serializable);
    }
    ~LsmMvccInner()=default;
};


class TxnLocalIterator:public Iterators
{
private:
    mmstore<std::string_view,Value> *map_;
    mmstore_iterator<std::string_view,Value> *iter;
private:
public:
    TxnLocalIterator(mmstore<std::string_view,Value> *map,const Bound<std::string_view>& lower,const Bound<std::string_view>& upper)
    {
        map_ = map;
        iter = map_->create_iterator(lower,upper);
    }
    TxnLocalIterator(mmstore<std::string_view,Value> *map){
        map_ = map;
        iter = map_->create_iterator();
    }
    ~TxnLocalIterator(){
        delete iter;
    }

    const Key& key()const override{
        if(iter->is_valid()){
            static Key return_key{};
            return_key.user_key = iter->get_key();
            return return_key;

        }else{
            static const Key empty_key{};
            return empty_key;
        }
    }

    Value value()const override{
        if(iter->is_valid()){
            return iter->get_value();
        }else{
            return nullptr;
        }
    }

    bool is_valid()override{
        return iter->is_valid();
    }

    bool next() override{
        iter->next();
        return true;
    }

    uint32_t num_active_iterators() override{
        return is_valid()?1:0;
    }
};

class TxnIterator:public Iterators
{
private:
    Transaction *txn;
    std::unique_ptr<Iterators> iter;
    bool own_txn_;
private:
    void skip_deletes(){
        while (iter->is_valid() && iter->value().is_empty())
        {
            iter->next();
        }
    }

    void add_to_read_set(const LsmKey&key);

public:
    TxnIterator(Transaction* txn_,std::unique_ptr<Iterators> iter_,bool own_txn=false){
        txn = txn_;
        own_txn_=own_txn;
        iter = std::move(iter_);
        skip_deletes();
        if(is_valid())add_to_read_set(key());
    }
    ~TxnIterator(){
        if(own_txn_)delete txn;
    }

    const Key& key()const override{
        if(iter->is_valid()){
            return iter->key();
        }else{
            static const Key empty_key{};
            return empty_key;
        }
    }

    Value value()const override{
        if(iter->is_valid()){
            return iter->value();
        }else{
            return nullptr;
        }
    }

    bool is_valid()override{
        return iter->is_valid();
    }

    bool next() override{
        iter->next();
        skip_deletes();
        if(is_valid())add_to_read_set(key());
        return true;
    }

    uint32_t num_active_iterators() override{
        return iter->num_active_iterators();
    }
};







