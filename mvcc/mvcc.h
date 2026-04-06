#pragma once
#include <unordered_set>
#include <shared_mutex>
#include <memory>
#include <mutex>
#include <atomic>
#include <optional>
#include <string>
#include <string_view>

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
    std::unique_ptr<Map<std::string, std::string>> local_storage;
    std::atomic<bool> committed{false};
    std::unordered_set<uint32_t> write_set;
    std::unordered_set<uint32_t> read_set;
    bool serializable_;
public:
    Transaction(uint64_t read_ts,LsmStorageInner *inner,bool serializable){
        read_ts_ = read_ts;
        inner_ = inner;
        serializable_ = serializable;
        local_storage = std::make_unique<Map<std::string, std::string>>();
    }
    std::optional<std::string> get(std::string_view key);

    std::unique_ptr<Iterators> scan(const Bound<std::string_view>&lower,const Bound<std::string_view>&upper);

    std::unique_ptr<Iterators> scan_out(const Bound<std::string_view>&lower,const Bound<std::string_view>&upper);
    void put(std::string_view key, std::string_view value);
    //void put(std::string_view key, const std::string& value) { put(key, std::string_view(value)); }
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


class TxnLocalIterator : public Iterators {
private:
    Map<std::string, std::string>* map_;
    mmstore_iterator<std::string, std::string>* iter;
private:
public:
    TxnLocalIterator(Map<std::string, std::string>* map, const Bound<std::string_view>& lower,
                     const Bound<std::string_view>& upper) {
        map_ = map;
        Bound<std::string> lo;
        Bound<std::string> hi;
        lo.type = lower.type;
        hi.type = upper.type;
        if (lower.type != 0) {
            lo.key = std::string(lower.key);
        }
        if (upper.type != 0) {
            hi.key = std::string(upper.key);
        }
        iter = map_->create_iterator(lo, hi);
    }
    TxnLocalIterator(Map<std::string, std::string>* map) {
        map_ = map;
        iter = map_->create_iterator();
    }
    ~TxnLocalIterator(){
        delete iter;
    }

    LsmKeyView key_view() const override {
        if (iter->is_valid()) {
            const std::string& k = iter->get_key();
            return {std::string_view(k), 0};
        }
        return {};
    }

    std::string_view value_view() const override {
        if (iter->is_valid()) {
            return std::string_view(iter->get_value());
        }
        return {};
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
    void skip_deletes() {
        while (iter->is_valid() && iter->value_view().empty()) {
            iter->next();
        }
    }

    void add_to_read_set(LsmKeyView kv);

public:
    TxnIterator(Transaction* txn_,std::unique_ptr<Iterators> iter_,bool own_txn=false){
        txn = txn_;
        own_txn_=own_txn;
        iter = std::move(iter_);
        skip_deletes();
        if (is_valid()) {
            add_to_read_set(key_view());
        }
    }
    ~TxnIterator(){
        if(own_txn_)delete txn;
    }

    LsmKeyView key_view() const override {
        if (iter->is_valid()) {
            return iter->key_view();
        }
        return {};
    }

    std::string_view value_view() const override {
        if (iter->is_valid()) {
            return iter->value_view();
        }
        return {};
    }

    bool is_valid()override{
        return iter->is_valid();
    }

    bool next() override{
        iter->next();
        skip_deletes();
        if (is_valid()) {
            add_to_read_set(key_view());
        }
        return true;
    }

    uint32_t num_active_iterators() override{
        return iter->num_active_iterators();
    }
};







