#include "mvcc.h"
#include "../lsm_tree.h"
#include "../iters/two_merge_iterators.h"

Value Transaction::get(std::string_view key)
{
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    if(serializable_){
        std::unique_lock<std::shared_mutex> guard{lock}; // 写锁
        read_set.insert(Fingerprint32(key.data(),key.size()));
    }
    std::shared_lock<std::shared_mutex> guard{lock};
    try
    {
        auto it = local_storage->get(key);
        return it;
    }
    catch(const std::exception& e)
    {
        return inner_->get_with_ts(key,read_ts_);
    }
}

std::unique_ptr<Iterators> Transaction::scan(const Bound<std::string_view>&lower,const Bound<std::string_view>&upper)
{
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    auto local_iter = std::make_unique<TxnLocalIterator>(local_storage.get(),lower,upper);
    return std::make_unique<TxnIterator>(this,std::make_unique<TwoMergeIterator>(
        std::move(local_iter),inner_->scan_with_ts(lower,upper,read_ts_)
    ));
}

std::unique_ptr<Iterators> Transaction::scan_out(const Bound<std::string_view>&lower
    ,const Bound<std::string_view>&upper)
{
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    return std::make_unique<TxnLocalIterator>(local_storage.get(),lower,upper);
}

void Transaction::put(std::string_view key,Value val)
{
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    std::unique_lock<std::shared_mutex> guard{lock};
    write_set.insert(Fingerprint32(key.data(),key.size()));
    local_storage->put(key,val);
}
void Transaction::delete_(std::string_view key)
{
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    std::unique_lock<std::shared_mutex> guard{lock};
    write_set.insert(Fingerprint32(key.data(),key.size()));
    local_storage->put(key,Value());
}
bool Transaction::commit()
{
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    auto mvcc = inner_->mvcc();
    std::unique_lock<std::mutex> guard{mvcc->commit_lock};
    std::unique_lock<std::shared_mutex> guard1{lock};
    bool serializability_check=serializable_;
    {
        
        if(write_set.size()!=0){
            std::shared_lock<std::shared_mutex> guard{mvcc->mutex_};
            //read commit txn data
            auto start_it = mvcc->committed_txns.upper_bound(read_ts_);
            for(auto iter = start_it; iter != mvcc->committed_txns.end(); ++iter){
                // for(auto iter=read_set.begin();iter!=read_set.end();++iter){
                //     if(mvcc->committed_txns[i].key_hashes.count(*iter)){
                //         return false; // dead lock need rollback
                //     }
                // }
                CommittedTxnData& txn_data = iter->second;
                for (auto key_hash : read_set) {
                if (txn_data.key_hashes.count(key_hash)) {
                    // 冲突
                    return false;
                }
            }
            }
        }
    }
    std::vector<WriteBatchRecord> batch;
    {
        auto iter = local_storage->create_iterator();
        while (iter->is_valid())
        {
            if(!iter->get_value().is_empty()){
                batch.push_back(WriteBatchRecord{1,Key(iter->get_key(),read_ts_),iter->get_value()});
            }else{
                batch.push_back(WriteBatchRecord{0,Key(iter->get_key(),read_ts_),Value()});
            }
            iter->next();
        } 
    }

    auto ts = inner_->write_batch_inner(batch);
    if(serializability_check){
        auto water_mark = mvcc->watermark();
        std::unique_lock<std::shared_mutex> guard{mvcc->mutex_};
        CommittedTxnData commit_data;
        commit_data.read_ts = read_ts_;
        commit_data.commit_ts = ts;
        commit_data.key_hashes=write_set;
        mvcc->committed_txns[ts]=commit_data;
        auto iter = mvcc->committed_txns.begin();
        while (iter!=mvcc->committed_txns.end())
        {
            if(iter->first<water_mark){
                iter = mvcc->committed_txns.erase(iter);
            }else{
                break;
            }
        }
    }

    committed.store(true, std::memory_order_release);
    return true;
}


Transaction::~Transaction()
{
    auto mvcc = inner_->mvcc();
    mvcc->mutex_.lock();
    mvcc->marks.remove_reader(read_ts_);
    mvcc->mutex_.unlock();
}

void TxnIterator::add_to_read_set(const Key&key){
    std::unique_lock<std::shared_mutex> guard{txn->lock};
    txn->read_set.insert(Fingerprint32(key.user_key.data(),key.user_key.size()));
}