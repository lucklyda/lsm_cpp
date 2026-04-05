#include "mvcc.h"
#include "../lsm_tree.h"
#include "../iters/two_merge_iterators.h"

std::optional<std::string> Transaction::get(std::string_view key) {
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    if (serializable_) {
        std::unique_lock<std::shared_mutex> wguard{lock};
        read_set.insert(Fingerprint32(key.data(), key.size()));
    }
    std::shared_lock<std::shared_mutex> guard{lock};
    try {
        const std::string& v = local_storage->get(std::string(key));
        if (v.empty()) {
            return std::nullopt;
        }
        return v;
    } catch (const std::exception&) {
        return inner_->get_with_ts(key, read_ts_);
    }
}

std::unique_ptr<Iterators> Transaction::scan(const Bound<std::string_view>& lower,
                                             const Bound<std::string_view>& upper) {
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    auto local_iter = std::make_unique<TxnLocalIterator>(local_storage.get(), lower, upper);
    return std::make_unique<TxnIterator>(
        this, std::make_unique<TwoMergeIterator>(std::move(local_iter), inner_->scan_with_ts(lower, upper, read_ts_)));
}

std::unique_ptr<Iterators> Transaction::scan_out(const Bound<std::string_view>& lower,
                                                   const Bound<std::string_view>& upper) {
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    return std::make_unique<TxnLocalIterator>(local_storage.get(), lower, upper);
}

void Transaction::put(std::string_view key, std::string_view val) {
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    std::unique_lock<std::shared_mutex> guard{lock};
    write_set.insert(Fingerprint32(key.data(), key.size()));
    local_storage->put(std::string(key), std::string(val));
}

void Transaction::delete_(std::string_view key) {
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    std::unique_lock<std::shared_mutex> guard{lock};
    write_set.insert(Fingerprint32(key.data(), key.size()));
    local_storage->put(std::string(key), std::string());
}

bool Transaction::commit() {
    if (committed.load(std::memory_order_acquire)) {
        throw std::logic_error("cannot operate on committed txn!");
    }
    auto mvcc = inner_->mvcc();
    std::unique_lock<std::mutex> guard{mvcc->commit_lock};
    std::unique_lock<std::shared_mutex> guard1{lock};
    bool serializability_check = serializable_;
    {

        if (write_set.size() != 0) {
            std::shared_lock<std::shared_mutex> rguard{mvcc->mutex_};
            auto start_it = mvcc->committed_txns.upper_bound(read_ts_);
            for (auto iter = start_it; iter != mvcc->committed_txns.end(); ++iter) {
                CommittedTxnData& txn_data = iter->second;
                for (auto key_hash : read_set) {
                    if (txn_data.key_hashes.count(key_hash)) {
                        return false;
                    }
                }
            }
        }
    }
    std::vector<WriteBatchRecord> batch;
    {
        auto iter = local_storage->create_iterator();
        while (iter->is_valid()) {
            const std::string& k = iter->get_key();
            const std::string& v = iter->get_value();
            if (!v.empty()) {
                batch.push_back(WriteBatchRecord{1, k, v});
            } else {
                batch.push_back(WriteBatchRecord{0, k, std::string()});
            }
            iter->next();
        }
        delete iter;
    }

    auto ts = inner_->write_batch_inner(batch);
    if (serializability_check) {
        auto water_mark = mvcc->watermark();
        std::unique_lock<std::shared_mutex> wguard{mvcc->mutex_};
        CommittedTxnData commit_data;
        commit_data.read_ts = read_ts_;
        commit_data.commit_ts = ts;
        commit_data.key_hashes = write_set;
        mvcc->committed_txns[ts] = commit_data;
        auto iter = mvcc->committed_txns.begin();
        while (iter != mvcc->committed_txns.end()) {
            if (iter->first < water_mark) {
                iter = mvcc->committed_txns.erase(iter);
            } else {
                break;
            }
        }
    }

    committed.store(true, std::memory_order_release);
    return true;
}

Transaction::~Transaction() {
    auto mvcc = inner_->mvcc();
    mvcc->mutex_.lock();
    mvcc->marks.remove_reader(read_ts_);
    mvcc->mutex_.unlock();
}

void TxnIterator::add_to_read_set(LsmKeyView kv) {
    std::unique_lock<std::shared_mutex> guard{txn->lock};
    txn->read_set.insert(Fingerprint32(kv.user_key.data(), kv.user_key.size()));
}
