#pragma once
#include "revocery/wal.h"
#include "table/table_builder.h"
#include "deps/arena/arena.h"
#include <shared_mutex>
#include <atomic>

class MemTableIterator : public Iterators {
private:
    mmstore<MemLsmKey, MemLsmValue>* map_;
    mmstore_iterator<MemLsmKey, MemLsmValue>* iter;

public:
    MemTableIterator(mmstore<MemLsmKey, MemLsmValue>* map, const Bound<MemLsmKey>& lower,
                     const Bound<MemLsmKey>& upper) {
        map_ = map;
        iter = map_->create_iterator(lower, upper);
    }

    MemTableIterator(mmstore<MemLsmKey, MemLsmValue>* map) {
        map_ = map;
        iter = map_->create_iterator();
    }

    ~MemTableIterator() { delete iter; }

    LsmKeyView key_view() const override {
        if (iter->is_valid()) {
            return iter->get_key().view();
        }
        return {};
    }

    std::string_view value_view() const override {
        if (iter->is_valid()) {
            return iter->get_value().view();
        }
        return {};
    }

    bool is_valid() override { return iter->is_valid(); }

    bool next() override {
        iter->next();
        return true;
    }

    uint32_t num_active_iterators() override { return is_valid() ? 1 : 0; }
};

class MemTable {
private:
    std::shared_ptr<Arena> arena_;
    std::shared_ptr<Map<MemLsmKey, MemLsmValue>> map;
    std::shared_ptr<Wal> wal;
    uint64_t id_;
    std::atomic<uint64_t> approximate_size_;
    mutable std::shared_mutex mutex;

public:
    explicit MemTable(uint64_t id) : arena_(std::make_shared<Arena>()), id_(id), approximate_size_(0) {
        map = std::make_shared<Map<MemLsmKey, MemLsmValue>>();
        wal = nullptr;
    }

    MemTable(uint64_t id, const char* path, bool recovery)
        : arena_(std::make_shared<Arena>()), id_(id), approximate_size_(0) {
        map = std::make_shared<Map<MemLsmKey, MemLsmValue>>();
        if (recovery) {
            wal = std::shared_ptr<Wal>(Wal::recover(path, [this](std::vector<std::pair<Key, Value>> batch) {
                put_batch_owned(std::move(batch));
            }));
            if (wal == nullptr) {
                throw std::logic_error("Recovery Error!");
            }
        } else {
            wal = std::shared_ptr<Wal>(new Wal(path));
        }
    }

    ~MemTable() = default;

    MemTable(const MemTable& other) {
        std::shared_lock lock(other.mutex);
        arena_ = other.arena_;
        map = other.map;
        wal = other.wal;
        id_ = other.id_;
        approximate_size_.store(other.approximate_size_.load());
    }

    uint64_t get_max_ts() {
        mutex.lock_shared();
        auto iter = map->create_iterator();
        uint64_t ts = 0;
        while (iter->is_valid()) {
            ts = std::max(ts, iter->get_key().ts);
            iter->next();
        }
        mutex.unlock_shared();
        delete iter;
        return ts;
    }

    bool put(std::string_view user_key, uint64_t ts, std::string_view value_bytes) {
        char* kcopy = arena_->AllocateCopy(user_key.data(), user_key.size());
        MemLsmKey mkey(StringRef(kcopy, user_key.size()), ts);
        MemLsmValue mval;
        if (!value_bytes.empty()) {
            char* vcopy = arena_->AllocateCopy(value_bytes.data(), value_bytes.size());
            mval = MemLsmValue(StringRef(vcopy, value_bytes.size()));
        }
        std::vector<std::pair<Key, Value>> wal_batch;
        wal_batch.emplace_back(Key(std::string(user_key), ts),
                               value_bytes.empty() ? Value() : Value(std::string(value_bytes)));
        return put_impl(std::move(mkey), std::move(mval), wal ? &wal_batch : nullptr);
    }

    bool put(const Key& key, const Value& value) {
        return put(std::string_view(key.user_key), key.ts,
                   std::string_view(value.value.data(), value.value.size()));
    }

    Value get_lookup(std::string_view user_key, uint64_t ts) {
        MemLsmKey probe(StringRef(user_key.data(), user_key.size()), ts);
        mutex.lock_shared();
        try {
            const MemLsmValue& mv = map->get(probe);
            mutex.unlock_shared();
            if (mv.is_empty()) {
                return Value();
            }
            auto v = mv.view();
            return Value(std::string(v.data(), v.size()));
        } catch (const std::exception&) {
            mutex.unlock_shared();
            return Value();
        }
    }

    bool put_batch_owned(const std::vector<std::pair<Key, Value>>& pairs) {
        mutex.lock();
        uint64_t size = 0;
        for (const auto& pair : pairs) {
            size += pair.first.user_key.length() + sizeof(uint64_t);
            size += pair.second.value.size();
            char* kcopy = arena_->AllocateCopy(pair.first.user_key.data(), pair.first.user_key.size());
            MemLsmKey mkey(StringRef(kcopy, pair.first.user_key.size()), pair.first.ts);
            MemLsmValue mval;
            if (!pair.second.value.empty()) {
                char* vcopy =
                    arena_->AllocateCopy(pair.second.value.data(), pair.second.value.size());
                mval = MemLsmValue(StringRef(vcopy, pair.second.value.size()));
            }
            map->put(mkey, mval);
        }
        mutex.unlock();
        approximate_size_.fetch_add(size, std::memory_order_relaxed);
        if (wal) {
            wal->put_batch(pairs);
        }
        return true;
    }

    Iterators* scan(const Bound<Key>& lower, const Bound<Key>& upper) {
        Bound<MemLsmKey> lo;
        Bound<MemLsmKey> hi;
        lo.type = lower.type;
        hi.type = upper.type;
        if (lower.type != 0) {
            lo.key = MemLsmKey(StringRef(lower.key.user_key.data(), lower.key.user_key.size()),
                               lower.key.ts);
        }
        if (upper.type != 0) {
            hi.key = MemLsmKey(StringRef(upper.key.user_key.data(), upper.key.user_key.size()),
                               upper.key.ts);
        }
        return new MemTableIterator(map.get(), lo, hi);
    }

    Iterators* scan() { return new MemTableIterator(map.get()); }

    void sync_wal() {
        if (wal) {
            wal->sync();
        }
    }

    bool flush(TableBuilder* builder) {
        mutex.lock_shared();
        auto iter = map->create_iterator();
        while (iter->is_valid()) {
            auto kv = iter->get_key().view();
            auto vv = iter->get_value().view();
            Key ok(std::string(kv.user_key), kv.ts);
            Value ov(std::string(vv.data(), vv.size()));
            builder->add(ok, ov);
            iter->next();
        }
        mutex.unlock_shared();
        delete iter;
        return true;
    }

    uint64_t id() { return id_; }
    uint64_t approximate_size() { return approximate_size_.load(std::memory_order_relaxed); }
    bool is_empty() { return map->is_empty(); }

private:
    bool put_impl(MemLsmKey mkey, MemLsmValue mval, const std::vector<std::pair<Key, Value>>* wal_batch) {
        mutex.lock();
        uint64_t size = mkey.user_key.length() + sizeof(uint64_t) + mval.size();
        map->put(mkey, mval);
        mutex.unlock();
        approximate_size_.fetch_add(size, std::memory_order_relaxed);
        if (wal && wal_batch) {
            wal->put_batch(*wal_batch);
        }
        return true;
    }
};
