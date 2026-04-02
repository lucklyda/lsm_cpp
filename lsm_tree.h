#pragma once
#include "mem_table.h"
#include "mvcc/mvcc.h"
#include "revocery/manifest.h"

#include <unordered_map>
#include <memory>
#include <thread>
#include <chrono>
#include <optional>
#include <condition_variable>

struct WriteBatchRecord
{
    uint8_t type; //0 is delete;1 is insert
    LsmKey key;
    Value value;
};


struct LsmStorageOptions
{
   usize block_size;
   usize target_sst_size;
   usize num_memtable_limit;
   std::shared_ptr<CompactionOptions> compaction_options;
   bool enable_wal;
   bool serializable;
};

struct LsmStorageState
{
   std::shared_ptr<MemTable> memtable;
   std::vector<std::shared_ptr<MemTable>> imm_memtables;
   std::vector<usize> l0_sstables;
   std::vector<LevelItem> levels; //l1-ln
   std::unordered_map<usize,std::shared_ptr<Sstable>> sstables;

   LsmStorageState(const LsmStorageOptions& options);
};

struct CompactionFilter {
    std::string prefix;
};

class LsmStorageInner:public std::enable_shared_from_this<LsmStorageInner>
{
friend class Transaction;
friend class LsmTree;
private:
    /*     
    pub(crate) block_cache: Arc<BlockCache>,*/
    std::shared_ptr<const LsmStorageState> state;
    std::shared_mutex state_lock;
    std::mutex write_mutex_; 
    std::string path;

    std::atomic<uint64_t> next_sst_id_;
    LsmStorageOptions options;
    std::shared_ptr<CompactionController> compaction_controller;
    std::shared_ptr<Manifest> manifest_;
    
    std::shared_ptr<LsmMvccInner> mvcc_;
    std::vector<CompactionFilter>  compaction_filters;
    
public:
    LsmStorageInner(std::string path_,LsmStorageOptions options_);
    ~LsmStorageInner();

public:
    uint64_t next_sst_id(){
        return next_sst_id_.fetch_add(1,std::memory_order::memory_order_relaxed);
    }

    LsmMvccInner *mvcc(){
        return mvcc_.get();
    }

    bool sync(){
        state->memtable->sync_wal();
        return true;
    }

    void add_compaction_filter(CompactionFilter compaction_filter){
        compaction_filters.push_back(compaction_filter);
    }

    Value get(std::string_view key);

    bool write_batch(const std::vector<WriteBatchRecord>& batch);

    bool put(std::string_view key,Value value);

    bool delete_(std::string_view key);

    std::unique_ptr<Transaction> new_txn();

    std::unique_ptr<Iterators> scan(const Bound<std::string_view>&lower,const Bound<std::string_view>&upper);

    std::optional<std::thread> spawn_compaction_thread(
        std::condition_variable& stop_cv,
        std::mutex& stop_mtx,
        bool& stop_flag
    );

    std::optional<std::thread> spawn_flush_thread(
        std::condition_variable& stop_cv,
        std::mutex& stop_mtx,
        bool& stop_flag
    );

    std::vector<std::shared_ptr<Sstable>> compact(CompactionTask &task);

    bool force_full_compaction();

    void trigger_compaction();
    void trigger_flush();

public:
    static std::string path_of_sst_static(const char*dir,uint64_t id){
        char filename[64];
        std::snprintf(filename, sizeof(filename), "%05llu.sst", (unsigned long long)id);

        std::string path = dir;
        if (!path.empty() && path.back() != '/') {
            path += '/';
        }
        path += filename;

        return path;
    }

    std::string path_of_sst(uint64_t id){
        return LsmStorageInner::path_of_sst_static(path.c_str(),id);
    }

    static std::string path_of_wal_static(const char*dir,uint64_t id){
        char filename[64];
        std::snprintf(filename, sizeof(filename), "%05llu.wal", (unsigned long long)id);

        std::string path = dir;
        if (!path.empty() && path.back() != '/') {
            path += '/';
        }
        path += filename;

        return path;
    }

    std::string path_of_wal(uint64_t id){
        return LsmStorageInner::path_of_wal_static(path.c_str(),id);
    }

    static std::string path_of_manifest_static(const char*dir){
        char filename[64];
        std::snprintf(filename, sizeof(filename), "manifest");

        std::string path = dir;
        if (!path.empty() && path.back() != '/') {
            path += '/';
        }
        path += filename;

        return path;
    }

    std::string path_of_manifest(){
        return LsmStorageInner::path_of_manifest_static(path.c_str());
    }

    void sync_dir(){
        int fd = ::open(path.c_str(), O_DIRECTORY | O_RDONLY);
        if (fd == -1) throw std::runtime_error("open dir failed");
        if (::fsync(fd) == -1) {
            ::close(fd);
            throw std::runtime_error("fsync dir failed");
        }
        ::close(fd);
    }

    bool key_within(std::string_view user_key,const LsmKey&table_begin,const LsmKey&table_end);

private:
    std::shared_ptr<const LsmStorageState> get_snapshot() {
        std::shared_lock<std::shared_mutex> lock(state_lock);
        return state;
    }

    void update_state(std::shared_ptr<LsmStorageState> new_state) {
        std::unique_lock<std::shared_mutex> lock(state_lock);
        state = std::move(new_state);
    }

    void update_state(std::unique_ptr<LsmStorageState> new_state) {
        std::unique_lock<std::shared_mutex> lock(state_lock);
        state = std::move(new_state);
    }

    Value get_with_ts(std::string_view key,uint64_t ts);
    uint64_t write_batch_inner(const std::vector<WriteBatchRecord>& batch);
    bool freeze_memtable_with_memtable(std::unique_ptr<MemTable>);
    bool force_freeze_memtable();
    bool force_flush_next_imm_memtable();
    std::unique_ptr<SsTableIterator> create_sst_iterator_with_end(Sstable*table,
        const LsmKey&lower,const LsmKey&upper);
    std::unique_ptr<Iterators> scan_with_ts(const Bound<std::string_view>&lower,const Bound<std::string_view>&upper,uint64_t ts);

    std::vector<std::shared_ptr<Sstable>> compact_generate_sst_from_iter(std::unique_ptr<Iterators> iter);

};

class LsmTree
{
private:
    /* data */
    std::shared_ptr<LsmStorageInner> inner;

    std::condition_variable flush_stop_cv_;   
    std::mutex flush_mtx_;                    
    bool flush_stop_flag_ = false;            
    std::optional<std::thread> flush_thread_;

    // ==== Compaction Thread ====
    std::condition_variable compaction_stop_cv_;  
    std::mutex compaction_mtx_;                  
    bool compaction_stop_flag_ = false;        
    std::optional<std::thread> compaction_thread_;

public:
    LsmTree(std::string path,LsmStorageOptions options);
    ~LsmTree();

    std::unique_ptr<Transaction> new_txn(){
        return inner->new_txn();
    }
    bool write_batch(const std::vector<WriteBatchRecord>&batch){
        return inner->write_batch(batch);
    }
    void add_compaction_filter(CompactionFilter filter){
        inner->add_compaction_filter(filter);
    }
    Value get(std::string_view key){
        return inner->get(key);
    }
    bool put(std::string_view key,Value val){
        return inner->put(key,val);
    }

    bool delete_(std::string_view key){
        return inner->delete_(key);
    }
    void sync(){
        inner->sync();
    }
    std::unique_ptr<Iterators> scan(const Bound<std::string_view> &lower,const Bound<std::string_view> &upper)
    {
        return inner->scan(lower,upper);
    }
};








