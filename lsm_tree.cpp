#include "lsm_tree.h"
#include "table/table.h"
#include "iters/two_merge_iterators.h"
#include "iters/merge_iterators.h"
#include "iters/concat_iterator.h"
#include "iters/lsm_iterators.h"
#include "table/table_builder.h"
#include "compact/compact.h"

#include <filesystem> 

std::pair<Bound<LsmKey>,Bound<LsmKey>> 
map_key_bound_plus_ts(const Bound<std::string_view>&lower,
        const Bound<std::string_view>&upper,uint64_t ts)
{
    Bound<LsmKey> lower_;
    Bound<LsmKey> upper_;
    switch (lower.type)
    {
    case 0:{
        lower_.type = 0;
    }break;
    case 1:{
        lower_.type = 1;
        lower_.key = LsmKey(lower.key,ts);
    }break;
    case 2:{
        lower_.type = 2;
        lower_.key = LsmKey(lower.key,TS_END);
    }break;
    default:
        break;
    }

    switch (upper.type)
    {
    case 0:{
        upper_.type = 0;
    }break;
    case 1:{
        upper_.type = 1;
        upper_.key = LsmKey(upper.key,TS_END);
    }break;
    case 2:{
        upper_.type = 2;
        upper_.key = LsmKey(upper.key,TS_BEGIN);
    }break;
    default:
        break;
    }

    return {lower_,upper_};
}



std::pair<Bound<LsmKey>, Bound<LsmKey>>
map_key_bound_plus_ts1(const Bound<std::string_view>& lower,
                      const Bound<std::string_view>& upper,
                      uint64_t read_ts)
{
    Bound<LsmKey> lower_;
    Bound<LsmKey> upper_;

    // --------------------------
    // 处理 lower 下界
    // --------------------------
    switch (lower.type)
    {
    case 0: // Unbounded
        lower_.type = 0;
        break;
    case 1: { // Included → 直接使用
        lower_.type = 1;
        lower_.key = LsmKey(lower.key, TS_BEGIN);
        break;
    }
    case 2: { // Excluded → key + \x00
        std::string buf(lower.key);
        buf.push_back('\x00');
        lower_.type = 1; // 变成 Included
        lower_.key = LsmKey(buf, TS_BEGIN);
        break;
    }
    default:
        break;
    }

    // --------------------------
    // 处理 upper 上界
    // --------------------------
    switch (upper.type)
    {
    case 0: // Unbounded
        upper_.type = 0;
        break;
    case 1: { // Included → 直接使用
        upper_.type = 1;
        upper_.key = LsmKey(upper.key, read_ts);
        break;
    }
    case 2: { // Excluded → key + \x00
        std::string buf(upper.key);
        buf.push_back('\x00');
        upper_.type = 1; // 变成 Included
        upper_.key = LsmKey(buf, read_ts);
        break;
    }
    default:
        break;
    }

    return {lower_, upper_};
}


bool range_overlap(const Bound<std::string_view>& lower,const Bound<std::string_view>& upper
                    ,const LsmKey&table_begin,const LsmKey&table_end)
{
    if(lower.type==1){
        if(lower.key>table_end.user_key)return false;
    }else if(lower.type==2){
        if(lower.key>=table_end.user_key)return false;
    }

    if(upper.type==1){
        if(upper.key<table_begin.user_key)return false;
    }else if(upper.type==2){
        if(upper.key<=table_begin.user_key)return false;
    }
    return true;
}

LsmStorageState::LsmStorageState(const LsmStorageOptions& options){
    if (const auto* opt = dynamic_cast<const LeveledCompactionOptions*>(options.compaction_options.get())) {
        int max_levels = opt->max_levels;
        for (int level = 1; level <= max_levels; ++level) {
            LevelItem item;
            item.level_num = level;
            levels.emplace_back(item);
        }
    }else if(dynamic_cast<const TieredCompactionOptions*>(options.compaction_options.get())){
        
    }else if(const auto* opt = dynamic_cast<const SimpleLeveledCompactionOptions*>(options.compaction_options.get())){
        int max_levels = opt->max_levels;
        for (int level = 1; level <= max_levels; ++level) {
            LevelItem item;
            item.level_num = level;
            levels.emplace_back(item);
        }
    }else{
        LevelItem item;
        item.level_num = 1;
        levels.emplace_back(item);
    }

    memtable = std::make_unique<MemTable>(0);
}

LsmStorageInner::LsmStorageInner(std::string path_,LsmStorageOptions options_)
{
    auto out_state = std::make_unique<LsmStorageState>(options_);
    compaction_controller = create_controller(*options_.compaction_options);
    if(!FileObject::create_dir(path_.c_str())){
        throw std::logic_error("Db Dit Error");
    }
    uint64_t next_sst_id = 1;
    uint64_t begin_ts = 0;
    std::string manifest_path = LsmStorageInner::path_of_manifest_static(path_.c_str());
    Manifest *manifest = nullptr;
    if(FileObject::check_file(manifest_path.c_str())){
        try
        {
            auto meta_list = Manifest::recover(manifest_path.c_str());
            manifest = meta_list.first;
            std::unordered_set<uint64_t> memtables;
            for(auto& record:meta_list.second){
                switch (record->type_)
                {
                case 1:{
                    auto new_mem = (NewMemtableRecord*)(record.get());
                    next_sst_id=std::max(next_sst_id,new_mem->id);
                    memtables.insert(new_mem->id);
                } break;
                case 2:{
                    auto flush_re = (FlushRecord*)(record.get());
                    auto it = memtables.find(flush_re->id);
                    if(it ==memtables.end()){
                        throw std::logic_error("Get Flush Sstable But Not Find");
                    }
                    memtables.erase(it);
                    if(compaction_controller->flush_to_l0()){
                        out_state->l0_sstables.insert(out_state->l0_sstables.begin(), flush_re->id);
                    }else{
                        LevelItem item;
                        item.level_num = flush_re->id;
                        item.sst_ids.push_back(flush_re->id);
                        out_state->levels.insert(out_state->levels.begin(),item);
                    }
                } break;
                case 3:{
                    auto compation_record=(CompactionRecord*)(record.get());
                    auto pair = compaction_controller->apply_compaction_result(*out_state.get(),
                    *compation_record->task_.get(),compation_record->output_,true);
                    for(auto &o:compation_record->output_){
                        next_sst_id=std::max(next_sst_id,o);
                    }

                } break;
                default:
                    break;
                }
            }

            for(auto l0_sst:out_state->l0_sstables){
                auto file =FileObject::open_inner(
                    LsmStorageInner::path_of_sst_static(path_.c_str(),l0_sst).c_str()
                );
                auto sstable = Sstable::open(l0_sst,std::unique_ptr<FileObject>(file));
                begin_ts = std::max(begin_ts,sstable->max_ts());
                out_state->sstables.emplace(l0_sst,sstable);
            }
            next_sst_id+=1;
            if(dynamic_cast<LeveledCompactionController*>(compaction_controller.get())){
                for(auto &sst:out_state->levels){
                    std::sort(sst.sst_ids.begin(),sst.sst_ids.end(),[&](usize x,usize y){
                        auto &ptr_x = out_state->sstables.at(x);
                        auto &ptr_y = out_state->sstables.at(y);
                        return ptr_x->first_key_()<ptr_y->first_key_();
                    });
                }
            }

            //revocery wal
            if(options_.enable_wal){
                for(auto it = memtables.begin();it!=memtables.end();++it){
                    auto memtable = new MemTable(*it,LsmStorageInner::path_of_wal_static(path_.c_str(),*it).c_str(),true);
                    begin_ts = std::max(begin_ts,memtable->get_max_ts());
                    if(!memtable->is_empty()){
                        out_state->imm_memtables.insert(out_state->imm_memtables.begin(),std::move(
                            std::shared_ptr<MemTable>(memtable)));
                    }
                }

                out_state->memtable = std::make_unique<MemTable>(next_sst_id,
                        LsmStorageInner::path_of_wal_static(path_.c_str(),next_sst_id).c_str(),false);
            }else{
                out_state->memtable = std::make_unique<MemTable>(next_sst_id);
            }
            manifest->add_record(NewMemtableRecord(next_sst_id));
            next_sst_id+=1;
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }else{
        if(options_.enable_wal){
            out_state->memtable = std::make_unique<MemTable>(out_state->memtable->id(),
            LsmStorageInner::path_of_wal_static(path_.c_str(),out_state->memtable->id()).c_str(),false);
        }
        manifest=new Manifest(manifest_path.c_str());
        manifest->add_record(NewMemtableRecord(out_state->memtable->id()));
    }

    update_state(std::move(out_state));
    path = path_;
    next_sst_id_ = next_sst_id;
    manifest_ = std::unique_ptr<Manifest>(manifest);
    options = options_;
    mvcc_ = std::make_unique<LsmMvccInner>(begin_ts);
}

LsmStorageInner::~LsmStorageInner(){

}


std::optional<std::thread> LsmStorageInner::spawn_compaction_thread(
        std::condition_variable& stop_cv,
        std::mutex& stop_mtx,
        bool& stop_flag
    )
{
    auto opt = options.compaction_options;
    if(opt->type==CompactionOptionsType::LeveledCmpact||
        opt->type==CompactionOptionsType::SimpleLeveledCmpact||
        opt->type==CompactionOptionsType::TiereCmpact)
    {
        std::shared_ptr<LsmStorageInner> this_shared = shared_from_this();
        std::thread compaction_thread([this_shared, 
            &stop_cv, &stop_mtx, &stop_flag]() {
                using namespace std::chrono;
                // 50ms tick 定时器
                const auto tick_interval = milliseconds(50);
                auto next_tick = steady_clock::now() + tick_interval;

                while (true) {
                    std::unique_lock<std::mutex> lock(stop_mtx);
                    bool stopped = stop_cv.wait_until(lock, next_tick, [&stop_flag]() {
                        return stop_flag;
                    });
                    if (stopped) {
                        // 收到停止信号 → 退出线程
                        return;
                    }
                    next_tick = steady_clock::now() + tick_interval;
                    try {
                        this_shared->trigger_compaction();
                    } catch (const std::exception& e) {
                        // 等价 eprintln!
                        std::cerr << "compaction failed: " << e.what() << std::endl;
                    }
                }
        });
        return compaction_thread;
    }
    return std::nullopt; 
}

std::optional<std::thread> LsmStorageInner::spawn_flush_thread(
        std::condition_variable& stop_cv,
        std::mutex& stop_mtx,
        bool& stop_flag
    )
{
    std::shared_ptr<LsmStorageInner> this_shared = shared_from_this();
    std::thread compaction_thread([this_shared, 
        &stop_cv, &stop_mtx, &stop_flag]() {
            using namespace std::chrono;
            // 50ms tick 定时器
            const auto tick_interval = milliseconds(50);
            auto next_tick = steady_clock::now() + tick_interval;

            while (true) {
                std::unique_lock<std::mutex> lock(stop_mtx);
                bool stopped = stop_cv.wait_until(lock, next_tick, [&stop_flag]() {
                    return stop_flag;
                });
                if (stopped) {
                    // 收到停止信号 → 退出线程
                    return;
                }
                next_tick = steady_clock::now() + tick_interval;
                try {
                    this_shared->trigger_flush();
                } catch (const std::exception& e) {
                    // 等价 eprintln!
                    std::cerr << "compaction failed: " << e.what() << std::endl;
                }
            }
    });
    return compaction_thread;
}

std::vector<std::shared_ptr<Sstable>> LsmStorageInner::compact(CompactionTask &task)
{
    auto snapshot = get_snapshot();
    if (const auto* o = dynamic_cast<const ForceFullCompactionTask*>(&task)) {
        std::vector<std::unique_ptr<Iterators>> l0_iters;
        std::vector<std::shared_ptr<Sstable>> l1_iters;
        for(auto &l0:o->l0_sstables){
            l0_iters.push_back(std::unique_ptr<Iterators>(SsTableIterator::
                create_and_seek_to_first(snapshot->sstables.at(l0).get())));
        }

        for(auto &l1:o->l1_sstables){
            l1_iters.push_back(snapshot->sstables.at(l1));
        }
        return compact_generate_sst_from_iter(
            std::make_unique<TwoMergeIterator>(
                std::make_unique<MergeIterators>(std::move(l0_iters)),
                SstConcatIterator::create_and_seek_to_first(l1_iters)
            )
        );
    }else if(const auto* o = dynamic_cast<const TieredCompactionTask*>(&task)){
        std::vector<std::unique_ptr<Iterators>> iters;
        for(auto &tir:o->tiers){
            std::vector<std::shared_ptr<Sstable>> tables;
            for(auto &id:tir.sst_ids)tables.push_back(snapshot->sstables.at(id));
            iters.push_back(SstConcatIterator::create_and_seek_to_first(tables));
        }
        return compact_generate_sst_from_iter(
            std::make_unique<MergeIterators>(std::move(iters))
        );
    }else if(const auto* o = dynamic_cast<const LeveledCompactionTask*>(&task)){
        if(o->upper_level>-1){
            std::vector<std::shared_ptr<Sstable>> upper_tables;
            std::vector<std::shared_ptr<Sstable>> lower_tables;
            for(auto &id:o->upper_level_sst_ids){
                upper_tables.push_back(snapshot->sstables.at(id));
            }
            for(auto &id:o->lower_level_sst_ids){
                lower_tables.push_back(snapshot->sstables.at(id));
            }
            return compact_generate_sst_from_iter(std::make_unique<TwoMergeIterator>(
                SstConcatIterator::create_and_seek_to_first(upper_tables),
                SstConcatIterator::create_and_seek_to_first(lower_tables)
            ));
        }else{
            std::vector<std::unique_ptr<Iterators>> upper_iters;
            for(auto &id:o->upper_level_sst_ids){
                upper_iters.push_back(std::unique_ptr<SsTableIterator>(SsTableIterator::create_and_seek_to_first(
                    snapshot->sstables.at(id).get()
                )));
            }
            std::vector<std::shared_ptr<Sstable>> lower_tables;
            for(auto &id:o->lower_level_sst_ids){
                lower_tables.push_back(snapshot->sstables.at(id));
            }
            return compact_generate_sst_from_iter(std::make_unique<TwoMergeIterator>(
                std::make_unique<MergeIterators>(std::move(upper_iters)),
                SstConcatIterator::create_and_seek_to_first(lower_tables)
            ));
        }
    }else if(const auto* o = dynamic_cast<const SimpleLeveledCompactionTask*>(&task)){
        if(o->upper_level>-1){
            std::vector<std::shared_ptr<Sstable>> upper_tables;
            std::vector<std::shared_ptr<Sstable>> lower_tables;
            for(auto &id:o->upper_level_sst_ids){
                upper_tables.push_back(snapshot->sstables.at(id));
            }
            for(auto &id:o->lower_level_sst_ids){
                lower_tables.push_back(snapshot->sstables.at(id));
            }
            return compact_generate_sst_from_iter(std::make_unique<TwoMergeIterator>(
                SstConcatIterator::create_and_seek_to_first(upper_tables),
                SstConcatIterator::create_and_seek_to_first(lower_tables)
            ));
        }else{
            std::vector<std::unique_ptr<Iterators>> upper_iters;
            for(auto &id:o->upper_level_sst_ids){
                upper_iters.push_back(std::unique_ptr<SsTableIterator>(SsTableIterator::create_and_seek_to_first(
                    snapshot->sstables.at(id).get()
                )));
            }
            std::vector<std::shared_ptr<Sstable>> lower_tables;
            for(auto &id:o->lower_level_sst_ids){
                lower_tables.push_back(snapshot->sstables.at(id));
            }
            return compact_generate_sst_from_iter(std::make_unique<TwoMergeIterator>(
                std::make_unique<MergeIterators>(std::move(upper_iters)),
                SstConcatIterator::create_and_seek_to_first(lower_tables)
            ));
        }
    }
    return std::vector<std::shared_ptr<Sstable>>{};
}

bool LsmStorageInner::force_full_compaction()
{
    // std::vector<uint64_t> l0_sst;
    // std::vector<uint64_t> l1_sst;
    // {
    //     auto snapshot = get_snapshot();
    //     l0_sst = snapshot->l0_sstables;
    //     if(snapshot->levels.size()>0)l1_sst=snapshot->levels[0].sst_ids;
    // }
    // if(l0_sst.size()==0 && l1_sst.size()==0)return true;

    // auto task = std::make_unique<ForceFullCompactionTask>();
    // task->l0_sstables = l0_sst;
    // task->l1_sstables = l1_sst;

    return false;
}

void LsmStorageInner::trigger_compaction()
{
    auto snapshot = get_snapshot();
    auto task = compaction_controller->generate_compaction_task(*snapshot);
    if(task){
        auto new_sst = compact(*task);
        std::vector<uint64_t> output;
        for(auto &t:new_sst)output.push_back(t->sst_id()); 

        //我觉得可以在这里不加锁?
        std::unique_lock write_lock(write_mutex_);
        auto new_state = std::make_unique<LsmStorageState>(*get_snapshot());
        for(auto &t:new_sst){
            new_state->sstables.emplace(t->sst_id(),std::move(t));
        }
        auto remove_files = compaction_controller->apply_compaction_result(*new_state,*task,output,false);
        for(auto &delete_id:remove_files){
            FileObject::delete_file(path_of_sst(delete_id).c_str());
            new_state->sstables.erase(delete_id);
        }
        update_state(std::move(new_state));
        sync_dir();
        manifest_->add_record(CompactionRecord(std::move(task),output));
    }
}

void LsmStorageInner::trigger_flush()
{
    auto snapshot = get_snapshot();
    if(snapshot->imm_memtables.size()+1>=options.num_memtable_limit){
        force_flush_next_imm_memtable();
    }
}

std::unique_ptr<Transaction> LsmStorageInner::new_txn()
{
    return mvcc_->new_txn(this,options.serializable);
}

Value LsmStorageInner::get(std::string_view key){
    auto txn = new_txn();
    return txn->get(key);
}

bool LsmStorageInner::write_batch(const std::vector<WriteBatchRecord>& batch){
    if(!options.serializable){
        return write_batch_inner(batch);
    }else{
        auto txn = new_txn();
        for(auto &r:batch){
            if(r.type==0)txn->delete_(r.key.user_key);
            else txn->put(r.key.user_key,r.value);
        }
        if(!txn->commit()){
            throw std::logic_error("Thread Check Error!!");
        }
    }
    return true;
}

bool LsmStorageInner::put(std::string_view key,Value value){
    if(!options.serializable){
        WriteBatchRecord r;
        r.type = 1;
        r.key = LsmKey(key,0);
        r.value = value;
        return write_batch_inner({r});
    }else{
        auto txn = new_txn();
        txn->put(key,value);
        if(!txn->commit()){
            throw std::logic_error("Thread Check Error!!");
        }
        return true;
    }
}

bool LsmStorageInner::delete_(std::string_view key){
    if(!options.serializable){
        WriteBatchRecord r;
        r.type = 0;
        r.key = LsmKey(key,0);
        return write_batch_inner({r});
    }else{
        auto txn = new_txn();
        txn->delete_(key);
        if(!txn->commit()){
            throw std::logic_error("Thread Check Error!!");
        }
        return true;
    }
}

std::unique_ptr<Iterators> LsmStorageInner::
scan(const Bound<std::string_view>&lower,const Bound<std::string_view>&upper)
{

    auto txn = new_txn().release();
    auto inner_scan = txn->scan_out(lower,upper);
    
    return std::make_unique<TxnIterator>(txn,std::make_unique<TwoMergeIterator>(
        std::move(inner_scan),scan_with_ts(lower,upper,txn->read_ts())
    ),true);
}


bool LsmStorageInner::key_within(std::string_view user_key,
    const LsmKey&table_begin,const LsmKey&table_end)
{
    return table_begin.user_key<=user_key && user_key<=table_end.user_key;
}

Value LsmStorageInner::get_with_ts(std::string_view key,uint64_t ts)
{
    auto snapshot = get_snapshot();
    std::vector<std::unique_ptr<Iterators>> mem_iters;
    Bound<LsmKey> lower;
    Bound<LsmKey> upper;
    lower.type=1;
    lower.key = LsmKey(key,TS_BEGIN);
    upper.type=1;
    upper.key = LsmKey(key,TS_END);
    mem_iters.push_back(std::unique_ptr<Iterators>(snapshot->memtable->scan(
        lower,upper
    )));
    for(auto &mem:snapshot->imm_memtables){
        mem_iters.push_back(std::unique_ptr<Iterators>(mem->scan(lower,upper)));
    }
    auto merge_mem_iter = std::make_unique<MergeIterators>(std::move(mem_iters));
    std::vector<std::unique_ptr<Iterators>> l0_iters;

    auto keep_table = [&](std::string_view key,Sstable*table){
        if(key_within(key,table->first_key_(),table->last_key_())){
            if(auto bloom=table->get_bloom()){
                if(bloom->may_contain(Fingerprint32(key.data(),key.length()))){
                    return true;
                }
            }else{
                return true;
            }
        }
        return false;
    };
    for(auto &sst:snapshot->l0_sstables){
        auto table = snapshot->sstables.at(sst);
        if(keep_table(key,table.get())){
            l0_iters.push_back(std::unique_ptr<Iterators>(SsTableIterator::create_and_seek_to_key(
                table.get(),LsmKey(key,TS_BEGIN)
            )));
        }
    }
    auto l0_merge_iter = std::make_unique<MergeIterators>(std::move(l0_iters));

    std::vector<std::unique_ptr<Iterators>> ln_iters;
    for(auto &pair:snapshot->levels){
        std::vector<std::shared_ptr<Sstable>> tables;
        for(auto &id:pair.sst_ids){
            auto table = snapshot->sstables.at(id);
            if(keep_table(key,table.get())){
                tables.push_back(table);
            }
        }
        ln_iters.push_back(std::unique_ptr<Iterators>(SstConcatIterator::create_and_seek_to_key(
            tables,LsmKey(key,TS_BEGIN)
        )));
    }

    Bound<LsmKey> upper_;
    upper_.type = 0;
    auto iter = std::make_unique<LsmIterator>(
        std::make_unique<TwoMergeIterator>(
            std::make_unique<TwoMergeIterator>(std::move(merge_mem_iter),std::move(l0_merge_iter)),
            std::make_unique<MergeIterators>(std::move(ln_iters))
        ),
        upper_,
        ts
    );
    if(iter->is_valid() && iter->key().user_key==key && !iter->value().is_empty()){
        return iter->value();
    }
    return Value();
}

uint64_t LsmStorageInner::write_batch_inner(const std::vector<WriteBatchRecord>& batch)
{
    std::unique_lock write_lock(write_mutex_); 
    auto new_state = std::make_unique<LsmStorageState>(*get_snapshot());
    uint64_t read_ts = mvcc_->latest_commit_ts()+1;
    for(auto &r:batch){
        if(r.type==0){
            new_state->memtable->put(LsmKey(r.key.user_key,read_ts),Value());
        }else{
            new_state->memtable->put(LsmKey(r.key.user_key,read_ts),r.value);
        }
    }
    update_state(std::move(new_state));
    if(state->memtable->approximate_size()>=options.target_sst_size){
        force_freeze_memtable();
    }
    mvcc_->update_commit_ts(read_ts);
    return read_ts;
}


bool LsmStorageInner::freeze_memtable_with_memtable(std::unique_ptr<MemTable> memtable)
{
    auto new_state = std::make_unique<LsmStorageState>(*get_snapshot());
    auto old_mem = std::move(new_state->memtable);
    new_state->memtable = std::move(memtable);
    old_mem->sync_wal();
    new_state->imm_memtables.insert(new_state->imm_memtables.begin(),
                std::move(old_mem));
    update_state(std::move(new_state));
    return true;
}

bool LsmStorageInner::force_freeze_memtable()
{
    auto nextsst = next_sst_id();
    MemTable *memtable = nullptr;
    if(options.enable_wal){
        memtable = new MemTable(nextsst,path_of_wal(nextsst).c_str(),false);
    }else{
        memtable = new MemTable(nextsst);
    }

    freeze_memtable_with_memtable(std::unique_ptr<MemTable>(memtable));
    manifest_->add_record(NewMemtableRecord(nextsst));
    sync_dir();
    return true;
}


bool LsmStorageInner::force_flush_next_imm_memtable()
{
    //TOOD:这里必须加锁吗 
    std::unique_lock write_lock(write_mutex_);
    uint64_t flush_id = 0;
    auto block_size = options.block_size;
    auto builder = new TableBuilder(block_size);
    {
        auto snapshot = get_snapshot();
        auto &last_mem = snapshot->imm_memtables.back();
        last_mem->flush(builder);
        flush_id = last_mem->id();
    }

    auto sstable = builder->build(flush_id,path_of_sst(flush_id).c_str());
    {
        auto new_state = std::make_unique<LsmStorageState>(*get_snapshot());
        new_state->imm_memtables.pop_back();
        if(compaction_controller->flush_to_l0()){
            new_state->l0_sstables.insert(new_state->l0_sstables.begin(),flush_id);
        }else{
            LevelItem item;
            item.level_num = flush_id;
            item.sst_ids = {flush_id};
            new_state->levels.insert(new_state->levels.begin(),item);
        }
        new_state->sstables.emplace(flush_id,std::move(sstable));
        update_state(std::move(new_state));
    }

    if(options.enable_wal){
        FileObject::delete_file(path_of_wal(flush_id).c_str());
    }
    manifest_->add_record(FlushRecord(flush_id));
    return true;
}

std::unique_ptr<SsTableIterator> LsmStorageInner::create_sst_iterator_with_end(Sstable*table,
        const LsmKey&lower,const LsmKey&upper)
{
    return std::unique_ptr<SsTableIterator>(SsTableIterator::create_and_seek_to_range(table,lower,upper));
}

std::unique_ptr<Iterators> LsmStorageInner::scan_with_ts(const Bound<std::string_view>&lower,
    const Bound<std::string_view>&upper,uint64_t ts)
{
    auto snapshot = get_snapshot();
    auto bound_key = map_key_bound_plus_ts(lower,upper,ts);
    std::vector<std::unique_ptr<Iterators>> mem_iters;
    auto mem_iter = snapshot->memtable->scan(bound_key.first,bound_key.second);
    if(mem_iter->is_valid()){
        mem_iters.push_back(std::unique_ptr<Iterators>(mem_iter));
    }else{
        delete mem_iter;
    }
    for(auto &sst:snapshot->imm_memtables){
        auto imm_iter = sst->scan(bound_key.first,bound_key.second);
        if(imm_iter->is_valid()){
            mem_iters.push_back(std::unique_ptr<Iterators>(imm_iter));
        }else{
            delete mem_iter;
        }
    }

    auto mm_merge_iter = std::make_unique<MergeIterators>(std::move(mem_iters));

    auto bound_key1 = map_key_bound_plus_ts1(lower,upper,ts);
    std::vector<std::unique_ptr<Iterators>> l0_iters;
    for(auto &item:snapshot->l0_sstables){
        if (auto it = snapshot->sstables.find(item); it != snapshot->sstables.end()){
            if(range_overlap(lower,upper,
                it->second->first_key_(),it->second->last_key_())){
                auto table_iter = create_sst_iterator_with_end(it->second.get(),
                bound_key1.first.key,bound_key1.second.key);
                if(table_iter->is_valid()){
                    l0_iters.push_back(std::move(table_iter));
                }
            }
        }
    }

    std::vector<std::unique_ptr<Iterators>> ln_iters;
    for(auto&item:snapshot->levels){
        std::vector<std::shared_ptr<Sstable>> tables;
        for(auto &sst:item.sst_ids){
            if (auto it = snapshot->sstables.find(sst); it != snapshot->sstables.end()){
                if(range_overlap(lower,upper,
                it->second->first_key_(),it->second->last_key_())){
                    tables.push_back(it->second);
                }
            }
        }
        auto ln_iter = lower.type==0?SstConcatIterator::create_and_seek_to_first(tables):
                SstConcatIterator::create_and_seek_to_key(tables,bound_key1.first.key);
        if(ln_iter->is_valid())ln_iters.push_back(std::move(ln_iter));
    }

    auto sst0_merge_iter = std::make_unique<MergeIterators>(std::move(l0_iters));
    auto sst1n_merge_iter = std::make_unique<MergeIterators>(std::move(ln_iters));
    auto merge_iter1 = std::make_unique<TwoMergeIterator>(std::move(mm_merge_iter),std::move(sst0_merge_iter));
    auto inner_iter = std::make_unique<TwoMergeIterator>(std::move(merge_iter1),
    std::move(sst1n_merge_iter));
    auto lsm_iter = std::make_unique<LsmIterator>(std::move(inner_iter),bound_key.second,ts);

    return std::make_unique<FusedIterator>(std::move(lsm_iter));

}

bool starts_with(std::string_view a, std::string_view b) noexcept {
    if (b.size() > a.size()) {
        return false;
    }
    // 零拷贝比较前 N 个字符
    return std::memcmp(a.data(), b.data(), b.size()) == 0;
}

std::vector<std::shared_ptr<Sstable>> LsmStorageInner::
compact_generate_sst_from_iter(std::unique_ptr<Iterators> iter)
{
    std::unique_ptr<TableBuilder> builder=nullptr;
    std::vector<std::shared_ptr<Sstable>> new_sst;
    std::string_view last_key;

    auto water_mark = mvcc_->watermark();
    bool first_key_below_watermark = false;

    while (iter->is_valid())
    {
        if(!builder){
            builder = std::make_unique<TableBuilder>(options.block_size);
        }
        bool same_as_last_key = iter->key().user_key==last_key;
        if (!same_as_last_key){
            first_key_below_watermark = true;
        }

        if(!same_as_last_key && iter->key().ts<=water_mark && iter->value().is_empty()){
            last_key = iter->key().user_key;
            iter->next();
            first_key_below_watermark = false;
            continue;
        }

        if(iter->key().ts<=water_mark){
            if(!first_key_below_watermark){
                iter->next();
                continue;
            }
            first_key_below_watermark = false;
            bool skip = false;
            for(auto &fliter:compaction_filters){
                if(starts_with(iter->key().user_key,fliter.prefix)){
                    iter->next();
                    skip = true;
                    break;
                }
            }
            if(skip)continue;
        }

        if(builder->estimated_size()>=options.target_sst_size){
            auto sst_id = next_sst_id();
            auto sst_table = builder->build(sst_id,path_of_sst(sst_id).c_str()).release();
            new_sst.push_back(std::shared_ptr<Sstable>(sst_table));
            builder = std::make_unique<TableBuilder>(options.block_size);
        }

        builder->add(iter->key(),iter->value());
        if(!same_as_last_key){
            last_key = iter->key().user_key;
        }
        iter->next();
    }

    if(!builder->is_empty()){
        auto sst_id = next_sst_id();
        auto sst_table = builder->build(sst_id,path_of_sst(sst_id).c_str()).release();
        new_sst.push_back(std::shared_ptr<Sstable>(sst_table));    
    }
    return new_sst;
}


LsmTree::LsmTree(std::string path,LsmStorageOptions options){
    inner = std::make_shared<LsmStorageInner>(path,options);
    flush_thread_ = inner->spawn_flush_thread(flush_stop_cv_,flush_mtx_,flush_stop_flag_);
    compaction_thread_ = inner->spawn_compaction_thread(compaction_stop_cv_,compaction_mtx_,compaction_stop_flag_);
}
LsmTree::~LsmTree(){
    {
        std::lock_guard lock(flush_mtx_);
        flush_stop_flag_ = true;
    }
    flush_stop_cv_.notify_all();
    if (flush_thread_) flush_thread_->join();
    {
        std::lock_guard lock(compaction_mtx_);
        compaction_stop_flag_ = true;
    }
    compaction_stop_cv_.notify_all();
    if (compaction_thread_) compaction_thread_->join();

    if(inner->options.enable_wal){
        inner->sync();
        inner->sync_dir();
    }

    {
        auto snapshot = inner->get_snapshot();
        if(!snapshot->memtable->is_empty()){
            inner->freeze_memtable_with_memtable(std::make_unique<MemTable>(inner->next_sst_id()));
        }
        while (snapshot->imm_memtables.size()>0)
        {
            inner->force_flush_next_imm_memtable();
            snapshot = inner->get_snapshot();
        } 
    } 
    inner->sync_dir();
}