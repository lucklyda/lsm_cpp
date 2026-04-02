#include "compact.h"
#include "../lsm_tree.h"
#include <unordered_set>


std::vector<uint64_t> LeveledCompactionController::find_overlapping_ssts(const LsmStorageState& snapshot,
        const std::vector<uint64_t>&sst_ids,uint64_t in_level)
{
    auto get_first_key = [&](size_t id) { return snapshot.sstables.at(id)->first_key_(); };
    auto min_iter = std::min_element(sst_ids.begin(), sst_ids.end(),
        [&](size_t a, size_t b) { return get_first_key(a) < get_first_key(b); });
    auto begin_key = get_first_key(*min_iter);


    auto get_last_key = [&](size_t id) { return snapshot.sstables.at(id)->last_key_(); };
    auto max_iter = std::max_element(sst_ids.begin(), sst_ids.end(),
        [&](size_t a, size_t b) { return get_last_key(a) < get_last_key(b); });
    auto end_key = get_last_key(*max_iter);

    std::vector<size_t> overlap_ssts;
    const auto& level_sst_ids = snapshot.levels[in_level - 1].sst_ids;
    for (size_t sst_id : level_sst_ids) {
        const auto& sst = snapshot.sstables.at(sst_id);
        if (!(sst->last_key_() < begin_key || sst->first_key_() > end_key)) {
            overlap_ssts.push_back(sst_id);
        }
    }
    return overlap_ssts;
}

std::unique_ptr<CompactionTask> LeveledCompactionController::generate_compaction_task(
        const LsmStorageState& snapshot)
{
    std::vector<uint64_t> target_level_size(options_.max_levels,0);
    std::vector<uint64_t> real_level_size(options_.max_levels,0);
    for (size_t level = 0; level < options_.max_levels; ++level) {
        size_t total_size = 0;
        for (size_t sst_id : snapshot.levels[level].sst_ids) {
            auto it = snapshot.sstables.find(sst_id);
            if (it != snapshot.sstables.end()) {
                total_size += it->second->table_size();
            }
        }
        real_level_size[level] = total_size;
    }

    uint64_t base_level_size_bytes=options_.base_level_size_mb*1024*1024;
    target_level_size[options_.max_levels - 1] = std::max(
        real_level_size[options_.max_levels - 1], base_level_size_bytes);
    for (int i = static_cast<int>(options_.max_levels) - 2; i >= 0; --i) {
        uint64_t next_level_size = target_level_size[i + 1];
        if (next_level_size > base_level_size_bytes) {
            target_level_size[i] = next_level_size / options_.level_size_multiplier;
        }
    }

    uint64_t base_level = options_.max_levels;
    for (uint64_t i = 0; i < options_.max_levels; ++i) {
        if (target_level_size[i] > 0) {
            base_level = i + 1;
            break;
        }
    }

    if (snapshot.l0_sstables.size() >= options_.level0_file_num_compaction_trigger) {
        auto task = std::make_unique<LeveledCompactionTask>();
        task->upper_level =-1;
        task->upper_level_sst_ids = snapshot.l0_sstables;
        task->lower_level = base_level;
        task->lower_level_sst_ids = find_overlapping_ssts(snapshot, snapshot.l0_sstables, base_level);
        task->is_lower_level_bottom_level = (base_level == options_.max_levels);
        return task;
    }

    double max_prio = 1.0;
    size_t max_prio_level = 0;   
    for (size_t level = 0; level < options_.max_levels; ++level) {
        if (target_level_size[level] == 0) continue;
        double prio = static_cast<double>(real_level_size[level]) /
                      static_cast<double>(target_level_size[level]);
        if (prio > max_prio) {
            max_prio = prio;
            max_prio_level = level + 1; 
        }
    }

    if (max_prio_level > 0) {
        // 从选中层中选一个 SST ID（按最小 ID，原 Rust 代码使用 .min()）
        const auto& level_sst_ids = snapshot.levels[max_prio_level - 1].sst_ids;
        if (level_sst_ids.empty()) return nullptr; // 防御
        size_t selected_sst = *std::min_element(level_sst_ids.begin(), level_sst_ids.end());

        auto task = std::make_unique<LeveledCompactionTask>();
        task->upper_level = max_prio_level;
        task->upper_level_sst_ids = { selected_sst };
        task->lower_level = max_prio_level + 1;
        task->lower_level_sst_ids = find_overlapping_ssts(snapshot, { selected_sst }, max_prio_level + 1);
        task->is_lower_level_bottom_level = (max_prio_level + 1 >= options_.max_levels);
        return task;
    }
    return nullptr;
}

std::vector<size_t> LeveledCompactionController::apply_compaction_result(
        LsmStorageState& snapshot,
        const CompactionTask& task,
        const std::vector<size_t>& output,
        bool in_recovery)
{
    std::vector<size_t> files_to_remove;

    LeveledCompactionTask level_task;
    if (const auto* o = dynamic_cast<const LeveledCompactionTask*>(&task)) {
        level_task = *o;
    }else{
        throw std::logic_error("Task MisMatch");
    }
    
    std::unordered_set<size_t> upper_set(level_task.upper_level_sst_ids.begin(),
                                         level_task.upper_level_sst_ids.end());
    std::unordered_set<size_t> lower_set(level_task.lower_level_sst_ids.begin(),
                                         level_task.lower_level_sst_ids.end());
    
     if (level_task.upper_level>-1) {
        size_t level_idx = level_task.upper_level - 1;
        auto& level_ssts = snapshot.levels[level_idx].sst_ids;
        std::vector<size_t> new_level_ssts;
        for (size_t id : level_ssts) {
            if (upper_set.count(id)) {
                upper_set.erase(id);  // 标记为已找到并移除
            } else {
                new_level_ssts.push_back(id);
            }
        }
        snapshot.levels[level_idx].sst_ids = std::move(new_level_ssts);
     }else{
        std::vector<size_t> new_l0_ssts;
        for (size_t id : snapshot.l0_sstables) {
            if (upper_set.count(id)) {
                upper_set.erase(id);
            } else {
                new_l0_ssts.push_back(id);
            }
        }
        snapshot.l0_sstables = std::move(new_l0_ssts);
     }

     files_to_remove.insert(files_to_remove.end(),
                           level_task.upper_level_sst_ids.begin(),
                           level_task.upper_level_sst_ids.end());
    files_to_remove.insert(files_to_remove.end(),
                           level_task.lower_level_sst_ids.begin(),
                           level_task.lower_level_sst_ids.end());
    size_t lower_level_idx = level_task.lower_level - 1;
    auto& lower_level_ssts = snapshot.levels[lower_level_idx].sst_ids;
    std::vector<size_t> new_lower_level;
    for (size_t id : lower_level_ssts) {
        if (lower_set.count(id)) {
            lower_set.erase(id);
        } else {
            new_lower_level.push_back(id);
        }
    }

    new_lower_level.insert(new_lower_level.end(), output.begin(), output.end());

    if (!in_recovery) {
        std::sort(new_lower_level.begin(), new_lower_level.end(),
            [&](size_t a, size_t b) {
                return snapshot.sstables[a]->first_key_() < snapshot.sstables[b]->first_key_();
            });
    }

    snapshot.levels[lower_level_idx].sst_ids = std::move(new_lower_level);
    return files_to_remove;
}


std::unique_ptr<CompactionTask> TieredCompactionController::generate_compaction_task(
    const LsmStorageState& snapshot)
{
    if(snapshot.levels.size()<options_.num_tiers){
        return nullptr;
    }
    //TOOD 这里不用sstable的数量最为依据
    uint64_t size = 0;
    for(uint32_t i=0;i<snapshot.levels.size()-1;++i){
        size+=snapshot.levels[i].sst_ids.size();
    }

    double space_amp_ratio=(1.0*size)/(snapshot.levels[snapshot.levels.size()-1].sst_ids.size()*1.0);
    if(space_amp_ratio>=options_.max_size_amplification_percent){
        auto task = std::make_unique<TieredCompactionTask>();
        task->bottom_tier_included=true;
        task->tiers=snapshot.levels;
        return task;
    }

    double size_ratio_trigger=(100.0+options_.size_ratio)/100.0;
    uint64_t  cumulative_size= 0;
    for(uint32_t i=0;i<snapshot.levels.size()-1;++i){
        cumulative_size+=snapshot.levels[i].sst_ids.size();
        size_t next_level_size = snapshot.levels[i + 1].sst_ids.size();
        double current_ratio = static_cast<double>(next_level_size) / cumulative_size;
        if (current_ratio > size_ratio_trigger && i + 1 >= options_.min_merge_width) {
            auto task = std::make_unique<TieredCompactionTask>();
            task->tiers.assign(snapshot.levels.begin(), snapshot.levels.begin() + i + 1);
            task->bottom_tier_included = false;
            return task;
        }
    }

    size_t num_tiers_to_take = snapshot.levels.size();
    if (options_.max_merge_width>-1) {
        num_tiers_to_take = std::min(num_tiers_to_take, (size_t)options_.max_merge_width);
    }
    auto task = std::make_unique<TieredCompactionTask>();
    task->tiers.assign(snapshot.levels.begin(), snapshot.levels.begin() + num_tiers_to_take);
    task->bottom_tier_included = (snapshot.levels.size() >= num_tiers_to_take);
    return task;
}
std::vector<size_t> TieredCompactionController::apply_compaction_result(
    LsmStorageState& snapshot,
    const CompactionTask& task,
    const std::vector<size_t>& output,
    bool in_recovery)
{
    std::vector<size_t> files_to_remove;

    TieredCompactionTask tiere_task;
    if (const auto* o = dynamic_cast<const TieredCompactionTask*>(&task)) {
        tiere_task = *o;
    }else{
        throw std::logic_error("Task MisMatch");
    }

    std::unordered_map<uint64_t,std::vector<uint64_t>> tier_to_remove;
    for(auto &iter:tiere_task.tiers){
        tier_to_remove[iter.level_num]=iter.sst_ids;
    }
    std::vector<LevelItem> levels;
    bool new_tier_added=false;
    for(auto &entry:snapshot.levels){
        auto iter = tier_to_remove.find(entry.level_num);
        if(iter!=tier_to_remove.end()){
            files_to_remove.insert(files_to_remove.end(),iter->second.begin(),iter->second.end());
            tier_to_remove.erase(iter);
        }else{
            //new add
            levels.push_back({iter->first,entry.sst_ids});
        }

        if(tier_to_remove.size()==0 && !new_tier_added){
            new_tier_added = true;
            levels.push_back({output[0],output});
        }
    }
    if(tier_to_remove.size()!=0){
        throw std::logic_error("some tiers not found??");
    }
    snapshot.levels.swap(levels);
    return files_to_remove;
}

std::unique_ptr<CompactionTask> SimpleLeveledCompactionController::generate_compaction_task(
        const LsmStorageState& snapshot)
{
    if(snapshot.l0_sstables.size()>=options_.level0_file_num_compaction_trigger){
        auto task = std::make_unique<SimpleLeveledCompactionTask>();
        task->upper_level = -1;
        task->upper_level_sst_ids = snapshot.l0_sstables;
        task->lower_level=1;
        task->lower_level_sst_ids=snapshot.levels[0].sst_ids;
        task->is_lower_level_bottom_level=(1==options_.max_levels);
        return task;
    }
    for(uint32_t level = 1;level<options_.max_levels;++level){
        auto upper_count = snapshot.levels[level-1].sst_ids.size();
        if(upper_count==0)continue;
        auto current_level_count = snapshot.levels[level].sst_ids.size();
        double ratio = (current_level_count*1.0)/(upper_count*1.0)/100.0;
        if(ratio<=options_.size_ratio_percent){
            auto task = std::make_unique<SimpleLeveledCompactionTask>();
            task->upper_level = level;
            task->upper_level_sst_ids =snapshot.levels[level-1].sst_ids;
            task->lower_level=level+1;
            task->lower_level_sst_ids=snapshot.levels[level].sst_ids;
            task->is_lower_level_bottom_level=(level+1==options_.max_levels);
            return task;
        }
    }
    return nullptr;
}
std::vector<size_t> SimpleLeveledCompactionController::apply_compaction_result(
    LsmStorageState& snapshot,
    const CompactionTask& task,
    const std::vector<size_t>& output,
    bool in_recovery)
{
    std::vector<size_t> files_to_remove;
    SimpleLeveledCompactionTask siple_task;
    if (const auto* o = dynamic_cast<const SimpleLeveledCompactionTask*>(&task)) {
        siple_task = *o;
    }else{
        throw std::logic_error("Task MisMatch");
    }

    if(siple_task.upper_level>-1){
        files_to_remove=snapshot.levels[siple_task.upper_level-1].sst_ids;
        snapshot.levels[siple_task.upper_level-1].sst_ids.clear();
    }else{
        files_to_remove = siple_task.lower_level_sst_ids;
        std::unordered_set<uint64_t> l0_ssts_compacted(
            siple_task.upper_level_sst_ids.begin(), 
            siple_task.upper_level_sst_ids.end()
        );
        std::vector<uint64_t> new_l0_sstables;
        new_l0_sstables.reserve(snapshot.l0_sstables.size());
        for (uint64_t x : snapshot.l0_sstables) {
            if (!l0_ssts_compacted.erase(x)) {
                new_l0_sstables.push_back(x);
            }
        }
        snapshot.l0_sstables.swap(new_l0_sstables);
    }
    files_to_remove.insert(files_to_remove.end(),siple_task.lower_level_sst_ids.begin(),siple_task.lower_level_sst_ids.end());
    return files_to_remove;
}


std::unique_ptr<CompactionController> create_controller(const CompactionOptions& opts) {
    if (const auto* o = dynamic_cast<const LeveledCompactionOptions*>(&opts)) {
        return std::make_unique<LeveledCompactionController>(*o);
    }
    if (const auto* o = dynamic_cast<const TieredCompactionOptions*>(&opts)) {
        return std::make_unique<TieredCompactionController>(*o);
    }
    if (const auto* o = dynamic_cast<const SimpleLeveledCompactionOptions*>(&opts)) {
        return std::make_unique<SimpleLeveledCompactionController>(*o);
    }
    
    return std::make_unique<NoCompactionController>();
}