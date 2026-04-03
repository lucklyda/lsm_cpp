#pragma once
#include <memory>
#include <vector>
#include <variant>
#include <stdexcept>
#include "../key.h"

class LsmStorageInner;
struct LsmStorageState;
class Sstable;
struct LevelItem;
using ArcSsTable = std::unique_ptr<Sstable>;

class CompactionTask {
public:
    CompactionTask() = default;
    virtual ~CompactionTask() = default;
    virtual bool compact_to_bottom_level() const = 0;
//for manifest
    virtual uint32_t encode(char*data){
        memcpy(data,&type_,sizeof(uint8_t));
        memcpy(data+sizeof(uint8_t),&tot_len_,sizeof(uint32_t));
        return sizeof(uint8_t)+sizeof(uint32_t);
    }
    virtual void decode(const char*data){
        type_ = *(uint8_t*)(data);
        tot_len_ = *(uint32_t*)(data+sizeof(uint8_t));
    }
    virtual void caluc_len(){
        tot_len_=5;
    }
public:
    uint8_t type_;
    uint32_t tot_len_=0;
};
enum CompactionOptionsType{
    NoneCmpact,
    LeveledCmpact,
    TiereCmpact,
    SimpleLeveledCmpact,
};


class CompactionOptions
{
public:
    CompactionOptionsType type = CompactionOptionsType::NoneCmpact;
public:
    CompactionOptions(/* args */) = default;
    virtual ~CompactionOptions() = default;
};

class NoCompactionOptions : public CompactionOptions
{
private:
    /* data */
public:
    NoCompactionOptions(/* args */)=default;
    virtual ~NoCompactionOptions()=default;
};







class LeveledCompactionTask : public CompactionTask {
public:
    LeveledCompactionTask()=default;
    ~LeveledCompactionTask() override=default;
    int16_t upper_level;          // -1 is l0
    std::vector<uint64_t> upper_level_sst_ids;
    uint64_t lower_level;
    std::vector<uint64_t> lower_level_sst_ids;
    bool is_lower_level_bottom_level;

    bool compact_to_bottom_level() const override { return is_lower_level_bottom_level; }

    uint32_t encode(char*data) override{
        type_ = 1;
        uint32_t offset = CompactionTask::encode(data);
        memcpy(data+offset,&upper_level,sizeof(int16_t));
        offset+=sizeof(int16_t);
        uint32_t ups = upper_level_sst_ids.size();
        memcpy(data+offset,&ups,sizeof(uint32_t));
        offset+=sizeof(uint32_t);
        for(auto &us:upper_level_sst_ids){
            memcpy(data+offset,&us,sizeof(uint64_t));
            offset+=sizeof(uint64_t);
        }

        memcpy(data+offset,&lower_level,sizeof(uint64_t));
        offset+=sizeof(uint64_t);
        uint32_t los = lower_level_sst_ids.size();
        memcpy(data+offset,&los,sizeof(uint32_t));
        offset+=sizeof(uint32_t);
        for(auto &us:lower_level_sst_ids){
            memcpy(data+offset,&us,sizeof(uint64_t));
            offset+=sizeof(uint64_t);
        }
        uint8_t is_bot = is_lower_level_bottom_level?1:0;
        memcpy(data+offset,&is_bot,sizeof(uint8_t));
        return offset+sizeof(uint8_t);
    }
    void decode(const char*data)override{
        CompactionTask::decode(data);
        uint32_t offset = 5;
        upper_level = *(int16_t*)(data+offset);
        offset+=sizeof(int16_t);
        uint32_t upn = *(uint32_t*)(data+offset);
        offset+=sizeof(uint32_t);
        for(uint32_t i=0;i<upn;++i){
            upper_level_sst_ids.push_back(*(uint64_t*)(data+offset));
            offset+=sizeof(uint64_t);
        }
        lower_level = *(uint64_t*)(data+offset);
        offset+=sizeof(uint64_t);
        uint32_t lwn = *(uint32_t*)(data+offset);
        offset+=sizeof(uint32_t);
        for(uint32_t i=0;i<lwn;++i){
            lower_level_sst_ids.push_back(*(uint64_t*)(data+offset));
            offset+=sizeof(uint64_t);
        }
        uint8_t isbtm = *(uint8_t*)(data+offset);
        is_lower_level_bottom_level = isbtm;
    }
    void caluc_len() override{
        tot_len_=5+sizeof(int16_t)+sizeof(uint32_t)+upper_level_sst_ids.size()*sizeof(uint64_t)
            +sizeof(uint64_t)+sizeof(uint32_t)+lower_level_sst_ids.size()*sizeof(uint64_t)
            +sizeof(uint8_t);
    }
};

class LeveledCompactionOptions:public CompactionOptions
{
public:
    uint64_t level_size_multiplier;
    uint64_t level0_file_num_compaction_trigger;
    uint64_t max_levels;
    uint64_t base_level_size_mb;
public:
    LeveledCompactionOptions(){
        type = CompactionOptionsType::LeveledCmpact;
    }
    ~LeveledCompactionOptions() override = default;
};



class TieredCompactionTask : public CompactionTask {
public:
    TieredCompactionTask()=default;
    ~TieredCompactionTask() override =default;
    bool bottom_tier_included;
    std::vector<LevelItem> tiers;  // (tier_id, sst_ids)

    bool compact_to_bottom_level() const override { return bottom_tier_included; }

    uint32_t encode(char*data) override{
        type_=2;
        uint32_t offset = CompactionTask::encode(data);
        uint8_t btm = bottom_tier_included?1:0;
        memcpy(data+offset,&btm,sizeof(uint8_t));
        offset+=sizeof(uint8_t);
        uint32_t size1 = tiers.size();
        memcpy(data+offset,&size1,sizeof(uint32_t));
        offset+=sizeof(uint32_t);
        for(auto &tir:tiers){
            memcpy(data+offset,&tir.level_num,sizeof(uint64_t));
            offset+=sizeof(uint64_t);
            uint32_t size2 = tir.sst_ids.size();
            memcpy(data+offset,&size2,sizeof(uint32_t));
            offset+=sizeof(uint32_t);
            for(auto &sst:tir.sst_ids){
                memcpy(data+offset,&sst,sizeof(uint64_t));
                offset+=sizeof(uint64_t);
            }
        }
        return offset;
    }
    void decode(const char*data)override{
        CompactionTask::decode(data);
        uint32_t offset = 5;
        uint8_t btm=*(uint8_t*)(data+offset);
        offset+=sizeof(uint8_t);
        bottom_tier_included=btm;
        uint32_t size1 = *(uint32_t*)(data+offset);
        for(uint32_t i=0;i<size1;++i){
            LevelItem item;
            item.level_num=*(uint64_t*)(data+offset);
            offset+=sizeof(uint64_t);
            uint32_t size2 = *(uint32_t*)(data+offset);
            offset+=sizeof(uint32_t);
            for(uint32_t j=0;j<size2;++j){
                item.sst_ids.push_back(*(uint64_t*)(data+offset));
                offset+=sizeof(uint64_t);
            }
            tiers.push_back(item);
        }
    }
    void caluc_len()override{
        tot_len_ = 5+sizeof(uint8_t)+sizeof(uint32_t);
        for(auto &item:tiers){
            tot_len_+=sizeof(uint64_t)+sizeof(uint32_t)+item.sst_ids.size()*sizeof(uint64_t);
        }
    }
};

class TieredCompactionOptions :public CompactionOptions
{
public:
    uint64_t num_tiers;
    uint64_t max_size_amplification_percent;
    uint64_t size_ratio;
    uint64_t min_merge_width;
    int max_merge_width;
public:
    TieredCompactionOptions(){
        type = CompactionOptionsType::TiereCmpact;
    }
    ~TieredCompactionOptions() override = default;
};


class SimpleLeveledCompactionTask : public CompactionTask {
public:
    SimpleLeveledCompactionTask() = default;
    ~SimpleLeveledCompactionTask() override =default;
    int16_t upper_level;          // -1 is l0
    std::vector<uint64_t> upper_level_sst_ids;
    uint64_t lower_level;
    std::vector<uint64_t> lower_level_sst_ids;
    bool is_lower_level_bottom_level;

    bool compact_to_bottom_level() const override { return is_lower_level_bottom_level; }

        uint32_t encode(char*data) override{
        type_ = 3;
        uint32_t offset = CompactionTask::encode(data);
        memcpy(data+offset,&upper_level,sizeof(int16_t));
        offset+=sizeof(int16_t);
        uint32_t ups = upper_level_sst_ids.size();
        memcpy(data+offset,&ups,sizeof(uint32_t));
        offset+=sizeof(uint32_t);
        for(auto &us:upper_level_sst_ids){
            memcpy(data+offset,&us,sizeof(uint64_t));
            offset+=sizeof(uint64_t);
        }

        memcpy(data+offset,&lower_level,sizeof(uint64_t));
        offset+=sizeof(uint64_t);
        uint32_t los = lower_level_sst_ids.size();
        memcpy(data+offset,&los,sizeof(uint32_t));
        offset+=sizeof(uint32_t);
        for(auto &us:lower_level_sst_ids){
            memcpy(data+offset,&us,sizeof(uint64_t));
            offset+=sizeof(uint64_t);
        }
        uint8_t is_bot = is_lower_level_bottom_level?1:0;
        memcpy(data+offset,&is_bot,sizeof(uint8_t));
        return offset+sizeof(uint8_t);
    }
    void decode(const char*data)override{
        CompactionTask::decode(data);
        uint32_t offset = 5;
        upper_level = *(int16_t*)(data+offset);
        offset+=sizeof(int16_t);
        uint32_t upn = *(uint32_t*)(data+offset);
        offset+=sizeof(uint32_t);
        for(uint32_t i=0;i<upn;++i){
            upper_level_sst_ids.push_back(*(uint64_t*)(data+offset));
            offset+=sizeof(uint64_t);
        }
        lower_level = *(uint64_t*)(data+offset);
        offset+=sizeof(uint64_t);
        uint32_t lwn = *(uint32_t*)(data+offset);
        offset+=sizeof(uint32_t);
        for(uint32_t i=0;i<lwn;++i){
            lower_level_sst_ids.push_back(*(uint64_t*)(data+offset));
            offset+=sizeof(uint64_t);
        }
        uint8_t isbtm = *(uint8_t*)(data+offset);
        is_lower_level_bottom_level = isbtm;
    }
    void caluc_len() override{
        tot_len_=5+sizeof(int16_t)+sizeof(uint32_t)+upper_level_sst_ids.size()*sizeof(uint64_t)
            +sizeof(uint64_t)+sizeof(uint32_t)+lower_level_sst_ids.size()*sizeof(uint64_t)
            +sizeof(uint8_t);
    }
};
class SimpleLeveledCompactionOptions:public CompactionOptions
{
public:
    /* data */
    uint64_t size_ratio_percent;
    uint64_t level0_file_num_compaction_trigger;
    uint64_t max_levels;
public:
    SimpleLeveledCompactionOptions(){
        type = CompactionOptionsType::SimpleLeveledCmpact;
    }
    ~SimpleLeveledCompactionOptions() override =default;
};



class ForceFullCompactionTask : public CompactionTask {
public:
    ForceFullCompactionTask()=default;
    ~ForceFullCompactionTask() override =default;
    std::vector<uint64_t> l0_sstables;
    std::vector<uint64_t> l1_sstables;

    bool compact_to_bottom_level() const override { return true; }

};



class CompactionController {
public:
    CompactionController()=default;
    virtual ~CompactionController() = default;
    virtual std::unique_ptr<CompactionTask> generate_compaction_task(
        const LsmStorageState& snapshot) = 0;
    virtual std::vector<size_t> apply_compaction_result(
        LsmStorageState& snapshot,
        const CompactionTask& task,
        const std::vector<size_t>& output,
        bool in_recovery) = 0;
    virtual bool flush_to_l0() const = 0;
};

class LeveledCompactionController : public CompactionController {
public:
    explicit LeveledCompactionController(const LeveledCompactionOptions& opts):options_(opts)
    {

    }
    std::unique_ptr<CompactionTask> generate_compaction_task(
        const LsmStorageState& snapshot) override;
    std::vector<size_t> apply_compaction_result(
        LsmStorageState& snapshot,
        const CompactionTask& task,
        const std::vector<size_t>& output,
        bool in_recovery) override;
    bool flush_to_l0() const override { return true; }
    ~LeveledCompactionController() override =default;
private:
    std::vector<uint64_t> find_overlapping_ssts(const LsmStorageState& snapshot,
        const std::vector<uint64_t>&sst_ids,uint64_t in_level);
private:
    LeveledCompactionOptions options_;
};

class TieredCompactionController : public CompactionController {
public:
    explicit TieredCompactionController(const TieredCompactionOptions& opts):options_(opts)
    {

    }
    std::unique_ptr<CompactionTask> generate_compaction_task(
        const LsmStorageState& snapshot) override;
    std::vector<size_t> apply_compaction_result(
        LsmStorageState& snapshot,
        const CompactionTask& task,
        const std::vector<size_t>& output,
        bool in_recovery) override;
    bool flush_to_l0() const override { return false; }
    ~TieredCompactionController() override =default;
private:
    TieredCompactionOptions options_;
};

// Simple 控制器
class SimpleLeveledCompactionController : public CompactionController {
public:
    explicit SimpleLeveledCompactionController(const SimpleLeveledCompactionOptions& opts):options_(opts)
    {
        
    }
    std::unique_ptr<CompactionTask> generate_compaction_task(
        const LsmStorageState& snapshot) override;
    std::vector<size_t> apply_compaction_result(
        LsmStorageState& snapshot,
        const CompactionTask& task,
        const std::vector<size_t>& output,
        bool in_recovery) override;
    bool flush_to_l0() const override { return true; }
    ~SimpleLeveledCompactionController() override =default;
private:
    SimpleLeveledCompactionOptions options_;
};

// 无压缩控制器
class NoCompactionController : public CompactionController {
public:
    NoCompactionController() = default;
    ~NoCompactionController() override = default;
    std::unique_ptr<CompactionTask> generate_compaction_task(
        const LsmStorageState& snapshot) override {
        return nullptr;
    }
    std::vector<size_t> apply_compaction_result(
        LsmStorageState& snapshot,
        const CompactionTask& task,
        const std::vector<size_t>& output,
        bool in_recovery) override {
        throw std::logic_error("NoCompaction cannot apply result");
    }
    bool flush_to_l0() const override { return true; }
};


std::unique_ptr<CompactionController> create_controller(const CompactionOptions& opts);

