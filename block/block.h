#pragma once
#include<stdint.h>
#include <string.h>
#include <memory>

#include "../key.h"
#include <assert.h>
class Block
{
public:
    char *data_;
    uint32_t len_;
    uint16_t* offset_;
    uint16_t nums;
    bool own_data = false;

public:

    Block() : data_(nullptr), len_(0), offset_(nullptr), nums(0), own_data(false) {}

    // char *encode(){
    //     uint32_t tot_len = len_+sizeof(uint16_t)*nums+sizeof(uint16_t);
    //     char *res = new char[tot_len];
    //     uint32_t offset = 0;
    //     memcpy(res+offset,data_,len_);
    //     offset+=len_;
    //     memcpy(res + offset, offset_, nums * sizeof(uint16_t));
    //     offset += nums * sizeof(uint16_t);
    //     memcpy(res + offset, &nums, sizeof(uint16_t));
    //     return res;
    // }

    static char *encode(char *data_,uint32_t len_,uint16_t* offset_,uint16_t nums,uint64_t &len){
        uint32_t tot_len = len_+sizeof(uint16_t)*nums+sizeof(uint16_t);
        char *res = new char[tot_len];
        uint32_t offset = 0;
        memcpy(res+offset,data_,len_);
        offset+=len_;
        memcpy(res + offset, offset_, nums * sizeof(uint16_t));
        offset += nums * sizeof(uint16_t);
        memcpy(res + offset, &nums, sizeof(uint16_t));
        len = tot_len;
        return res;
    }

    static Block* decode(const char *data,uint32_t len){
        uint16_t nums;
        memcpy(&nums,data + len - sizeof(uint16_t),sizeof(uint16_t));
        assert(len>sizeof(uint16_t) * (nums + 1));
        uint32_t data_len = len - sizeof(uint16_t) * (nums + 1);

        char *data_ = new char[data_len];
        memcpy(data_,data,data_len);
        uint16_t *offset_copy = new uint16_t[nums];
        memcpy(offset_copy, data + data_len, nums * sizeof(uint16_t));
        Block *b = new Block();
        b->data_ = data_;
        b->nums = nums;
        b->len_ = data_len;
        b->offset_ = offset_copy;
        b->own_data = true;
        return b;
    }

    ~Block(){
        if (own_data){
            own_data = false;
            delete []data_;
            delete []offset_;
        }
    }
};

class BlockIter
{
private:
    std::unique_ptr<Block> block_;
    Key key_;
    std::pair<uint64_t,uint64_t> value_range;
    uint16_t idx;
    Key first_key;
private:
    std::pair<int,uint64_t> parse_kv(uint64_t offset,bool is_first){
        uint64_t len = block_->len_;
        if(offset+2>len){
            return {-1,0};
        }
        const char *data = block_->data_;
        if(is_first){
            uint16_t key_len;
            memcpy(&key_len,data+offset,sizeof(uint16_t));

            uint16_t key_start = offset+sizeof(uint16_t);
            if(key_start+key_len>len){
                return {-1,0};
            }

            uint16_t ts_start = key_start+key_len;
            if(ts_start+sizeof(uint64_t)>len){
                return {-1,0};
            }
            uint64_t ts;
            memcpy(&ts,data+ts_start,sizeof(uint64_t));

            uint64_t value_len_start = ts_start+sizeof(uint64_t);
            if(value_len_start+sizeof(uint16_t)>len){
                return {-1,0};
            }
            uint16_t value_len;
            memcpy(&value_len,data+value_len_start,sizeof(uint16_t));
            uint32_t value_start = value_len_start+sizeof(uint16_t);
            if(value_start+value_len>len){
                return {-1,0};
            }
            first_key.ts = ts;
            first_key.user_key=std::string(data+key_start,key_len);
            key_ = first_key;
            return {value_start,value_len};
        }else{
            uint16_t overlap;
            memcpy(&overlap,data+offset,sizeof(uint16_t));
            uint64_t reset_len_start = offset+sizeof(uint16_t);
            if (reset_len_start+sizeof(uint16_t)>len){
                return {-1,0};
            }
            uint16_t rest_len;
            memcpy(&rest_len,data+reset_len_start,sizeof(uint16_t));
            uint64_t rest_key_start = reset_len_start+sizeof(uint16_t);
            if(rest_key_start+rest_len>len){
                return {-1,0};
            }
            std::string overlapkey(first_key.user_key.data(),overlap);
            overlapkey.append(data+rest_key_start,rest_len);
            uint64_t ts_start = rest_key_start+rest_len;
            if(ts_start+sizeof(uint64_t)>len){
                return {-1,0};
            }
            uint64_t inner_ts;
            memcpy(&inner_ts,data+ts_start,sizeof(uint64_t));
            uint64_t value_len_start = ts_start+sizeof(uint64_t);
            if(value_len_start+sizeof(uint16_t)>len){
                return {-1,0};
            }
            uint16_t value_len;
            memcpy(&value_len,data+value_len_start,sizeof(uint16_t));
            uint64_t value_start =value_len_start+sizeof(uint16_t);
            if(value_start+value_len>len){
                return {-1,0};
            }
            key_.ts=inner_ts;
            key_.user_key=overlapkey;
            return {value_start,value_len};
        }
    }

    void update_current(){
        if(idx>=block_->nums){
            return;
        }
        uint16_t offset = block_->offset_[idx];
        assert(offset<block_->len_);
        auto range_ = parse_kv(offset,idx==0);
        value_range=range_;
    }
public:
    BlockIter(std::unique_ptr<Block> block){
        block_ = std::move(block);
        idx = 0;
        value_range.first=0;
        value_range.second=0;

        if(block_->nums!=0){
            auto first_offset = block_->offset_[0];
            parse_kv(first_offset,true);
        }
    }

    static BlockIter* create_and_seek_to_first(std::unique_ptr<Block> block){
        BlockIter *res = new BlockIter(std::move(block));
        res->seek_to_first();
        return res;
    }

    static BlockIter* create_and_seek_to_key(std::unique_ptr<Block> block,const Key& key)
    {
        BlockIter *res = new BlockIter(std::move(block));
        res->seek_to_key(key);
        return res;
    }

    const Key& key() const { return key_; }

    LsmKeyView key_view() const {
        return {std::string_view(key_.user_key), key_.ts};
    }

    std::string_view value_view() const {
        return std::string_view(block_->data_ + value_range.first, value_range.second);
    }

    Value value() {
        auto v = value_view();
        return Value(std::string(v.data(), v.size()));
    }

    void seek_to_first(){
        idx = 0;
        update_current();
    }

    void next(){
        if(idx<block_->nums){
            idx+=1;
            update_current();
        }
    }

    void seek_to_key(const LsmKey& key){
        auto offsets = block_->offset_;
        uint64_t left = 0;
        uint64_t right = block_->nums;
        while (left<right)
        {
            auto mid = (left+right)/2;
            auto offset = offsets[mid];
            assert(offset<block_->len_);
            auto rang_inner = parse_kv(offset,mid==0);
            if(rang_inner.first==-1){
                right=mid;
                continue;
            }
            if(key_<key){
                left=mid+1;
            }else{
                right=mid;
            }
        }
        idx = left;
        update_current();
    }

    bool is_valid()
    {
        return idx<block_->nums && key_.user_key.length()>0;
    }

};