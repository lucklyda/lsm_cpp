#include "block.h"
#include <assert.h>
#include <vector>


class BlockBuilder
{
private:
    /* data */
    std::vector<uint16_t> offsets_;
    CharBuffer buf;
    //char *data;
    uint64_t block_size;
    Key first_key;
    //uint64_t current_size;
public:
    BlockBuilder(uint64_t block_size_)
    {
        block_size = block_size_;
        buf=CharBuffer(block_size);
        //data = new char[block_size];
       // current_size = 0;
    }
    ~BlockBuilder()
    {
        //delete []data;
    }
    
    uint16_t compute_overlap(const LsmKey &a,const LsmKey &b){
        uint16_t res = 0;
        u_int64_t a_len = a.user_key.length();
        u_int64_t b_len = b.user_key.length();
        while (res<a_len && res<b_len)
        {
            if(a.user_key[res]!=b.user_key[res])break;
            res++;
        }
        return res;
    }

    u_int64_t estimated_size(){
        return buf.size()+offsets_.size()*sizeof(uint16_t)+sizeof(uint16_t);
    }

    bool add(const Key& key,Value value){
        uint16_t current_size = buf.size();
        if(offsets_.size() == 0){
            uint16_t key_len = key.user_key.length();
            uint16_t value_len = value.size();
            // uint32_t write_len = key_len+value_len+sizeof(uint16_t)*2
            //                     +sizeof(uint64_t);
            buf.append((char*)&key_len,sizeof(uint16_t));
            buf.append(key.user_key.data(),key_len);
            buf.append((char*)&key.ts,sizeof(uint64_t));
            buf.append((char*)&value_len,sizeof(uint16_t));
            buf.append(value.to_data(),value_len);
            first_key = key;
            offsets_.push_back(current_size);
        }else{
            uint16_t overlap = compute_overlap(first_key,key);
            uint16_t key_len = key.user_key.length();
            uint16_t rest_len = key_len-overlap;
            uint16_t value_len = value.size();
            uint32_t write_len = key_len-overlap+value_len+sizeof(uint16_t)*3
                                +sizeof(uint64_t);
            if(write_len+estimated_size()>block_size){
                return false; 
            }
            buf.append((char*)&overlap,sizeof(uint16_t));
            buf.append((char*)&rest_len,sizeof(uint16_t));
            buf.append(key.user_key.data()+overlap,rest_len);
            buf.append((char*)&key.ts,sizeof(uint64_t));
            buf.append((char*)&value_len,sizeof(uint16_t));
            buf.append(value.to_data(),value_len);
            offsets_.push_back(current_size);
        }
        return true;
    }

    bool is_empty(){
        return buf.size()==0;
    }
    // Block* build(){
    //     Block *res=new Block();
    //     res->data_ = new char[current_size];
    //     res->len_ = current_size;
    //     res->offset_ = new uint16_t[offsets_.size()];
    //     for(uint64_t i=0;i<offsets_.size();++i){
    //         res->offset_[i]=offsets_[i];
    //     }
    //     res->nums = offsets_.size();
    //     res->own_data = true;
    //     buf.clear();
    //     offsets_.clear();
    //     current_size=0;
    //     first_key.user_key.clear();
    //     return res;
    // }

    char *build_data(uint64_t &len){
        char *res = Block::encode(buf.data(),buf.size(),offsets_.data(),offsets_.size(),len);
        buf.clear();
        offsets_.clear();
        first_key.user_key.clear();
        return res;
    }
};

