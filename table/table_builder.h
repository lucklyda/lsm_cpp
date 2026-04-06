#pragma once
#include "table.h"

class TableBuilder
{
public:
    std::unique_ptr<BlockBuilder> block_builder;
    Key first_key;
    Key last_key;
    CharBuffer data;
    std::vector<BlockMeta> metas;
    uint64_t block_size;
    std::vector<uint32_t> key_hashes;
    uint64_t max_ts;
public:
    TableBuilder(uint64_t block_size_)
    {
        block_builder = std::make_unique<BlockBuilder>(block_size_-6);
        block_size = block_size_;
        max_ts=0;
        first_key = Key();
        last_key = Key();
    }
    ~TableBuilder() = default;

    void add(const Key& key,Value value){
        if(key.ts>max_ts){
            max_ts=key.ts;
        }
        if(block_builder->add(key,value)){
            if(first_key.user_key.length()==0){
                first_key=key;
            }
            last_key=key;
        }else{
            uint64_t len=0;
            char *block_data = block_builder->build_data(len);
            uint32_t checksum = crc32c_hw(block_data,len);
            uint64_t offset = data.size();
            data.append(block_data,len);
            data.append((char*)&checksum,sizeof(uint32_t));
            BlockMeta meta;
            meta.offset = offset;
            meta.first_key=first_key;
            meta.last_key = last_key;
            metas.push_back(meta);
            block_builder = std::make_unique<BlockBuilder>(block_size-6);
            block_builder->add(key,value);
            first_key = key;
            last_key=key;
            delete []block_data;
        }
        key_hashes.push_back(Fingerprint32(key.user_key.data(),key.user_key.length()));
    }

    uint64_t estimated_size(){
        return data.size();
    }
    bool is_empty(){
        return block_builder->is_empty() && data.size()==0;
    }

    std::unique_ptr<Sstable> build(uint64_t id,const char *path,
        std::shared_ptr<BlockCache<BlockKey,std::shared_ptr<Block>>> block_cache=nullptr)
    {
        if(!block_builder->is_empty()){
            BlockMeta meta;
            meta.offset = data.size();
            meta.first_key = first_key;
            meta.last_key = last_key;
            uint64_t len=0;
            char *block_data = block_builder->build_data(len);
            uint32_t checksum = crc32c_hw(block_data,len);
            data.append(block_data,len);
            data.append((char*)&checksum,sizeof(uint32_t));
            metas.push_back(meta);
            block_builder=std::make_unique<BlockBuilder>(block_size-6);
        }

        auto bloom = Bloom::build_from_key_hashes(key_hashes,
            Bloom::bloom_bits_per_key(key_hashes.size(), 0.01));
        uint64_t tot_len=0;
        Key first_key;
        Key last_key;
        uint16_t nums = metas.size();
        if(nums>0){
            first_key = metas[0].first_key;
            last_key=metas[nums-1].last_key;
        }
        char *meta_data = BlockMeta::encode_block_meta(metas,max_ts,tot_len);
        std::vector<uint8_t> bloom_data;
        bloom.encode(bloom_data);
        uint32_t data_len = data.size();
        CharBuffer buf=std::move(data);
        buf.append(meta_data,tot_len);
        delete []meta_data;
        buf.append((char*)&data_len,sizeof(uint32_t));
        uint32_t bloom_offset = buf.size();
        buf.append((const char*)bloom_data.data(),bloom_data.size()*sizeof(uint8_t));
        buf.append((char*)&bloom_offset,sizeof(uint32_t));
        FileObject *file = FileObject::create(path);
        if(file==nullptr){
            return nullptr;
        }
        file->write(buf.data(),buf.size());
        auto sstable = new Sstable(std::unique_ptr<FileObject>(file),
                    metas,data_len,id,first_key,last_key,std::make_unique<Bloom>(bloom),max_ts,block_cache);
        return std::unique_ptr<Sstable>(sstable);
    }
};



