#pragma once
#include "../block/block.h"
#include "../block/block_builder.h"
#include "bloom.h"
#include <unistd.h> 
#include <fcntl.h>  
#include <sys/stat.h>
#include "../iterators.h"
class BlockMeta
{
public:
    uint64_t offset;
    Key first_key;
    Key last_key;
public:
    BlockMeta(/* args */) = default;
    ~BlockMeta() = default;
    static char* encode_block_meta(const std::vector<BlockMeta>& metas,uint64_t max_ts,uint64_t &len){
        uint16_t nums = metas.size();
        // uint64_t estimate_size = sizeof(uint16_t)+
        //     sizeof(uint64_t)+nums*(sizeof(uint64_t)+sizeof(uint64_t)+sizeof(uint16_t)+sizeof(uint16_t)+sizeof(uint64_t));

        uint64_t estimate_size =10+28*nums;
        for(const BlockMeta& meta:metas){
            estimate_size+=meta.first_key.user_key.length()+meta.last_key.user_key.length();
        }
        char *res = new char[estimate_size];
        memcpy(res,&nums,sizeof(uint16_t));
        uint64_t offset=sizeof(uint16_t);
        
        for(const BlockMeta& meta:metas){
            memcpy(res+offset,&meta.offset,sizeof(uint64_t));
            offset+=sizeof(uint64_t);
            uint16_t first_key_len=meta.first_key.user_key.length();
            uint16_t last_key_len=meta.last_key.user_key.length();
            memcpy(res+offset,&first_key_len,sizeof(uint16_t));
            offset+=sizeof(uint16_t);
            memcpy(res+offset,meta.first_key.user_key.data(),first_key_len);
            offset+=first_key_len;
            memcpy(res+offset,&meta.first_key.ts,sizeof(uint64_t));
            offset+=sizeof(uint64_t);
            memcpy(res+offset,&last_key_len,sizeof(uint16_t));
            offset+=sizeof(uint16_t);
            memcpy(res+offset,meta.last_key.user_key.data(),last_key_len);
            offset+=last_key_len;
            memcpy(res+offset,&meta.last_key.ts,sizeof(uint64_t));
            offset+=sizeof(uint64_t);
        }
        memcpy(res+offset,&max_ts,sizeof(uint64_t));
        len = estimate_size;
        return res;
    }

   static std::pair<std::vector<BlockMeta>,uint64_t> decode_block_meta(const char*data,uint64_t len){
        std::vector<BlockMeta> res;
        if(len<2){
            return {res,0};
        }
        uint16_t nums;
        memcpy(&nums,data,sizeof(uint16_t));
        // uint16_t nums = *(uint16_t*)(data);
        uint64_t offset=sizeof(uint16_t);
        for(uint16_t i=0;i<nums;++i){
            res.push_back(decode_one_meta(data,offset));
        }
        uint64_t max_ts;
        memcpy(&max_ts,data+offset,sizeof(uint64_t));
        return {res,max_ts};
    }
   //TOOD 安全检查
   static BlockMeta decode_one_meta(const char *data,uint64_t &offset){
        BlockMeta t;
       // t.offset = *(uint64_t*)(data+offset);
        memcpy(&t.offset,data+offset,sizeof(uint64_t));
        offset+=sizeof(uint64_t);
        uint16_t key_len;
        memcpy(&key_len,data+offset,sizeof(uint16_t));
       // uint16_t key_len = *(uint16_t*)(data+offset);
        offset+=sizeof(uint16_t);
        t.first_key.user_key = std::string(data+offset,key_len);
        offset+=key_len;
        memcpy(&t.first_key.ts,data+offset,sizeof(uint64_t));
        //t.first_key.ts=*(uint64_t*)(data+offset);
        offset+=sizeof(uint64_t);
        memcpy(&key_len,data+offset,sizeof(uint16_t));
        //key_len =*(uint16_t*)(data+offset);
        offset+=sizeof(uint16_t);
        t.last_key.user_key = std::string(data+offset,key_len);
        offset+=key_len;
        memcpy(&t.last_key.ts,data+offset,sizeof(uint64_t));
        //t.last_key.ts=*(uint64_t*)(data+offset);
        offset+=sizeof(uint64_t);
        return t;
    }
};

class FileObject
{
public:
    int fd = -1;    
    uint64_t size = 0;
    std::string path_; // for inner delete file
    mutable std::mutex lock;

public:
    FileObject() = default;
    FileObject(const FileObject&) = delete;
    FileObject& operator=(const FileObject&) = delete;

    FileObject(FileObject&& other) noexcept : fd(other.fd), size(other.size) {
        other.fd = -1;
        other.size = 0;
    }
    FileObject& operator=(FileObject&& other) noexcept {
        if (this != &other) {
            if (fd != -1) close(fd);
            fd = other.fd;
            size = other.size;
            other.fd = -1;
            other.size = 0;
        }
        return *this;
    }

    static FileObject* create(const char* path) {
        int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd == -1) return nullptr;
        return new FileObject(fd,path);
    }

    static FileObject* open_inner(const char* path) {
        int fd = open(path, O_RDWR);
        if (fd == -1) return nullptr;
        return new FileObject(fd,path);
    }

    static bool check_file(const char* path) {
        struct stat st{};
        if (stat(path, &st) != 0) return false;
        return S_ISREG(st.st_mode);
    }

    static bool delete_file(const char* path) {
        return std::remove(path) == 0;
    }

    static bool check_dir(const char* path) {
        struct stat st{};
        if (stat(path, &st) != 0) return false;
        return S_ISDIR(st.st_mode);
    }

    static bool create_dir(const char* path) {
        if (mkdir(path, 0755) == 0) return true;
        if (errno == EEXIST && check_dir(path)) return true;
        return false;
    }

    explicit FileObject(int fd_,const char*path) : fd(fd_),path_(path) {
        struct stat st{};
        if (fstat(fd_, &st) == 0) {
            size = static_cast<uint64_t>(st.st_size);
        } else {
            size = 0;
        }
    }

    void inner_close(){
        if (fd != -1) {
            close(fd);
            fd = -1; 
        }
    }

    ~FileObject() {
        if (fd != -1) {
            close(fd);
            fd = -1; 
        }
    }

    //涉及到多个线程的读写
    ssize_t read(char* data, uint64_t len, uint64_t offset) {
        std::scoped_lock<std::mutex> gurad{lock};
        if (fd == -1 || !data) return -1;
        lseek(fd, static_cast<off_t>(offset), SEEK_SET);
        return ::read(fd, data, static_cast<size_t>(len));
    }

    ssize_t write(const char* data, uint64_t len) {
        std::scoped_lock<std::mutex> gurad{lock};
        if (fd == -1 || !data) return -1;
        lseek(fd, 0, SEEK_END);
        ssize_t r = ::write(fd, data, static_cast<size_t>(len));
        if (r > 0) size += static_cast<uint64_t>(r);
        return r;
    }
};

class Sstable
{
    friend class TableBuilder;
private:
    std::shared_ptr<FileObject> file;
    std::vector<BlockMeta> metas;
    uint64_t block_meta_offset;
    uint64_t id;
    Key first_key;
    Key last_key;
    std::shared_ptr<Bloom> bloom;
    uint64_t max_ts_;
public:
    Sstable()=default;
    Sstable(std::unique_ptr<FileObject> file_,std::vector<BlockMeta> metas_,
       uint64_t block_meta_offset_, uint64_t id_,Key first_key_,Key last_key_,
        std::unique_ptr<Bloom> bloom_,uint64_t max_ts):
        file(std::move(file_)),metas(metas_),block_meta_offset(block_meta_offset_),
        id(id_),first_key(first_key_),last_key(last_key_),bloom(std::move(bloom_)),max_ts_(max_ts)
    {
    }

    ~Sstable() = default;
    // {
    //     if(file && file->fd!=-1){
    //         std::string path = file->path_;
    //         file->inner_close()
    //         FileObject::delete_file(path.c_str());

    //     }
    // }

    static Sstable* open(uint64_t id,std::unique_ptr<FileObject> file_){
        // auto len = file_->size;
        // char *data = new char[sizeof(uint32_t)];
        // if(file_->read(data,sizeof(uint32_t),len-sizeof(uint32_t))==-1){
        //     delete []data;
        //     return nullptr;
        // }
        // uint64_t bloom_offset = *(uint32_t*)data;
        // delete []data;
        // uint64_t bloom_len = len-sizeof(uint32_t)-bloom_offset;
        // data = new char[bloom_len];
        // if(file_->read(data,bloom_len,bloom_offset)==-1){
        //     delete []data;
        //     return nullptr;
        // }
        // auto bloom =new Bloom(Bloom::decode(data,bloom_len));
        // delete []data;
        // data = new char[sizeof(uint32_t)];
        // if(file_->read(data,sizeof(uint32_t),bloom_offset-sizeof(uint32_t))==-1){
        //     delete []data;
        //     return nullptr;
        // }
        // uint32_t data_len = *(uint32_t*)data;
        // delete []data;
        // auto meta_offset_ = data_len;
        // auto meta_len = bloom_offset - meta_offset_ - sizeof(uint32_t);
        // data = new char[meta_len];
        // if(file_->read(data,meta_len,meta_offset_)==-1){
        //     delete []data;
        //     return nullptr;
        // }
        // auto metadecode = BlockMeta::decode_block_meta(data,meta_len);
        // delete []data;
        // Key first_key;
        // Key last_key;
        // auto meta_nums = metadecode.first.size();
        // if(meta_nums>0){
        //     first_key = metadecode.first[0].first_key;
        //     last_key = metadecode.first[meta_nums-1].last_key;
        // }
        // return new Sstable(std::move(file_),metadecode.first,
        //         meta_offset_,id,first_key,last_key,std::unique_ptr<Bloom>(bloom),metadecode.second);

        auto len = file_->size;
        uint32_t bloom_offset;
        if (file_->read(reinterpret_cast<char*>(&bloom_offset), sizeof(bloom_offset),
                        len - sizeof(bloom_offset)) != sizeof(bloom_offset)) {
            return nullptr;
        }

        if (bloom_offset >= len) {
            return nullptr;
        }

        uint64_t bloom_len = len - sizeof(bloom_offset) - bloom_offset;
        if (bloom_len == 0) {
            return nullptr;
        }

        std::vector<char> bloom_data(bloom_len);
        if (file_->read(bloom_data.data(), bloom_len, bloom_offset) != static_cast<int64_t>(bloom_len)) {
            return nullptr;
        }
        auto bloom = std::make_unique<Bloom>(Bloom::decode(bloom_data.data(), bloom_len));
        if (!bloom) {
            return nullptr;
        }

        uint32_t data_len;
        uint64_t data_len_offset = bloom_offset - sizeof(data_len);
        if (file_->read(reinterpret_cast<char*>(&data_len), sizeof(data_len), data_len_offset) != sizeof(data_len)) {
            return nullptr;
        }
        uint64_t meta_offset = data_len;
        uint64_t meta_len = bloom_offset - meta_offset - sizeof(data_len);
        if (meta_len == 0) {
            return nullptr;
        }
        std::vector<char> meta_data(meta_len);
        if (file_->read(meta_data.data(), meta_len, meta_offset) != static_cast<int64_t>(meta_len)) {
            return nullptr;
        }

        auto [metas, max_ts] = BlockMeta::decode_block_meta(meta_data.data(), meta_len);
        
        Key first_key;
        Key last_key;
        if (!metas.empty()) {
            first_key = metas.front().first_key;
            last_key  = metas.back().last_key;
        }
        
        return new Sstable(std::move(file_),std::move(metas),
                meta_offset,id,first_key,last_key,std::move(bloom),max_ts);
    }
    

    std::unique_ptr<Block> read_block(uint16_t idx){
        if(idx>=metas.size()){
            return nullptr;
        }
        auto block_offset = metas[idx].offset;
        uint64_t read_len=0;
        if(idx<metas.size()-1){
            read_len = metas[idx+1].offset-block_offset;
        }else{
            read_len = block_meta_offset-block_offset;
        }
        char *data = new char[read_len];
        ssize_t read_size = file->read(data,read_len,block_offset);
        if(read_size==-1){
            delete []data;
            return nullptr;
        }
        assert(read_size == static_cast<ssize_t>(read_len));
        uint32_t checksum = *(uint32_t*)(data+read_len-4);
        auto data_check_sum = crc32c_hw(data,read_len-4);
        if(checksum!=data_check_sum){
            delete []data;
            return nullptr;
        }
        auto block = Block::decode(data,read_len-4);
        delete []data;
        return std::unique_ptr<Block>(block);
    }

    uint16_t find_block_idx(const Key& key){
        uint16_t left=0;
        uint16_t right=metas.size();
        while (left<right)
        {
            auto mid = (left+right)/2;
            const BlockMeta& mid_meta = metas[mid];
            if(mid_meta.first_key>key){
                right = mid;
            }else{
                left=mid+1;
            }
        }
        return left==0?0:left-1;
    }

    uint16_t num_of_blocks(){
        return metas.size();
    }

    const Key& first_key_(){
        return first_key;
    }
    const Key& last_key_(){
        return last_key;
    }
    uint64_t table_size(){
        return file->size;
    }
    uint64_t sst_id(){
        return id;
    }
    uint64_t max_ts(){
        return max_ts_;
    }

    Bloom* get_bloom(){
        return bloom.get();
    }
};


class SsTableIterator: public Iterators
{
private:
    /* data */
    Sstable* table;
    std::unique_ptr<BlockIter> blk_iter;
    uint16_t blk_idx;
    Key upper_bound;
    bool is_bigt_upper;
public:
    SsTableIterator(/* args */) = default;
    ~SsTableIterator() = default;

    static SsTableIterator* create_and_seek_to_first(Sstable* table_)
    {
        SsTableIterator *res = new SsTableIterator();
        res->table = table_;
        if(res->seek_to_first()){
            return res;
        }
        delete res;
        return nullptr;
    }

    bool seek_to_first(){
        auto block = table->read_block(0);
        if(block==nullptr){
            return false;
        }
        blk_iter = std::unique_ptr<BlockIter>(BlockIter::create_and_seek_to_first(std::move(block)));
        blk_idx=0;
        is_bigt_upper=false;
        check_upper();
        return true;
    }

    static SsTableIterator* create_and_seek_to_key(Sstable* table_,const Key& key)
    {
        auto blk_idx = table_->find_block_idx(key);
        auto block = table_->read_block(blk_idx);
        SsTableIterator *res = new SsTableIterator();
        res->blk_idx=blk_idx;
        res->table = table_;
        res->blk_iter=std::unique_ptr<BlockIter>(BlockIter::create_and_seek_to_key(std::move(block),key));
        res->is_bigt_upper=false;
        while (!res->blk_iter->is_valid())
        {
            if(!res->blk_iter->is_valid() && blk_idx<res->table->num_of_blocks()-1){
                blk_idx+=1;
                res->blk_iter=std::unique_ptr<BlockIter>(
                            BlockIter::create_and_seek_to_key(res->table->read_block(blk_idx),Key()));
            }else{
                break;
            }
        }
        res->check_upper();
        return res;
    }

    bool seek_to_key(const Key&key){
        return seek_to_range(key,Key());
    }

    static SsTableIterator*create_and_seek_to_range(Sstable* table_,const Key&lower_,const Key&upper_)
    {
        SsTableIterator *res=nullptr;
        if(lower_.user_key.length()==0){
            res = SsTableIterator::create_and_seek_to_first(table_);
            res->upper_bound=upper_;
        }else{
            auto blk_idx = table_->find_block_idx(lower_);
            auto block = table_->read_block(blk_idx);
            res = new SsTableIterator();
            res->blk_idx=blk_idx;
            res->table = table_;
            res->upper_bound=upper_;
            res->blk_iter=std::unique_ptr<BlockIter>(BlockIter::create_and_seek_to_key(std::move(block),lower_));
            res->is_bigt_upper=false;
            while (!res->blk_iter->is_valid())
            {
                if(!res->blk_iter->is_valid() && blk_idx<res->table->num_of_blocks()-1){
                    blk_idx+=1;
                    res->blk_iter=std::unique_ptr<BlockIter>(
                                BlockIter::create_and_seek_to_key(res->table->read_block(blk_idx),Key()));
                }else{
                    break;
                }
            }
        }
        res->check_upper();
        return res;
    }

    bool seek_to_range(const Key&lower,const Key&upper)
    {
        upper_bound = upper;
        is_bigt_upper = false;
        if(lower.user_key.length()>0)
        {
            blk_idx=table->find_block_idx(lower);
            auto block=table->read_block(blk_idx);
            if(block==nullptr){
                return false;
            }
            blk_iter=std::unique_ptr<BlockIter>(BlockIter::create_and_seek_to_key(std::move(block),lower));
            while (blk_iter && !blk_iter->is_valid())
            {
                if(blk_idx<table->num_of_blocks()-1){
                    blk_idx+=1;
                    blk_iter=std::unique_ptr<BlockIter>(
                        BlockIter::create_and_seek_to_key(table->read_block(blk_idx),lower));
                }else{
                    break;
                }
            }
        }else{
            seek_to_first();
        }
        check_upper();
        return blk_iter!=nullptr;
    }

    void check_upper()
    {
        if(upper_bound.user_key.length()>0){
            is_bigt_upper = blk_iter && blk_iter->is_valid() && !(upper_bound<blk_iter->key());
        }else{
            is_bigt_upper=false;
        }
    }

    const Key& key()const override{
        return blk_iter->key();
    }
    Value value()const override{
        return blk_iter->value();
    }

    bool is_valid() override{
        return blk_iter->is_valid() && !is_bigt_upper;
    }

    bool next()override{
        blk_iter->next();
        if(!blk_iter->is_valid() && blk_idx<table->num_of_blocks()-1){
            blk_idx+=1;
            blk_iter=std::unique_ptr<BlockIter>(
                        BlockIter::create_and_seek_to_key(table->read_block(blk_idx),Key()));
        }
        check_upper();
        return true;

    }

    uint32_t num_active_iterators() override
    {
        return is_valid()?1:0;
    }




};





