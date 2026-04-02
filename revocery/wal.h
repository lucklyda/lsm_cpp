#pragma once

#include <mutex>
#include "../deps/mmstore/skipmap.h"
#include "../deps/mmstore/map.h"
#include "../deps/disk/write_buffer.h"
#include "../table/table.h"
class Wal
{
private:
    /* data */
    mutable std::mutex mutex;
    std::unique_ptr<BufWriter<FileObject>> file;
public:
    Wal(const char *path){
        std::unique_ptr<FileObject> file_;
        if (FileObject::check_file(path)) {
            file_.reset(FileObject::open_inner(path));
        } else {
            file_.reset(FileObject::create(path));
        }
        if (!file_) {
            throw std::runtime_error("Failed to open file");
        }
        file = std::make_unique<BufWriter<FileObject>>(std::move(*file_));
    }

    Wal(FileObject *file_){
        file = std::make_unique<BufWriter<FileObject>>(std::move(*file_));
    }
    ~Wal()=default;

    Wal(const Wal& other) {
        std::lock_guard<std::mutex> lock_other(other.mutex);
        if (other.file) {
            other.file->flush();
            file = std::make_unique<BufWriter<FileObject>>(other.file->into_inner());
        }
    }

    static Wal*recover(const char *path,mmstore<Key,Value>& skiplist){
        FileObject *file_ = FileObject::check_file(path)?FileObject::open_inner(path):nullptr;
        if(file_==nullptr){
            return nullptr;
        }
        uint64_t size=file_->size;
        char *data = new char[size];
        if(file_->read(data,size,0)==-1){
            delete []data;
            return nullptr;
        }
        uint64_t offset = 0;
        while (offset<size)
        {
            uint32_t batch_len = *(uint32_t*)(data+offset);
            offset+=sizeof(uint32_t);
            if(offset+batch_len>size){
                delete []data;
                delete file_;
                return nullptr;
            }
            std::vector<std::pair<Key,const char*>> kv_pair;
            uint64_t inner_offset=0;
            uint32_t single_checksum=crc32c_hw(data+offset,batch_len);
            while (inner_offset<batch_len)
            {
                uint16_t key_len = *(uint16_t*)(data+offset+inner_offset);
                inner_offset+=sizeof(uint16_t);
                std::string key(data+offset+inner_offset,key_len);
                inner_offset+=key_len;
                uint64_t ts = *(uint64_t*)(data+offset+inner_offset);
                inner_offset+=sizeof(uint64_t);
                uint16_t value_len = *(uint16_t*)(data+offset+inner_offset);
                inner_offset+=sizeof(uint16_t);
                if(value_len>0){
                    std::string value(data+offset+inner_offset,value_len);
                    inner_offset+=value_len;
                    kv_pair.push_back({Key(key,ts),value.c_str()});
                }else{
                    kv_pair.push_back({Key(key,ts),nullptr});
                }
            }
            offset+=inner_offset;
            uint32_t checksum = *(uint32_t*)(data+offset);
            offset+=sizeof(uint32_t);
            if(single_checksum!=checksum){
                delete []data;
                delete file_;
                return nullptr;
            }
            for(auto &kv:kv_pair){
                skiplist.put(kv.first,Value(kv.second));
            }
        }

        return new Wal(file_);
    }

    bool put(const Key&key,const char* value){
        std::pair<Key,Value> pair(key,value);
        std::vector<std::pair<Key,Value>> pairs;
        pairs.push_back(pair);
        return put_batch(pairs);
    }

    bool put_batch(const std::vector<std::pair<Key,Value>> &pairs)
    {
        mutex.lock();
        CharBuffer buf;
        for(auto &pair:pairs){
            uint16_t key_len = pair.first.user_key.length();
            buf.append((char*)&key_len,sizeof(uint16_t));
            buf.append(pair.first.user_key.data(),key_len);
            buf.append((char*)&pair.first.ts,sizeof(uint64_t));
            uint16_t value_len=0;
            value_len=pair.second.size();
            buf.append((char*)&value_len,sizeof(uint16_t));
            if(value_len>0)buf.append(pair.second.to_data(),value_len);
        }
        uint32_t len = buf.size();
        file->write((char*)&len,sizeof(uint32_t));
        file->write(buf.data(),len);
        uint32_t checksum=crc32c_hw(buf.data(),len);
        file->write((char*)&checksum,sizeof(uint32_t));
        mutex.unlock();
        return true;
    }

    bool sync(){
        mutex.lock();
        file->flush();
        mutex.unlock();
        return true;
    }
};

