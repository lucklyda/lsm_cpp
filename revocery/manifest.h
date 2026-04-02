#pragma once

#include <mutex>
#include <string.h>

#include "../compact/compact.h"
#include "../table/table.h"

struct ManifestRecord
{
    uint8_t type_;
    uint32_t tot_len_;
    ManifestRecord(){
        type_ = 0;
    }
    ManifestRecord(uint8_t type){
        type_ = type;
    }
    virtual uint32_t encode(char *data)const {
        memcpy(data,&type_,sizeof(uint8_t));
        memcpy(data+sizeof(uint8_t),&tot_len_,sizeof(uint32_t));
        return sizeof(uint8_t)+sizeof(uint32_t);
    }
    virtual void decode(const char*data){
        type_ = *(uint8_t*)(data);
        tot_len_ = *(uint32_t*)(data+sizeof(uint8_t));
    }
};

struct FlushRecord:public ManifestRecord
{
    uint64_t id;
    FlushRecord(){
      type_=2;   
    }
    FlushRecord(uint64_t id_){
        type_=2;
        id=id_;
        tot_len_=sizeof(uint64_t)+sizeof(uint8_t)+sizeof(uint32_t);
    }
    virtual uint32_t encode(char *data)const {
        auto offset1 = ManifestRecord::encode(data);
        memcpy(data+offset1,&id,sizeof(uint64_t));
        return offset1+sizeof(uint64_t);
    }
    virtual void decode(const char*data){
        ManifestRecord::decode(data);
        id = *(uint64_t*)(data+5);
        tot_len_=sizeof(uint64_t)+sizeof(uint8_t)+sizeof(uint32_t);
    }
};

struct NewMemtableRecord:public ManifestRecord
{
    uint64_t id;
    NewMemtableRecord(){
        type_ = 1;
    }
    NewMemtableRecord(uint64_t id_){
        type_=1;
        id=id_;
        tot_len_=sizeof(uint64_t)+sizeof(uint8_t)+sizeof(uint32_t);
    }
    virtual uint32_t encode(char *data)const {
        auto offset1 = ManifestRecord::encode(data);
        memcpy(data+offset1,&id,sizeof(uint64_t));
        return offset1+sizeof(uint64_t);
    }
    virtual void decode(const char*data){
        ManifestRecord::decode(data);
        id = *(uint64_t*)(data+5);
        tot_len_=sizeof(uint64_t)+sizeof(uint8_t)+sizeof(uint32_t);
    }
};
struct CompactionRecord : public ManifestRecord
{
    std::unique_ptr<CompactionTask> task_;
    std::vector<uint64_t> output_;

    CompactionRecord(){
        type_=3;
    }

    CompactionRecord(std::unique_ptr<CompactionTask> task,const std::vector<uint64_t> &output)
    {
        task_ = std::move(task);
        task_->caluc_len();
        output_ = output;
        type_=3;
        tot_len_=5+task_->tot_len_+sizeof(uint32_t)+output_.size()*sizeof(uint64_t);
    }

    uint32_t encode(char *data)const override
    {
        uint32_t offset = ManifestRecord::encode(data);
        task_->encode(data+offset);
        offset+=task_->tot_len_;
        uint32_t size1 = output_.size();
        memcpy(data+offset,&size1,sizeof(uint32_t));
        offset+=sizeof(uint32_t);
        for(auto &o:output_){
            memcpy(data+offset,&o,sizeof(uint64_t));
            offset+=sizeof(uint64_t);
        }
        return offset;
    }

    void decode(const char* data) override
    {
        ManifestRecord::decode(data);
        uint32_t offset = 5;
        uint8_t inner_type = *(uint8_t*)(data+offset);
        switch (inner_type)
        {
        case 1:{
            task_ = std::make_unique<LeveledCompactionTask>();
            task_->decode(data+offset);
        }break;
        case 2:{
            task_ = std::make_unique<TieredCompactionTask>();
            task_->decode(data+offset);
        }break;
        case 3:{
            task_ = std::make_unique<SimpleLeveledCompactionTask>();
            task_->decode(data+offset);
        }break;
        default:
            break;
        }
        offset+=task_->tot_len_;
        uint32_t size1 = *(uint32_t*)(data+offset);
        offset+=sizeof(uint32_t);
        for(uint32_t i=0;i<size1;++i){
            output_.push_back(*(uint64_t*)(data+offset));
            offset+=sizeof(uint64_t);
        }
        tot_len_ = 5+task_->tot_len_;
    }
};


class Manifest
{
private:
    /* data */
    FileObject *file_;
    std::mutex lock;
public:
    Manifest(const char*path){
        file_ = FileObject::check_file(path)?FileObject::open_inner(path):FileObject::create(path);
    }

    Manifest(FileObject *file){
        file_ = file;
    }
    ~Manifest(){
        delete file_;
    }

    static std::pair<Manifest*,std::vector<std::unique_ptr<ManifestRecord>>> recover(const char*path){
        if(!FileObject::check_file(path)){
            throw std::logic_error("file not find");
        }
        auto file = FileObject::open_inner(path);
        uint64_t file_size = file->size;
        char *data = new char[file_size];
        if(file->read(data,file_size,0)==-1){
            delete data;
            throw std::logic_error("read error");
        }
        std::vector<std::unique_ptr<ManifestRecord>> record;
        uint64_t offset = 0;
        while (offset<file_size)
        {
            if(offset+5>=file_size){
                break;
            }
            uint8_t type = *(uint8_t*)(data+offset);
            std::unique_ptr<ManifestRecord> res;
            
            if(type==1){ res= std::make_unique<NewMemtableRecord>();
            }else if(type==2){res= std::make_unique<FlushRecord>();
            }else if(type == 3){res= std::make_unique<CompactionRecord>();
            }else{throw std::logic_error("No Such Type");
            }

            res->decode(data+offset);
            uint32_t inner_cheksum = crc32c_hw(data+offset,res->tot_len_);
            offset+=res->tot_len_;
            uint32_t checksum = *(uint32_t*)(data+offset);
            offset+=sizeof(uint32_t);
            if(inner_cheksum!=checksum){
                delete []data;
                throw std::logic_error("CHeck Sum Error");
            }

            record.push_back(std::move(res));

        }
        
        return {new Manifest(file),std::move(record)};
    }

    bool add_record(const ManifestRecord& record){
        std::scoped_lock<std::mutex> gurad{lock};
        char *data = new char[record.tot_len_];
        record.encode(data);
        uint32_t checksum = crc32c_hw(data,record.tot_len_);
        file_->write(data,record.tot_len_);
        file_->write((char*)&checksum,sizeof(uint32_t));
        delete []data;
        return true;
    }
};






