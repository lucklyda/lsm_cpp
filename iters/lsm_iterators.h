#pragma once
#include "../iterators.h"
#include "../deps/mmstore/mmstore.h"
#include <memory>

class LsmIterator:public Iterators
{
private:
    std::unique_ptr<Iterators> inner_;
    Bound<LsmKey> upper_bound_;
    bool is_valid_;
    uint64_t read_ts_;
    LsmKey pre_key_;
private:
    bool next_inner(){
        inner_->next();
        if(!inner_->is_valid()){
            is_valid_ = false;
            return true;
        }
        switch (upper_bound_.type)
        {
        case 0:{
        } break;
        case 1:{
            is_valid_ = inner_->key().user_key<=upper_bound_.key.user_key;
        } break;
        case 2:{
            is_valid_ = inner_->key().user_key<upper_bound_.key.user_key;
        } break;
        default:
            break;
        }
        return true;
    }
    bool move_to_non_delete(){
        while (true)
        {
            while(is_valid() && inner_->key().user_key==pre_key_.user_key){
                next_inner();
            }
            if(!is_valid())break;
            pre_key_.user_key = inner_->key().user_key;
            while (is_valid() && inner_->key().user_key==pre_key_.user_key
                && inner_->key().ts>read_ts_)
            {
                next_inner();
            }
            if(!is_valid())break;
            if(inner_->key().user_key!=pre_key_.user_key)continue;
            if(!inner_->value().is_empty())break;
        }
        return true;
    }
public:
    LsmIterator(std::unique_ptr<Iterators> inner,Bound<LsmKey> upper_bound,uint64_t read_ts){
        inner_ = std::move(inner);
        upper_bound_ = upper_bound;
        read_ts_ = read_ts;
        is_valid_ = inner_->is_valid();
        move_to_non_delete();
    }
    ~LsmIterator()=default;

    const Key& key()const override{
        return inner_->key();
    }
    Value value()const override{
        return inner_->value();
    }
    bool is_valid(){
        return is_valid_;
    }
    bool next(){
        next_inner();
        move_to_non_delete();
        return true;
    }

    uint32_t num_active_iterators() override {
        return 1;
    }
};


class FusedIterator:public Iterators
{
private:
    std::unique_ptr<Iterators> iter_;
    bool has_errored;
public:
    FusedIterator(std::unique_ptr<Iterators> iter){
        iter_ = std::move(iter);
        has_errored = false;
    }
    ~FusedIterator() = default;

    const Key& key()const{
        return iter_->key();
    }
    Value value()const{
        return iter_->value();
    }
    bool is_valid(){
        return !has_errored && iter_->is_valid();
    }
    bool next(){
        if(has_errored){
            throw std::logic_error("the iterator is tainted");
        }
        if(iter_->is_valid()){
            try
            {
                iter_->next();
            }
            catch(const std::exception& e)
            {
                has_errored = true;
            }
        }
        return true;
    }
};



