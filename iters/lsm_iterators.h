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
    std::string pre_user_;
private:
    bool next_inner(){
        inner_->next();
        if(!inner_->is_valid()){
            is_valid_ = false;
            return true;
        }
        auto ik = inner_->key_view();
        switch (upper_bound_.type)
        {
        case 0:{
        } break;
        case 1:{
            is_valid_ = ik.user_key <= std::string_view(upper_bound_.key.user_key);
        } break;
        case 2:{
            is_valid_ = ik.user_key < std::string_view(upper_bound_.key.user_key);
        } break;
        default:
            break;
        }
        return true;
    }
    bool move_to_non_delete(){
        while (true)
        {
            while(is_valid() && inner_->key_view().user_key==pre_user_){
                next_inner();
            }
            if(!is_valid())break;
            pre_user_ = std::string(inner_->key_view().user_key);
            while (is_valid() && inner_->key_view().user_key==pre_user_
                && inner_->key_view().ts>read_ts_)
            {
                next_inner();
            }
            if(!is_valid())break;
            if(inner_->key_view().user_key!=pre_user_)continue;
            if(!inner_->value_view().empty())break;
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

    LsmKeyView key_view() const override{
        return inner_->key_view();
    }
    std::string_view value_view() const override{
        return inner_->value_view();
    }
    bool is_valid() override{
        return is_valid_;
    }
    bool next() override{
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

    LsmKeyView key_view() const override {
        return iter_->key_view();
    }
    std::string_view value_view() const override {
        return iter_->value_view();
    }
    bool is_valid() override {
        return !has_errored && iter_->is_valid();
    }
    bool next() override {
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



