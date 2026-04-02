#pragma once
#include "../iterators.h"
#include <memory>

class TwoMergeIterator : public Iterators {
public:
    TwoMergeIterator(std::unique_ptr<Iterators> a, std::unique_ptr<Iterators> b)
        : a_(std::move(a)), b_(std::move(b)) {
        skip_b_if_equal();
    }

    void skip_b_if_equal() {
        while (a_->is_valid() && b_->is_valid() && 
               a_->key()==b_->key()) 
        {
            b_->next();
        }
    }
    bool choose_a() const {
        if (!a_->is_valid()) return false;
        if (!b_->is_valid()) return true;
        return a_->key()<b_->key();
    }

    bool is_valid() override {
        return a_->is_valid() || b_->is_valid();
    }

    const Key& key() const override {
        return choose_a() ? a_->key() : b_->key();
    }

    Value value() const override {
        return choose_a() ? a_->value() : b_->value();
    }

    bool next() override {
        if (!is_valid()) return true;

        if (choose_a()) {
            a_->next();
        } else {
            b_->next();
        }

        skip_b_if_equal(); // 跳过重复 key
        return true;
    }

    uint32_t num_active_iterators() override {
        return a_->num_active_iterators() + b_->num_active_iterators();
    }

private:
    std::unique_ptr<Iterators> a_;
    std::unique_ptr<Iterators> b_;
};