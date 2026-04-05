#pragma once
#include "../table/table.h"

class SstConcatIterator:public Iterators
{
private:
    /* data */
    std::unique_ptr<SsTableIterator> current_;
    uint32_t next_sst_idx_;
    std::vector<std::shared_ptr<Sstable>> sstables_;
public:
    SstConcatIterator(/* args */) = default;
    ~SstConcatIterator() = default;

    static void check_sst_valid(const std::vector<std::shared_ptr<Sstable>>& sstables) {
        for (const auto& sst : sstables) {
            if(sst->first_key_().cmp(sst->last_key_())>0){
                throw std::logic_error("sstable invalid");
            }
        }
        for (size_t i = 0; i + 1 < sstables.size(); ++i) {
            if (sstables[i]->last_key_() > sstables[i+1]->first_key_()) {
                throw std::logic_error("sstable invalid: overlapping key range");
            }
        }
    }

    void move_until_valid() {
        while (current_) {
            if (current_->is_valid()) break;

            if (next_sst_idx_ >= sstables_.size()) {
                current_.reset();
            } else {
                current_ =std::unique_ptr<SsTableIterator>(SsTableIterator::
                    create_and_seek_to_first(sstables_[next_sst_idx_].get()));
                next_sst_idx_++;
            }
        }
    }

    static std::unique_ptr<SstConcatIterator> create_and_seek_to_first(
        std::vector<std::shared_ptr<Sstable>> sstables
    ) {
        check_sst_valid(sstables);
        auto iter = std::make_unique<SstConcatIterator>();
        iter->sstables_ = std::move(sstables);
        iter->next_sst_idx_ = 0;

        if (!iter->sstables_.empty()) {
            iter->current_ = std::unique_ptr<SsTableIterator>(SsTableIterator::create_and_seek_to_first(iter->sstables_[0].get()));
            iter->next_sst_idx_ = 1;
            iter->move_until_valid();
        }
        return iter;
    }

    static std::unique_ptr<SstConcatIterator> create_and_seek_to_key(
        std::vector<std::shared_ptr<Sstable>> sstables,
        const Key& key
    ) {
        check_sst_valid(sstables);
        auto iter = std::make_unique<SstConcatIterator>();
        iter->sstables_ = std::move(sstables);
        size_t idx = 0;
        if (!iter->sstables_.empty()) {
            idx = partition_point(iter->sstables_.begin(), iter->sstables_.end(),
                [&](const std::shared_ptr<Sstable>& tbl) {
                    return  tbl->first_key_().cmp(key)<=0;
                }) - iter->sstables_.begin();
            if (idx > 0) idx--;
        }

        if (idx >= iter->sstables_.size()) {
            iter->next_sst_idx_ = iter->sstables_.size();
            return iter;
        }

        iter->current_ = std::unique_ptr<SsTableIterator>(SsTableIterator::
            create_and_seek_to_key(iter->sstables_[idx].get(), key));
        iter->next_sst_idx_ = idx + 1;
        iter->move_until_valid();
        return iter;
    }

    LsmKeyView key_view() const override{
        return current_->key_view();
    }
    std::string_view value_view() const override{
        return current_->value_view();
    }
    bool is_valid() override {
        if(current_)return current_->is_valid();
        return false;
    }
    bool next() override {
        current_->next();
        move_until_valid();
        return true;
    }

    uint32_t num_active_iterators() override {
        return 1;
    }
    
};


