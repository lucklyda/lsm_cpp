#pragma once
#include "../iterators.h"
#include <memory>
#include<queue>

struct HeapWrapper {
    int index;
    std::unique_ptr<Iterators> iter;

    HeapWrapper(int idx, std::unique_ptr<Iterators> it)
        : index(idx), iter(std::move(it)) {}

    // 重载比较：用于 priority_queue 最小堆
    bool operator>(const HeapWrapper& other) const {
        int cmp = iter->key().cmp(other.iter->key());
        if (cmp != 0) {
            return cmp > 0; 
        }
        return index > other.index; 
    }
};

struct HeapWrapperPtrGreater {
    bool operator()(
        const std::unique_ptr<HeapWrapper>& a,
        const std::unique_ptr<HeapWrapper>& b
    ) const {
        return *a > *b;
    }
};

using Heap = std::priority_queue<
        std::unique_ptr<HeapWrapper>,      
        std::vector<std::unique_ptr<HeapWrapper>>,
        HeapWrapperPtrGreater               
    >;

class MergeIterators :public Iterators
{
private:
    /* data */
    Heap heap;
    std::unique_ptr<HeapWrapper> current;
    
public:
    MergeIterators(std::vector<std::unique_ptr<Iterators>> iters){
        for (size_t i = 0; i < iters.size(); ++i) {
            auto& iter = iters[i];
            if (iter->is_valid()) {
                heap.push(
                    std::make_unique<HeapWrapper>(i, std::move(iter))
                );
            }
        }
        // 取出堆顶作为当前迭代器
        if (!heap.empty()) {
            current = std::move(const_cast<std::unique_ptr<HeapWrapper>&>(heap.top()));
            heap.pop();
        }

    }
    ~MergeIterators() = default;

    bool is_valid() override {
        return current != nullptr && current->iter->is_valid();
    }

    const Key& key()const override {
        return current->iter->key();
    }

    Value value() const override{
        return current->iter->value();
    }

    bool next() override{
        const LsmKey& current_key = current->iter->key();
        while (!heap.empty()) {
            auto candidate = std::move(const_cast<std::unique_ptr<HeapWrapper>&>(heap.top()));
            heap.pop();
            if(candidate->iter->key()==current_key){
                candidate->iter->next();
                if (candidate->iter->is_valid()) {
                    heap.push(std::move(candidate));
                }
            }else{
                heap.push(std::move(candidate));
                break;
            }
        }
        current->iter->next();
        if (!current->iter->is_valid()) {
            current.reset();
            if (!heap.empty()) {
                current = std::move(const_cast<std::unique_ptr<HeapWrapper>&>(heap.top()));
                heap.pop();
            }
        } else {
            heap.push(std::move(current));
            current = std::move(const_cast<std::unique_ptr<HeapWrapper>&>(heap.top()));
            heap.pop();
        }
        return true;
    }

    uint32_t num_active_iterators() override{
        return heap.size()+(current?1:0);
    }



};


